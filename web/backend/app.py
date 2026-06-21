#!/usr/bin/env python3
"""
JobScout Web Dashboard - FastAPI Application

A modern web application to view job matching results with automatic API documentation.

Usage:
    uv run python web/app.py
    
Then open:
    - http://localhost:8080 - Dashboard (default port, configurable in config.yaml)
    - http://localhost:8080/docs - API Documentation (Swagger UI)
    - http://localhost:8080/redoc - Alternative API Documentation
"""

import sys
import logging
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logging_utils import (
    is_nul_filter_active,
    setup_logging as setup_shared_logging,
)
from core.metrics_router import router as metrics_router
from core.llm_evaluation_queue import enqueue_stale_or_retryable_evaluations
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config import get_config, get_project_root
from .dependencies import _ensure_dev_bypass_allowed
from .exceptions import (
    ServiceException,
    service_exception_handler,
    http_exception_handler,
    general_exception_handler
)
from .routers import (
    matches_router,
    jobs_router,
    stats_router,
    policy_router,
    pipeline_router,
    pipeline_runs_router,
    notifications_router,
    candidate_preferences_router,
    resume_variants_router,
)

# Configure logging
setup_shared_logging(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.debug("NUL log sanitization active=%s", is_nul_filter_active())

# Load configuration
config = get_config()

WEB_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; "
    "script-src 'self' https://accounts.google.com https://accounts.gstatic.com; "
    "frame-src https://accounts.google.com; "
    "connect-src 'self' https://oauth2.googleapis.com; "
    "img-src 'self' data: https:; "
    "font-src 'self' data: https://frontend-cdn.perplexity.ai; "
    "style-src 'self' 'unsafe-inline' https://accounts.google.com/gsi/style; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "frame-ancestors 'none'"
)


@lru_cache(maxsize=8)
def _read_dashboard_html(path: str, mtime_ns: int, size: int) -> str:
    """Read dashboard HTML with a cache key that refreshes when the file changes."""
    del mtime_ns, size
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def _dashboard_response() -> HTMLResponse:
    """Serve the dashboard shell used by both `/` and `/dashboard`."""
    project_root = get_project_root()
    html_candidates = (
        project_root / 'web' / 'frontend' / 'dist' / 'index.html',
        project_root / 'web' / 'templates' / 'index.html',
    )

    html_path = next((path for path in html_candidates if path.exists()), None)

    if html_path is None:
        return HTMLResponse(
            content="<h1>Dashboard not found</h1><p>Please run the frontend build or provide web/templates/index.html</p>",
            status_code=404,
        )

    stat = html_path.stat()
    return HTMLResponse(
        content=_read_dashboard_html(str(html_path), stat.st_mtime_ns, stat.st_size)
    )


@asynccontextmanager
async def _lifespan(app: FastAPI):
    _ensure_dev_bypass_allowed()
    try:
        enqueue_stale_or_retryable_evaluations()
    except Exception:
        logger.warning("Failed to enqueue stale LLM evaluations during startup", exc_info=True)
    yield


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns a fully configured app instance. Called at module level for OSS
    single-user use, and by the SaaS layer (jobscout-cloud) to extend via
    dependency_overrides and additional routers.
    """
    from .routers.pipeline import add_rate_limit_handlers

    _app = FastAPI(
        title="JobScout API",
        description="API for viewing job matching results",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=_lifespan,
    )

    # Configure rate limiting
    add_rate_limit_handlers(_app)

    @_app.middleware("http")
    async def add_web_security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Content-Security-Policy", WEB_CONTENT_SECURITY_POLICY)
        if request.url.scheme == "https":
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains",
            )
        return response

    # Register exception handlers
    _app.add_exception_handler(ServiceException, service_exception_handler)
    _app.add_exception_handler(HTTPException, http_exception_handler)
    _app.add_exception_handler(Exception, general_exception_handler)

    # Include routers
    _app.include_router(matches_router)
    _app.include_router(jobs_router)
    _app.include_router(stats_router)
    _app.include_router(policy_router)
    _app.include_router(pipeline_router)
    _app.include_router(pipeline_runs_router)
    _app.include_router(notifications_router)
    _app.include_router(candidate_preferences_router)
    _app.include_router(resume_variants_router)
    _app.include_router(metrics_router)

    # Mount static files if they exist
    static_dir = get_project_root() / 'web' / 'static'
    if static_dir.exists():
        _app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    frontend_dist_dir = get_project_root() / 'web' / 'frontend' / 'dist'
    frontend_assets_dir = frontend_dist_dir / 'assets'
    if frontend_assets_dir.exists():
        _app.mount(
            "/assets",
            StaticFiles(directory=str(frontend_assets_dir)),
            name="frontend-assets",
        )

    favicon_path = frontend_dist_dir / 'favicon.svg'
    if favicon_path.exists():
        @_app.get("/favicon.svg", include_in_schema=False)
        def read_favicon():
            return FileResponse(favicon_path)

    @_app.get("/", response_class=HTMLResponse)
    def read_root():
        """Serve the main dashboard HTML page."""
        return _dashboard_response()

    @_app.get("/dashboard", response_class=HTMLResponse)
    def read_dashboard():
        """Serve the dashboard shell from an explicit dashboard URL."""
        return _dashboard_response()

    @_app.get("/verify-email", response_class=HTMLResponse)
    def read_verify_email():
        """Serve the dashboard shell for the React email verification route."""
        return _dashboard_response()

    @_app.get("/health")
    def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "service": "jobscout-web"}

    return _app


# Module-level instance for OSS single-user use and uvicorn entry point
app = create_app()


def main():
    """Run the web server."""
    import uvicorn
    
    logger.info(f"Starting JobScout Web Server on {config.web.host}:{config.web.port}")
    logger.info(f"Dashboard: http://{config.web.host}:{config.web.port}")
    logger.info(f"API Docs: http://{config.web.host}:{config.web.port}/docs")
    
    uvicorn.run(
        "web.backend.app:app",
        host=config.web.host,
        port=config.web.port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
