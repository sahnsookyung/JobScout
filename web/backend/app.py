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
import os
import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.logging_utils import (
    is_nil_filter_active,
    setup_logging as setup_shared_logging,
)
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config import get_config, get_project_root
from .exceptions import (
    ServiceException,
    service_exception_handler,
    http_exception_handler,
    general_exception_handler
)
from .routers import (
    matches_router,
    stats_router,
    policy_router,
    pipeline_router,
    notifications_router
)

# Configure logging
setup_shared_logging(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.debug("NIL log sanitization active=%s", is_nil_filter_active())

# Load configuration
config = get_config()


_STARTUP_ETL_LOCK_KEY  = "pipeline:startup:etl:lock"
_STARTUP_ETL_STATE_KEY = "pipeline:startup:etl:state"


async def _compact_startup_etl() -> None:
    """Process any pending jobs left over from a previous run (compact mode only)."""
    from core.config_loader import load_config
    from core.app_context import AppContext
    from core.redis_streams import get_redis_client, set_task_state
    from services.base.extraction import run_job_extraction
    from services.base.embeddings import run_embedding_extraction
    from database.uow import job_uow
    from database.models.job import JobPost
    from sqlalchemy import select, func

    # Pre-check: skip if nothing pending
    try:
        with job_uow() as repo:
            pending = repo.db.execute(
                select(func.count()).select_from(JobPost).where(
                    (JobPost.extraction_status == 'pending') |
                    (JobPost.facet_status      == 'pending') |
                    (JobPost.embedding_status  == 'pending')
                )
            ).scalar()
        if not pending:
            logger.info("Compact startup ETL: nothing pending, skipping")
            return
        logger.info("Compact startup ETL: %d pending jobs found", pending)
    except Exception:
        logger.exception("Compact startup ETL: pre-check failed, skipping")
        return

    # Redis distributed lock (ex=7200 to outlast large backlogs)
    try:
        redis = get_redis_client()
        acquired = redis.set(_STARTUP_ETL_LOCK_KEY, "1", nx=True, ex=7200)
        if not acquired:
            logger.info("Compact startup ETL: another worker holds the lock, skipping")
            return
    except Exception:
        logger.warning("Compact startup ETL: Redis unavailable, proceeding without lock")
        redis = None

    ctx = None
    try:
        ctx = AppContext.build(load_config())
        # Set running state AFTER ETL confirmed to start (avoids non-atomic gap)
        if redis:
            try:
                set_task_state(_STARTUP_ETL_STATE_KEY, {"status": "running"}, ttl=7200)
            except Exception:
                pass
        stop = threading.Event()
        await asyncio.to_thread(run_job_extraction, ctx, stop, 200)
        await asyncio.to_thread(run_embedding_extraction, ctx, stop, 100)
        logger.info("Compact startup ETL: complete")
    except Exception:
        logger.exception("Compact startup ETL: failed")
    finally:
        if ctx is not None:
            try:
                if hasattr(ctx, 'aclose'):
                    await ctx.aclose()
                elif hasattr(ctx, 'close'):
                    ctx.close()
            except Exception:
                logger.warning("Compact startup ETL: error closing AppContext")
        if redis is not None:
            try:
                redis.delete(_STARTUP_ETL_LOCK_KEY)
                set_task_state(_STARTUP_ETL_STATE_KEY, {"status": "done"}, ttl=60)
            except Exception:
                pass


@asynccontextmanager
async def _lifespan(app: FastAPI):
    if not os.getenv("ORCHESTRATOR_URL", "").strip():
        asyncio.create_task(_compact_startup_etl())
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

    # Register exception handlers
    _app.add_exception_handler(ServiceException, service_exception_handler)
    _app.add_exception_handler(HTTPException, http_exception_handler)
    _app.add_exception_handler(Exception, general_exception_handler)

    # Include routers
    _app.include_router(matches_router)
    _app.include_router(stats_router)
    _app.include_router(policy_router)
    _app.include_router(pipeline_router)
    _app.include_router(notifications_router)

    # Mount static files if they exist
    static_dir = get_project_root() / 'web' / 'static'
    if static_dir.exists():
        _app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @_app.get("/", response_class=HTMLResponse)
    def read_root():
        """Serve the main dashboard HTML page."""
        html_path = get_project_root() / 'web' / 'templates' / 'index.html'

        if not html_path.exists():
            return HTMLResponse(
                content="<h1>Dashboard not found</h1><p>Please ensure web/templates/index.html exists</p>",
                status_code=404
            )

        with open(html_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())

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
