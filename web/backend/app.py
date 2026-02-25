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
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
config = get_config()

# Create FastAPI app
app = FastAPI(
    title="JobScout API",
    description="API for viewing job matching results",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure rate limiting
from .routers.pipeline import add_rate_limit_handlers
add_rate_limit_handlers(app)

# Register exception handlers
app.add_exception_handler(ServiceException, service_exception_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# Include routers
app.include_router(matches_router)
app.include_router(stats_router)
app.include_router(policy_router)
app.include_router(pipeline_router)
app.include_router(notifications_router)

# Mount static files if they exist
static_dir = get_project_root() / 'web' / 'static'
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
def read_root():
    """
    Serve the main dashboard HTML page.
    """
    html_path = get_project_root() / 'web' / 'templates' / 'index.html'
    
    if not html_path.exists():
        return HTMLResponse(
            content="<h1>Dashboard not found</h1><p>Please ensure web/templates/index.html exists</p>",
            status_code=404
        )
    
    with open(html_path, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "jobscout-web"}


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
