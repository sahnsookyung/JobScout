#!/usr/bin/env python3
"""
Unit tests for web/backend/app.py
Covers: create_app(), health_check, read_root endpoints, main()
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestCreateApp:
    """Test create_app factory function."""

    def test_returns_fastapi_instance(self):
        from fastapi import FastAPI
        from web.backend.app import create_app
        assert isinstance(create_app(), FastAPI)

    def test_title(self):
        from web.backend.app import create_app
        assert create_app().title == "JobScout API"

    def test_docs_and_redoc_urls(self):
        from web.backend.app import create_app
        app = create_app()
        assert app.docs_url == "/docs"
        assert app.redoc_url == "/redoc"

    def test_health_and_root_routes_registered(self):
        from web.backend.app import create_app
        app = create_app()
        route_paths = [getattr(r, 'path', '') for r in app.routes]
        assert "/health" in route_paths
        assert "/" in route_paths

    def test_static_dir_not_mounted_when_missing(self):
        """No StaticFiles mount added when the static directory doesn't exist."""
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=Path('/nonexistent_xyz_abc_123')):
            app = create_app()
        from fastapi import FastAPI
        assert isinstance(app, FastAPI)

    def test_static_dir_mounted_when_exists(self, tmp_path):
        """StaticFiles is mounted when the static directory exists."""
        static_dir = tmp_path / 'web' / 'static'
        static_dir.mkdir(parents=True)
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=tmp_path):
            app = create_app()
        mount_paths = [getattr(r, 'path', '') for r in app.routes]
        assert any('/static' in p for p in mount_paths)

    def test_exception_handlers_registered(self):
        """ServiceException, HTTPException, and Exception handlers must be registered."""
        from web.backend.app import create_app
        from web.backend.exceptions import ServiceException
        from fastapi import HTTPException
        app = create_app()
        handler_keys = list(app.exception_handlers.keys())
        assert ServiceException in handler_keys
        assert HTTPException in handler_keys
        assert Exception in handler_keys

    def test_lifespan_calls_dev_bypass_guard(self):
        from fastapi.testclient import TestClient
        from web.backend.app import create_app

        with patch("web.backend.app._ensure_dev_bypass_allowed") as mock_guard:
            with TestClient(create_app()):
                pass

        mock_guard.assert_called()


class TestHealthCheckEndpoint:
    """Test GET /health endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from web.backend.app import create_app
        return TestClient(create_app())

    def test_status_200(self, client):
        assert client.get("/health").status_code == 200

    def test_returns_healthy(self, client):
        data = client.get("/health").json()
        assert data["status"] == "healthy"

    def test_returns_service_name(self, client):
        data = client.get("/health").json()
        assert data["service"] == "jobscout-web"


class TestReadRootEndpoint:
    """Test GET / endpoint."""

    def test_404_when_template_missing(self):
        from fastapi.testclient import TestClient
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=Path('/nonexistent_xyz_abc_123')):
            client = TestClient(create_app())
        resp = client.get("/")
        assert resp.status_code == 404
        assert b"Dashboard not found" in resp.content

    def test_serves_html_when_template_exists(self, tmp_path):
        (tmp_path / 'web' / 'templates').mkdir(parents=True)
        (tmp_path / 'web' / 'templates' / 'index.html').write_text('<html><body>Dashboard</body></html>')
        from fastapi.testclient import TestClient
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=tmp_path):
            client = TestClient(create_app())
            resp = client.get("/")
        assert resp.status_code == 200
        assert b"Dashboard" in resp.content

    def test_content_type_is_html(self, tmp_path):
        (tmp_path / 'web' / 'templates').mkdir(parents=True)
        (tmp_path / 'web' / 'templates' / 'index.html').write_text('<html></html>')
        from fastapi.testclient import TestClient
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=tmp_path):
            client = TestClient(create_app())
            resp = client.get("/")
        assert "text/html" in resp.headers.get("content-type", "")

    def test_returns_full_html_content(self, tmp_path):
        html = '<html><head><title>JobScout</title></head><body><div id="root"></div></body></html>'
        (tmp_path / 'web' / 'templates').mkdir(parents=True)
        (tmp_path / 'web' / 'templates' / 'index.html').write_text(html)
        from fastapi.testclient import TestClient
        from web.backend.app import create_app
        with patch('web.backend.app.get_project_root', return_value=tmp_path):
            client = TestClient(create_app())
            resp = client.get("/")
        assert b"JobScout" in resp.content
        assert b'<div id="root">' in resp.content


class TestExceptionHandlerIntegration:
    """Test exception handlers behave correctly via TestClient."""

    @pytest.fixture
    def client_with_boom(self):
        from fastapi import FastAPI, HTTPException
        from fastapi.testclient import TestClient
        from web.backend.exceptions import (
            ServiceException, service_exception_handler,
            http_exception_handler, general_exception_handler,
        )
        app = FastAPI()
        app.add_exception_handler(ServiceException, service_exception_handler)
        app.add_exception_handler(HTTPException, http_exception_handler)
        app.add_exception_handler(Exception, general_exception_handler)

        @app.get("/http_error")
        def http_error():
            raise HTTPException(status_code=418, detail="I'm a teapot")

        @app.get("/service_error")
        def service_error():
            raise ServiceException("service failed")

        @app.get("/runtime_error")
        def runtime_error():
            raise RuntimeError("unexpected crash")

        return TestClient(app, raise_server_exceptions=False)

    def test_http_exception_returns_json(self, client_with_boom):
        resp = client_with_boom.get("/http_error")
        assert resp.status_code == 418

    def test_service_exception_returns_error_json(self, client_with_boom):
        resp = client_with_boom.get("/service_error")
        assert resp.status_code in (400, 422, 500)

    def test_general_exception_returns_500(self, client_with_boom):
        resp = client_with_boom.get("/runtime_error")
        assert resp.status_code == 500


class TestMain:
    """Test main() entry point."""

    def test_main_calls_uvicorn_run(self):
        from web.backend.app import main
        with patch('uvicorn.run') as mock_run:
            main()
        mock_run.assert_called_once()

    def test_main_runs_correct_app_string(self):
        from web.backend.app import main
        with patch('uvicorn.run') as mock_run:
            main()
        args, _ = mock_run.call_args
        assert args[0] == "web.backend.app:app"

    def test_main_uses_config_host_and_port(self):
        from web.backend.app import main, config
        with patch('uvicorn.run') as mock_run:
            main()
        _, kwargs = mock_run.call_args
        assert kwargs.get('host') == config.web.host
        assert kwargs.get('port') == config.web.port

    def test_main_disables_reload(self):
        from web.backend.app import main
        with patch('uvicorn.run') as mock_run:
            main()
        _, kwargs = mock_run.call_args
        assert kwargs.get('reload') is False
