#!/usr/bin/env python3
"""
Unit tests for web/backend/app.py
Covers: create_app(), health_check, read_root endpoints, main(), _compact_startup_etl
"""
import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock


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


# ---------------------------------------------------------------------------
# _compact_startup_etl (Change 4 — compact mode startup recovery)
# ---------------------------------------------------------------------------

class TestCompactStartupEtl:
    """Tests for the 7 behaviours specified in the fix plan."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_uow(self, count):
        """Return a job_uow mock whose context manager yields a repo with `count` pending jobs."""
        mock_repo = MagicMock()
        mock_repo.db.execute.return_value.scalar.return_value = count
        mock_uow = MagicMock()
        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)
        return mock_uow

    def _make_ctx(self):
        ctx = MagicMock()
        ctx.aclose = AsyncMock()
        return ctx

    # Behaviour 1
    def test_precheck_skips_when_nothing_pending(self):
        """Pre-check exits immediately when pending count is 0; Redis is never touched."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(0)
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client") as mock_redis_fn:
            self._run(_compact_startup_etl())
        mock_redis_fn.assert_not_called()

    # Behaviour 2
    def test_precheck_triggers_etl_when_pending_jobs_exist(self):
        """When pending > 0, ETL runs: run_job_extraction and run_embedding_extraction are called."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(3)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = self._make_ctx()
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction") as mock_ext, \
             patch("services.base.embeddings.run_embedding_extraction") as mock_emb:
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())
        mock_ext.assert_called_once()
        mock_emb.assert_called_once()

    # Behaviour 3
    def test_redis_lock_prevents_second_worker(self):
        """When the Redis lock is not acquired, ETL is skipped entirely."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(5)
        mock_redis = MagicMock()
        mock_redis.set.return_value = False  # lock not acquired
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("services.base.extraction.run_job_extraction") as mock_ext:
            self._run(_compact_startup_etl())
        mock_ext.assert_not_called()

    # Behaviour 4
    def test_app_context_closed_in_finally_when_etl_raises(self):
        """AppContext.aclose() is called in finally even when ETL raises."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = self._make_ctx()
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction", side_effect=RuntimeError("boom")), \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())  # must not propagate
        mock_ctx.aclose.assert_called_once()

    # Behaviour 6
    def test_startup_etl_state_key_set_to_done_after_completion(self):
        """_STARTUP_ETL_STATE_KEY is updated to status='done' after successful ETL."""
        from web.backend.app import _compact_startup_etl, _STARTUP_ETL_STATE_KEY
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = self._make_ctx()
        set_calls = []
        def capture(key, state, **kw):
            set_calls.append((key, state))
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state", side_effect=capture), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction"), \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())
        done_calls = [(k, s) for k, s in set_calls if s.get("status") == "done"]
        assert len(done_calls) >= 1
        assert done_calls[0][0] == _STARTUP_ETL_STATE_KEY

    # Behaviour 7
    def test_no_redis_etl_runs_without_lock(self):
        """When Redis is unavailable, ETL still runs (no crash, proceeds without lock)."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(2)
        mock_ctx = self._make_ctx()
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", side_effect=Exception("Redis down")), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction") as mock_ext, \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())
        mock_ext.assert_called_once()

    def test_precheck_exception_is_swallowed(self):
        """When the pre-check DB query raises, the function returns without crashing."""
        from web.backend.app import _compact_startup_etl
        mock_uow = MagicMock()
        mock_uow.return_value.__enter__ = MagicMock(side_effect=RuntimeError("DB down"))
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client") as mock_redis_fn:
            self._run(_compact_startup_etl())  # must not raise
        mock_redis_fn.assert_not_called()

    def test_ctx_close_called_when_no_aclose(self):
        """When AppContext has no aclose, close() is used instead."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = MagicMock(spec=["close"])  # no aclose attribute
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction"), \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())
        mock_ctx.close.assert_called_once()

    def test_redis_cleanup_failure_does_not_propagate(self):
        """If redis.delete raises in the finally block, the function still exits cleanly."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.delete.side_effect = Exception("Redis gone")
        mock_ctx = self._make_ctx()
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction"), \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())  # must not raise

    def test_running_state_set_failure_does_not_abort_etl(self):
        """If set_task_state raises when marking 'running', ETL still executes."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = self._make_ctx()
        call_count = [0]
        def set_state_side_effect(key, state, **kw):
            call_count[0] += 1
            if state.get("status") == "running":
                raise Exception("Redis flap")
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state", side_effect=set_state_side_effect), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction") as mock_ext, \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())  # must not raise
        mock_ext.assert_called_once()

    def test_ctx_close_exception_is_swallowed(self):
        """If ctx.aclose() raises, the exception is caught and logged (no propagation)."""
        from web.backend.app import _compact_startup_etl
        mock_uow = self._make_uow(1)
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_ctx = self._make_ctx()
        mock_ctx.aclose = AsyncMock(side_effect=RuntimeError("aclose boom"))
        with patch("database.uow.job_uow", mock_uow), \
             patch("core.redis_streams.get_redis_client", return_value=mock_redis), \
             patch("core.redis_streams.set_task_state"), \
             patch("core.app_context.AppContext") as mock_app_ctx, \
             patch("core.config_loader.load_config"), \
             patch("services.base.extraction.run_job_extraction"), \
             patch("services.base.embeddings.run_embedding_extraction"):
            mock_app_ctx.build.return_value = mock_ctx
            self._run(_compact_startup_etl())  # must not raise

    def test_lifespan_creates_etl_task_in_compact_mode(self):
        """_lifespan schedules _compact_startup_etl when ORCHESTRATOR_URL is unset."""
        from web.backend.app import _lifespan

        async def run():
            created = []
            def capture(coro, **kw):
                coro.close()  # prevent "coroutine never awaited" warning
                created.append(1)
                return MagicMock()
            with patch("asyncio.create_task", side_effect=capture), \
                 patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}):
                async with _lifespan(MagicMock()):
                    pass
            assert len(created) == 1

        self._run(run())

    def test_lifespan_skips_etl_task_in_split_mode(self):
        """_lifespan does NOT schedule startup ETL when ORCHESTRATOR_URL is set."""
        from web.backend.app import _lifespan

        async def run():
            created = []
            def capture(coro, **kw):
                coro.close()
                created.append(1)
                return MagicMock()
            with patch("asyncio.create_task", side_effect=capture), \
                 patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator:8084"}):
                async with _lifespan(MagicMock()):
                    pass
            assert len(created) == 0

        self._run(run())
