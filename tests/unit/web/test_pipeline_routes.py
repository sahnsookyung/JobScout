#!/usr/bin/env python3
"""Unit tests for pipeline router lifecycle edge cases."""

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID


class TestPipelineRoutes(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.backend.dependencies import get_current_user
        from web.backend.routers.pipeline import limiter, router

        limiter.enabled = False
        self.app = FastAPI()
        self.app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000001")
        )
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_returns_seed_websites_and_api_metadata(self, mock_config):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=SimpleNamespace(url="http://jobspy:8000"),
            scrapers=[
                ScraperConfig(
                    site_type=["tokyodev"],
                    description="English-friendly Japan startup roles",
                    tags=["japan", "startup"],
                    search_term="",
                    results_wanted=5,
                    options={"seniorities": ["junior"]},
                ),
                ScraperConfig(
                    site_type=["indeed"],
                    search_term="software engineer",
                    location="Tokyo",
                    country="Japan",
                    results_wanted=10,
                    hours_old=168,
                ),
            ],
        )

        response = self.client.get("/api/pipeline/sources")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["api_based_fetching"])
        self.assertEqual(data["jobspy_url"], "http://jobspy:8000")
        self.assertIn("https://www.tokyodev.com/jobs", data["seed_websites"])
        self.assertEqual(data["total_count"], 2)
        self.assertEqual(data["filtered_count"], 2)
        self.assertEqual(data["sources"][0]["display_name"], "TokyoDev")
        self.assertEqual(data["sources"][0]["fetch_mode"], "jobspy_api")
        self.assertEqual(data["sources"][0]["provider_name"], "JobSpy")
        self.assertIsNone(data["sources"][0]["api_health"])
        self.assertIsNone(data["sources"][0]["external_fetch_status"])
        self.assertIn("startup", data["sources"][0]["search_keywords"])
        self.assertEqual(data["sources"][1]["fetch_mode"], "jobspy_api")
        self.assertEqual(data["sources"][1]["provider_name"], "JobSpy")

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_filters_searchable_metadata(self, mock_config):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=SimpleNamespace(url="http://jobspy:8000"),
            scrapers=[
                ScraperConfig(
                    site_type=["tokyodev"],
                    tags=["japan", "startup"],
                    search_term="",
                    results_wanted=5,
                ),
                ScraperConfig(
                    site_type=["indeed"],
                    search_term="platform engineer",
                    location="Berlin",
                    country="Germany",
                    results_wanted=10,
                ),
            ],
        )

        response = self.client.get("/api/pipeline/sources?search=berlin platform")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["search_query"], "berlin platform")
        self.assertEqual(data["total_count"], 2)
        self.assertEqual(data["filtered_count"], 1)
        self.assertEqual(data["sources"][0]["display_name"], "Indeed")
        self.assertEqual(data["seed_websites"], ["https://www.indeed.com"])

    @patch("web.backend.routers.pipeline.JobSpyClient")
    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_checks_jobspy_health_when_requested(self, mock_config, mock_client_cls):
        from core.config_loader import JobSpyConfig, ScraperConfig

        client = MagicMock()
        client.check_health.return_value = {
            "available": True,
            "status": "available",
            "endpoint": "http://jobspy:8000/health",
            "status_code": 200,
            "response_time_ms": 12,
            "error": None,
        }
        mock_client_cls.return_value = client
        mock_config.return_value = SimpleNamespace(
            jobspy=JobSpyConfig(url="http://jobspy:8000", health_timeout_seconds=0.25),
            scrapers=[
                ScraperConfig(site_type=["linkedin"], search_term="software engineer"),
            ],
        )

        response = self.client.get("/api/pipeline/sources?include_status=true")

        self.assertEqual(response.status_code, 200)
        health = response.json()["sources"][0]["api_health"]
        self.assertTrue(health["available"])
        self.assertEqual(health["status_code"], 200)
        client.check_health.assert_called_once_with(timeout_seconds=0.25)
        client.close.assert_called_once()

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_reports_unconfigured_jobspy_status(self, mock_config):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=None,
            scrapers=[ScraperConfig(site_type=["indeed"], search_term="platform")],
        )

        response = self.client.get("/api/pipeline/sources?include_status=true")

        self.assertEqual(response.status_code, 200)
        health = response.json()["sources"][0]["api_health"]
        self.assertFalse(health["available"])
        self.assertEqual(health["status"], "not_configured")

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_marks_ats_and_custom_sources_without_jobspy_health(self, mock_config):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=None,
            scrapers=[
                ScraperConfig(site_type=["greenhouse"], display_name="Greenhouse Careers"),
                ScraperConfig(site_type=["workday"], display_name="Workday Careers"),
                ScraperConfig(site_type=["internal_feed"], search_term="platform"),
            ],
        )

        response = self.client.get("/api/pipeline/sources?include_status=true")

        self.assertEqual(response.status_code, 200)
        sources = response.json()["sources"]
        self.assertEqual(sources[0]["fetch_mode"], "ats_api")
        self.assertEqual(sources[0]["provider_name"], "Greenhouse ATS")
        self.assertIsNone(sources[0]["api_health"])
        self.assertTrue(sources[0]["api_fetch_available"])
        self.assertEqual(sources[0]["availability_reason"], "ats_api_available")
        self.assertEqual(sources[1]["fetch_mode"], "ats_api")
        self.assertFalse(sources[1]["api_fetch_available"])
        self.assertEqual(
            sources[1]["availability_reason"],
            "not_supported_api_adapter_missing",
        )
        self.assertEqual(sources[2]["fetch_mode"], "custom_source")
        self.assertIsNone(sources[2]["api_health"])

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_marks_web_sources_disabled_in_production(self, mock_config):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=SimpleNamespace(url="http://jobspy:8000"),
            scrapers=[
                ScraperConfig(site_type=["tokyodev"], search_term="", results_wanted=5),
                ScraperConfig(
                    site_type=["indeed"],
                    search_term="platform",
                    enabled=False,
                    fetch_mode="jobspy_api",
                ),
                ScraperConfig(site_type=["greenhouse"], display_name="Greenhouse"),
            ],
        )

        with patch.dict(os.environ, {"JOBSCOUT_ENV": "production"}):
            response = self.client.get("/api/pipeline/sources")

        self.assertEqual(response.status_code, 200)
        sources = response.json()["sources"]
        self.assertTrue(sources[0]["deployment_allowed"])
        self.assertFalse(sources[0]["api_fetch_available"])
        self.assertEqual(sources[0]["fetch_mode"], "jobspy_api")
        self.assertIsNone(sources[0]["external_fetch_status"])
        self.assertFalse(sources[1]["deployment_allowed"])
        self.assertFalse(sources[1]["api_fetch_available"])
        self.assertEqual(sources[1]["disabled_reason"], "source_disabled")
        self.assertTrue(sources[2]["deployment_allowed"])
        self.assertTrue(sources[2]["api_fetch_available"])
        self.assertEqual(sources[2]["fetch_mode"], "ats_api")
        self.assertTrue(response.json()["api_based_fetching"])

    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    @patch("web.backend.services.clients.orchestrator_client")
    def test_process_jobs_endpoint_starts_imported_job_processing(
        self,
        mock_orchestrator,
        mock_get_redis,
        mock_set_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = None
        mock_get_redis.return_value = redis
        mock_orchestrator.start_process_imported_jobs_pipeline.return_value = {
            "success": True,
            "task_id": "process-jobs-abc123",
            "status": "queued",
            "current_stage": "extract",
            "result": {"extracted_count": 0, "embedded_count": 0},
        }

        response = self.client.post("/api/pipeline/process-jobs")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["task_id"], "process-jobs-abc123")
        mock_orchestrator.start_process_imported_jobs_pipeline.assert_called_once()
        redis.set.assert_called_once()
        mock_set_task_state.assert_called_once()
        state = mock_set_task_state.call_args.args[1]
        self.assertEqual(state["task_type"], "job_processing")
        self.assertEqual(state["owner_id"], "00000000-0000-0000-0000-000000000001")

    @patch("web.backend.routers.pipeline.get_external_seed_fetcher_status")
    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_reports_external_worker_status(
        self,
        mock_config,
        mock_external_status,
    ):
        from core.config_loader import ScraperConfig

        mock_config.return_value = SimpleNamespace(
            jobspy=None,
            scrapers=[
                ScraperConfig(site_type=["tokyodev"], search_term="", results_wanted=5),
            ],
        )
        mock_external_status.return_value = {
            "sources": {
                "tokyodev": {
                    "enabled": True,
                    "configured": True,
                    "status": "configured",
                    "provider": "cloudflare_worker_seed",
                    "budget_remaining": 42,
                }
            }
        }

        response = self.client.get("/api/pipeline/sources?include_status=true")

        self.assertEqual(response.status_code, 200)
        source = response.json()["sources"][0]
        self.assertEqual(source["provider_name"], "JobSpy")
        self.assertIsNone(source["external_fetch_status"])

    @patch("web.backend.routers.pipeline.get_config")
    def test_fetch_sources_endpoint_does_not_require_database_auth(self, mock_config):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from web.backend.routers.pipeline import router

        mock_config.return_value = SimpleNamespace(jobspy=None, scrapers=[])
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/api/pipeline/sources")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["sources"], [])

    @patch("web.backend.routers.pipeline.fetch_and_import_external_seed_source")
    def test_source_fetch_endpoint_returns_external_fetch_summary(self, mock_fetch):
        mock_fetch.return_value = SimpleNamespace(
            success=True,
            as_dict=lambda: {
                "success": True,
                "source": "tokyodev",
                "status": "ok",
                "fetched_count": 2,
                "imported_count": 2,
                "skipped_count": 0,
                "warnings": [],
                "next_eligible_at": "2026-05-23T00:00:00+00:00",
                "failure_class": None,
                "budget_remaining": 9,
            },
        )

        response = self.client.post(
            "/api/pipeline/source-fetch",
            json={"source": "tokyodev", "limit": 2},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["imported_count"], 2)
        mock_fetch.assert_called_once_with("tokyodev", tenant_id=None, limit=2)

    @patch("web.backend.routers.pipeline.fetch_and_import_external_seed_source")
    def test_source_fetch_endpoint_maps_rate_limited_summary_to_429(self, mock_fetch):
        mock_fetch.return_value = SimpleNamespace(
            success=False,
            as_dict=lambda: {
                "success": False,
                "source": "japandev",
                "status": "rate_limited",
                "fetched_count": 0,
                "imported_count": 0,
                "skipped_count": 0,
                "warnings": [],
                "next_eligible_at": "2026-05-23T04:00:00+00:00",
                "failure_class": "min_interval",
                "budget_remaining": None,
            },
        )

        response = self.client.post(
            "/api/pipeline/source-fetch",
            json={"source": "japandev"},
        )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json()["failure_class"], "min_interval")

    def test_source_fetch_endpoint_requires_admin_in_production_without_tenant_context(self):
        with patch.dict(os.environ, {"JOBSCOUT_ENV": "production"}):
            response = self.client.post(
                "/api/pipeline/source-fetch",
                json={"source": "tokyodev"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.json()["code"],
            "pipeline.source_fetch_admin_required",
        )

    @patch("web.backend.routers.pipeline.fetch_and_import_external_seed_source")
    def test_source_fetch_endpoint_rejects_non_admin_tenant_role(self, mock_fetch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from web.backend.dependencies import get_current_user
        from web.backend.routers.pipeline import router

        app = FastAPI()
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000001")
        )

        @app.middleware("http")
        async def add_member_tenant_context(request, call_next):
            request.state.tenant_id = UUID("00000000-0000-0000-0000-000000000201")
            request.state.tenant_role = "member"
            return await call_next(request)

        app.include_router(router)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/api/pipeline/source-fetch",
            json={"source": "tokyodev"},
        )

        self.assertEqual(response.status_code, 403)
        mock_fetch.assert_not_called()

    @patch("web.backend.routers.pipeline.fetch_and_import_external_seed_source")
    def test_source_fetch_endpoint_returns_external_fetch_error_without_500(self, mock_fetch):
        from etl.external_seed_fetcher import ExternalSeedFetchError

        mock_fetch.side_effect = ExternalSeedFetchError(
            "external_seed_unconfigured",
            "External seed fetcher URL or secret is not configured.",
            status_code=503,
            failure_class="external_seed_unconfigured",
        )

        response = self.client.post(
            "/api/pipeline/source-fetch",
            json={"source": "tokyodev"},
        )

        self.assertEqual(response.status_code, 503)
        payload = response.json()
        self.assertFalse(payload["success"])
        self.assertEqual(payload["failure_class"], "external_seed_unconfigured")

    @patch("web.backend.routers.pipeline.enqueue_job")
    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    def test_run_matching_starts_when_ready_resume_exists(
        self,
        _mock_redis,
        mock_eligibility,
        mock_enqueue,
    ):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=True,
            upload_id="upload-1",
            resume_fingerprint="fp-ready",
        )

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertTrue(data["task_id"])
        mock_enqueue.assert_called_once()

    @patch("web.backend.routers.pipeline._enqueue_matching_for_ready_resume")
    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    @patch("web.backend.routers.pipeline._guard_resume_not_uploading")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=object())
    def test_run_matching_reuses_active_matching_task(
        self,
        _mock_redis,
        mock_upload_guard,
        mock_eligibility,
        mock_enqueue_ready_resume,
    ):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=True,
            upload_id="upload-1",
            resume_fingerprint="fp-ready",
        )
        mock_enqueue_ready_resume.return_value = "match-active"

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["task_id"], "match-active")
        mock_upload_guard.assert_called_once()
        mock_enqueue_ready_resume.assert_called_once()

    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    def test_run_matching_reports_resume_processing_state(
        self,
        _mock_redis,
        mock_eligibility,
    ):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=False,
            processing_status="embedding",
            message="Latest uploaded resume is still processing (embedding).",
        )

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 409)
        data = response.json()
        self.assertEqual(data["code"], "pipeline.resume.upload_in_progress")
        self.assertIn("still processing (embedding)", data["message"])

    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    def test_resume_eligibility_endpoint_returns_authoritative_status(self, mock_eligibility):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=False,
            processing_status="embedding",
            message="Latest uploaded resume is still processing (embedding).",
            retryable=True,
            upload_id="upload-1",
            resume_hash="hash-1",
            resume_fingerprint="fp-1",
            processing_task_id="task-1",
        )

        response = self.client.get("/api/pipeline/resume-eligibility")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data["can_run"])
        self.assertEqual(data["status"], "embedding")
        self.assertEqual(data["task_id"], "task-1")

    @patch("web.backend.routers.pipeline.set_task_cancellation_requested")
    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_stop_matching_marks_active_redis_task_cancelled(
        self,
        mock_get_redis,
        mock_get_task_state,
        mock_set_task_state,
        mock_set_cancel_requested,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-2"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {"status": "running", "step": "scoring"}

        response = self.client.post("/api/pipeline/stop")

        self.assertEqual(response.status_code, 200)
        self.assertIn("cancellation requested", response.json()["message"].lower())
        mock_set_task_state.assert_called_once_with(
            "task-2",
            {"status": "cancellation_requested", "step": "scoring"},
            ttl=3600,
        )
        mock_set_cancel_requested.assert_called_once_with("task-2", ttl=3600)

    @patch("web.backend.routers.pipeline.get_task_state")
    def test_pipeline_events_streams_terminal_redis_status(self, mock_get_task_state):
        mock_get_task_state.return_value = {
            "status": "cancelled",
            "step": "scoring",
            "owner_id": "00000000-0000-0000-0000-000000000001",
            "result": {
                "matches_count": 2,
                "saved_count": 1,
                "execution_time": 1.2,
            },
            "error": "Cancelled by user",
        }

        with self.client.stream("GET", "/api/pipeline/events/task-3") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("data: ", body)
        payload = json.loads(body.split("data: ", 1)[1].split("\n", 1)[0].strip())
        self.assertEqual(payload["status"], "cancelled")
        self.assertEqual(payload["saved_count"], 1)

    @patch("web.backend.routers.pipeline.get_task_state")
    def test_pipeline_status_includes_stale_result_metadata(self, mock_get_task_state):
        mock_get_task_state.return_value = {
            "status": "completed",
            "step": "notifying",
            "upload_id": "upload-old",
            "resume_fingerprint": "fp-old",
            "owner_id": "00000000-0000-0000-0000-000000000001",
            "stale_due_to_newer_upload": True,
            "latest_upload_id": "upload-new",
            "latest_resume_fingerprint": "fp-new",
            "stale_message": "These results were generated from an older resume upload.",
            "result": {
                "matches_count": 4,
                "saved_count": 3,
                "notified_count": 1,
                "execution_time": 2.5,
            },
        }

        response = self.client.get("/api/pipeline/status/task-stale")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["stale_due_to_newer_upload"])
        self.assertEqual(data["latest_upload_id"], "upload-new")
        self.assertEqual(data["resume_fingerprint"], "fp-old")

    @patch("web.backend.routers.pipeline.get_task_state")
    def test_pipeline_status_explains_matching_backlog_no_progress(self, mock_get_task_state):
        mock_get_task_state.return_value = {
            "status": "completed",
            "step": "notifying",
            "owner_id": "00000000-0000-0000-0000-000000000001",
            "warnings": [{"code": "matching_backlog_no_progress"}],
            "stats": {
                "jobs_ready_to_score": 10,
                "jobs_pending_matching": 4,
                "jobs_pending_extraction": 0,
                "jobs_pending_embedding": 0,
            },
            "result": {
                "matches_count": 0,
                "saved_count": 0,
                "notified_count": 0,
                "execution_time": 0.5,
            },
        }

        response = self.client.get("/api/pipeline/status/task-stalled")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["warnings"][0]["code"], "matching_backlog_no_progress")
        self.assertIn("eligible", data["warnings"][0]["message"])

    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_active_pipeline_endpoint_returns_running_task(
        self,
        mock_get_redis,
        mock_get_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-active"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {
            "status": "running",
            "step": "matching",
            "upload_id": "upload-1",
            "resume_fingerprint": "fp-1",
        }

        response = self.client.get("/api/pipeline/active")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["task_id"], "task-active")
        self.assertEqual(data["status"], "running")
        self.assertEqual(data["step"], "vector_matching")
        self.assertEqual(data["upload_id"], "upload-1")

    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_active_pipeline_endpoint_returns_null_for_terminal_task(
        self,
        mock_get_redis,
        mock_get_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-done"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {"status": "completed"}

        response = self.client.get("/api/pipeline/active")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json())

    @patch("web.backend.routers.pipeline.evaluate_resume_preflight")
    def test_resume_preflight_endpoint_returns_read_only_status(self, mock_preflight):
        mock_preflight.return_value = SimpleNamespace(
            status="ready_already_known",
            message="Resume already processed and ready for matching.",
            retryable=False,
            can_skip_upload=True,
            resume_hash="hash-1",
            upload_id="upload-1",
            processing_task_id=None,
        )

        response = self.client.post("/api/pipeline/resume-preflight", json={"resume_hash": "hash-1"})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "ready_already_known")
        self.assertTrue(data["can_skip_upload"])

    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_select_resume_clears_latest_upload_marker(self, mock_get_redis):
        redis = MagicMock()
        mock_get_redis.return_value = redis

        with patch("web.backend.routers.pipeline.job_uow") as mock_uow:
            repo = MagicMock()
            repo.is_resume_ready.return_value = True
            repo.create_resume_upload.return_value = SimpleNamespace(id="upload-selected")
            mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post(
                "/api/pipeline/select-resume",
                json={"resume_hash": "hash-old", "original_filename": "resume-old.json"},
            )

        self.assertEqual(response.status_code, 200)
        redis.delete.assert_called_once()

    def test_upload_resume_returns_processing_message_for_existing_in_progress_upload(self):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="fp-123"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = False
                    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
                        id="upload-1",
                        processing_task_id="task-1",
                        status="in_progress",
                    )
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["task_id"], "task-1")

    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_task_state")
    def test_upload_resume_reconciles_stale_in_progress_upload(
        self,
        mock_get_task_state,
        mock_set_task_state,
    ):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}
        stale_upload = SimpleNamespace(
            id="upload-1",
            processing_task_id="task-stale",
            status="in_progress",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            resume_fingerprint="fp-stale",
            resume_hash="hash-stale",
            owner_id=UUID("00000000-0000-0000-0000-000000000001"),
        )
        recovered_upload = SimpleNamespace(
            id="upload-1",
            processing_task_id="task-stale",
            status="failed_reupload_required",
            created_at=stale_upload.created_at,
            resume_fingerprint="fp-stale",
            resume_hash="hash-stale",
            owner_id=stale_upload.owner_id,
        )

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="hash-stale"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = False
                    repo.get_latest_resume_upload_for_hash.return_value = stale_upload
                    repo.get_resume_processing_state.return_value = None
                    repo.get_structured_resume_by_fingerprint.return_value = None
                    repo.get_resume_upload.return_value = recovered_upload
                    mock_get_task_state.return_value = {"status": "processing", "step": "extracting"}
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data["task_id"], "task-stale")
        repo.update_resume_upload.assert_called()
        mock_set_task_state.assert_called()

    @patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://localhost:8084"}, clear=False)
    @patch("web.backend.routers.pipeline._stream_orchestrator_sse")
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline._active_task_id_for_owner", return_value="task-orchestrator")
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_falls_back_to_orchestrator_stream_when_redis_missing(
        self,
        _mock_get_task_state,
        _mock_active_task_id,
        mock_preflight,
        mock_stream,
    ):
        async def _stream():
            yield b'data: {"status":"running"}\n\n'

        mock_stream.return_value = _stream()

        with self.client.stream("GET", "/api/pipeline/events/task-orchestrator") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('"status":"running"', body)
        mock_preflight.assert_awaited_once()
        mock_stream.assert_called_once()

    @patch.dict(
        "os.environ",
        {
            "INTERNAL_ORCHESTRATOR_URL": "http://orchestrator:8084",
            "ORCHESTRATOR_URL": "http://localhost:8084",
        },
        clear=False,
    )
    @patch("web.backend.routers.pipeline._stream_orchestrator_sse")
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline._active_task_id_for_owner", return_value="task-orchestrator")
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_prefers_internal_orchestrator_url_inside_container(
        self,
        _mock_get_task_state,
        _mock_active_task_id,
        mock_preflight,
        mock_stream,
    ):
        async def _stream():
            yield b'data: {"status":"running"}\n\n'

        mock_stream.return_value = _stream()

        with self.client.stream("GET", "/api/pipeline/events/task-orchestrator") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('"status":"running"', body)
        mock_preflight.assert_awaited_once_with("http://orchestrator:8084", "task-orchestrator")
        mock_stream.assert_called_once_with("http://orchestrator:8084", "task-orchestrator")

    @patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://localhost:8084"}, clear=False)
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_returns_404_when_orchestrator_task_is_missing(
        self,
        _mock_get_task_state,
        mock_preflight,
    ):
        from fastapi import HTTPException

        mock_preflight.side_effect = HTTPException(status_code=404, detail="Task not found")

        response = self.client.get("/api/pipeline/events/task-missing")

        self.assertEqual(response.status_code, 404)

    def test_upload_resume_returns_ready_message_for_already_processed_resume(self):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="fp-ready"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = True
                    repo.get_latest_resume_upload_for_hash.return_value = None
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIsNone(data["task_id"])
        self.assertTrue(data["upload_id"])
        self.assertIn("ready", data["message"].lower())

    @patch("web.backend.routers.pipeline._retry_resume_background")
    @patch("web.backend.routers.pipeline.get_redis_client")
    @patch("web.backend.routers.pipeline.set_task_state")
    def test_retry_resume_creates_new_upload_attempt(self, mock_set_task_state, mock_get_redis, _mock_retry_background):
        redis = MagicMock()
        mock_get_redis.return_value = redis

        with patch("web.backend.routers.pipeline.job_uow") as mock_uow:
            source_upload = SimpleNamespace(
                id="upload-old",
                status="failed_retryable",
                resume_hash="hash-1",
                resume_fingerprint="fp-1",
                original_filename="resume.pdf",
            )
            retry_upload = SimpleNamespace(id="upload-new")
            repo = MagicMock()
            repo.get_resume_upload.return_value = source_upload
            repo.get_structured_resume_by_fingerprint.return_value = object()
            repo.create_resume_upload.return_value = retry_upload
            mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post("/api/pipeline/retry-resume", json={"upload_id": "upload-old"})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["upload_id"], "upload-new")
        self.assertEqual(data["status"], "in_progress")
        self.assertTrue(data["task_id"])
        repo.create_resume_upload.assert_called_once()
        self.assertGreaterEqual(repo.update_resume_upload.call_count, 1)
        mock_set_task_state.assert_called_once()
