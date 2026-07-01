#!/usr/bin/env python3
"""
Additional tests for Pipeline Router - helper functions.
Covers: web/backend/routers/pipeline.py (helper functions)
"""

import asyncio

import pytest
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from types import SimpleNamespace
from uuid import UUID
from unittest.mock import MagicMock, patch

from web.backend.routers.pipeline import _validate_task_id


class TestValidateTaskId:
    """Test _validate_task_id function for input validation."""

    def test_valid_alphanumeric_task_id(self):
        """Test valid alphanumeric task ID."""
        assert _validate_task_id("match-a1b2c3d4") is True
        assert _validate_task_id("task123") is True
        assert _validate_task_id("abc123xyz") is True

    def test_valid_task_id_with_hyphens(self):
        """Test valid task ID with hyphens."""
        assert _validate_task_id("match-task-123") is True
        assert _validate_task_id("test-id-456") is True
        assert _validate_task_id("a-b-c") is True

    def test_valid_max_length_task_id(self):
        """Test valid task ID at max length (50 chars)."""
        max_length_id = "a" * 50
        assert _validate_task_id(max_length_id) is True

    def test_valid_short_task_id(self):
        """Test valid short task ID."""
        assert _validate_task_id("a") is True
        assert _validate_task_id("ab") is True

    def test_invalid_empty_task_id(self):
        """Test empty task ID is invalid."""
        assert _validate_task_id("") is False

    def test_invalid_none_task_id(self):
        """Test None task ID is invalid."""
        assert _validate_task_id(None) is False

    def test_invalid_non_string_task_id(self):
        """Test non-string task ID is invalid."""
        assert _validate_task_id(123) is False
        assert _validate_task_id({"key": "value"}) is False
        assert _validate_task_id(["task"]) is False

    def test_invalid_too_long_task_id(self):
        """Test task ID exceeding max length (50 chars)."""
        too_long_id = "a" * 51
        assert _validate_task_id(too_long_id) is False

        very_long_id = "a" * 100
        assert _validate_task_id(very_long_id) is False

    def test_invalid_special_characters_task_id(self):
        """Test task ID with special characters is invalid."""
        assert _validate_task_id("task@123") is False
        assert _validate_task_id("task#123") is False
        assert _validate_task_id("task$123") is False
        assert _validate_task_id("task%123") is False

    def test_invalid_path_traversal_task_id(self):
        """Test path traversal attempts are invalid."""
        assert _validate_task_id("../etc/passwd") is False
        assert _validate_task_id("..\\..\\windows") is False
        assert _validate_task_id("/etc/passwd") is False
        assert _validate_task_id("C:\\Windows") is False

    def test_invalid_log_injection_task_id(self):
        """Test log injection attempts are invalid."""
        assert _validate_task_id("task\ninjection") is False
        assert _validate_task_id("task\rinjection") is False
        assert _validate_task_id("task\r\ninjection") is False

    def test_invalid_url_manipulation_task_id(self):
        """Test URL manipulation attempts are invalid."""
        assert _validate_task_id("task?id=123") is False
        assert _validate_task_id("task&param=value") is False
        assert _validate_task_id("task;drop") is False


class TestResumeGuards:
    def test_guard_resume_not_uploading_raises_for_active_upload(self):
        from web.backend.routers.pipeline import _guard_resume_not_uploading

        redis = MagicMock()
        redis.get.return_value = b"resume-task-1"

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}), \
             patch("web.backend.routers.pipeline._latest_resume_upload_uses_task", return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                _guard_resume_not_uploading(redis, "owner-1")

        assert exc_info.value.status_code == 409

    def test_guard_resume_not_uploading_ignores_redis_errors(self):
        from web.backend.routers.pipeline import _guard_resume_not_uploading

        redis = MagicMock()
        redis.get.side_effect = RuntimeError("redis down")

        _guard_resume_not_uploading(redis, "owner-1")

    def test_latest_resume_upload_uses_task_returns_false_when_missing(self):
        from web.backend.routers.pipeline import _latest_resume_upload_uses_task

        repo = MagicMock()
        repo.get_latest_resume_upload.return_value = None
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow):
            assert _latest_resume_upload_uses_task("owner-1", "task-1") is False

    def test_latest_resume_upload_uses_task_returns_true_when_repo_errors(self):
        from web.backend.routers.pipeline import _latest_resume_upload_uses_task

        with patch("web.backend.routers.pipeline.job_uow", side_effect=RuntimeError("db down")):
            assert _latest_resume_upload_uses_task("owner-1", "task-1") is True


class TestMatchingTaskHelpers:
    def test_classify_failed_resume_upload_marks_retryable_when_structured_resume_exists(self):
        from web.backend.routers.pipeline import _classify_failed_resume_upload

        repo = MagicMock()
        repo.get_resume_processing_state.return_value = SimpleNamespace(
            user_safe_message="Retry me",
            last_error="boom",
        )
        repo.get_structured_resume_by_fingerprint.return_value = object()

        status, message, retryable = _classify_failed_resume_upload(repo, "fp-1")

        assert status == "failed_retryable"
        assert message == "Retry me"
        assert retryable is True

    def test_classify_failed_resume_upload_marks_reupload_when_no_structured_resume(self):
        from web.backend.routers.pipeline import _classify_failed_resume_upload

        repo = MagicMock()
        repo.get_resume_processing_state.return_value = SimpleNamespace(
            user_safe_message=None,
            last_error="Need reupload",
        )
        repo.get_structured_resume_by_fingerprint.return_value = None

        status, message, retryable = _classify_failed_resume_upload(repo, "fp-1")

        assert status == "failed_reupload_required"
        assert message == "Need reupload"
        assert retryable is False

    def test_ensure_no_active_matching_task_raises_for_running_state(self):
        from web.backend.routers.pipeline import _ensure_no_active_matching_task

        redis = MagicMock()
        redis.get.return_value = b"match-task-1"

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "persisting"}):
            with pytest.raises(HTTPException) as exc_info:
                _ensure_no_active_matching_task(redis, "owner-1")

        assert exc_info.value.status_code == 409

    def test_ensure_no_active_matching_task_raises_for_queued_state(self):
        from web.backend.routers.pipeline import _ensure_no_active_matching_task

        redis = MagicMock()
        redis.get.return_value = b"process-jobs-1"

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "queued"}):
            with pytest.raises(HTTPException) as exc_info:
                _ensure_no_active_matching_task(redis, "owner-1")

        assert exc_info.value.status_code == 409

    def test_ensure_no_active_matching_task_ignores_redis_failure(self):
        from web.backend.routers.pipeline import _ensure_no_active_matching_task

        redis = MagicMock()
        redis.get.side_effect = RuntimeError("redis down")

        _ensure_no_active_matching_task(redis, "owner-1")

    def test_ensure_task_visible_to_owner_allows_active_ownerless_task(self):
        from web.backend.routers.pipeline import _ensure_task_visible_to_owner

        with patch(
            "web.backend.routers.pipeline._active_task_id_for_owner",
            return_value="process-jobs-1",
        ):
            _ensure_task_visible_to_owner(
                {"task_id": "process-jobs-1", "status": "running"},
                "owner-1",
            )

    def test_enqueue_matching_job_or_500_cleans_up_active_marker(self):
        from web.backend.routers.pipeline import _enqueue_matching_job_or_500

        redis = MagicMock()
        redis.get.return_value = b"match-task-1"

        with patch("web.backend.routers.pipeline.enqueue_job", side_effect=RuntimeError("stream down")), \
             patch("web.backend.routers.pipeline.clear_task_cancellation_requested") as mock_clear_cancel, \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set_state:
            with pytest.raises(HTTPException) as exc_info:
                _enqueue_matching_job_or_500(
                    "match-task-1",
                    "fp-1",
                    "upload-1",
                    "owner-1",
                    redis=redis,
                )

        assert exc_info.value.status_code == 500
        mock_clear_cancel.assert_called_once_with("match-task-1")
        mock_set_state.assert_called_once()
        redis.delete.assert_called_once()

    def test_enqueue_matching_for_ready_resume_reuses_active_task(self):
        from web.backend.routers.pipeline import _enqueue_matching_for_ready_resume

        redis = MagicMock()
        redis.get.return_value = b"match-active"

        with patch("web.backend.routers.pipeline._get_matching_redis_client", return_value=redis), \
             patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}), \
             patch("web.backend.routers.pipeline.enqueue_job") as mock_enqueue:
            task_id = _enqueue_matching_for_ready_resume(
                owner_id="00000000-0000-0000-0000-000000000001",
                upload_id="upload-1",
                resume_fingerprint="fp-1",
                trigger="resume_ready",
            )

        assert task_id == "match-active"
        mock_enqueue.assert_not_called()

    def test_auto_enqueue_reuses_latest_matching_task_for_same_upload(self):
        from web.backend.routers.pipeline import _enqueue_matching_for_ready_resume

        redis = MagicMock()
        redis.get.side_effect = [
            None,
            (
                '{"resume_fingerprint":"fp-1","task_id":"manual-task-1",'
                '"trigger":"manual","upload_id":"upload-1"}'
            ),
        ]

        with patch("web.backend.routers.pipeline._get_matching_redis_client", return_value=redis), \
             patch("web.backend.routers.pipeline.enqueue_job") as mock_enqueue:
            task_id = _enqueue_matching_for_ready_resume(
                owner_id="00000000-0000-0000-0000-000000000001",
                upload_id="upload-1",
                resume_fingerprint="fp-1",
                trigger="resume_ready",
            )

        assert task_id == "manual-task-1"
        mock_enqueue.assert_not_called()

    def test_manual_enqueue_ignores_latest_matching_marker(self):
        from web.backend.routers.pipeline import _enqueue_matching_for_ready_resume

        redis = MagicMock()
        redis.get.return_value = None
        redis.set.return_value = True

        with patch("web.backend.routers.pipeline._get_matching_redis_client", return_value=redis), \
             patch("uuid.uuid4", return_value=UUID("11111111-1111-1111-1111-111111111111")), \
             patch("web.backend.routers.pipeline._set_initial_matching_task_state"), \
             patch("web.backend.routers.pipeline._enqueue_matching_job_or_500") as mock_enqueue:
            task_id = _enqueue_matching_for_ready_resume(
                owner_id="00000000-0000-0000-0000-000000000001",
                upload_id="upload-1",
                resume_fingerprint="fp-1",
                trigger="manual",
            )

        assert task_id == "11111111-1111-1111-1111-111111111111"
        mock_enqueue.assert_called_once()
        assert redis.set.call_count >= 2

    def test_resume_status_from_task_state_includes_matching_task_id_and_safe_failure(self):
        from web.backend.routers.pipeline import _resume_status_from_task_state

        response = _resume_status_from_task_state(
            "resume-task-1",
            {
                "status": "completed",
                "step": None,
                "task_type": "resume_upload",
                "owner_id": "00000000-0000-0000-0000-000000000001",
                "matching_task_id": "match-task-1",
                "warnings": [{"code": "matching_enqueue_failed", "message": "raw ignored"}],
            },
        )

        assert response.matching_task_id == "match-task-1"
        assert response.phase == "completed"
        assert response.warnings[0].code == "matching_enqueue_failed"
        assert "raw ignored" not in response.warnings[0].message


class TestResumeEtlHelpers:
    def test_wait_for_resume_etl_final_state_returns_terminal_state(self):
        from web.backend.routers.pipeline import _wait_for_resume_etl_final_state

        states = [
            {"status": "running"},
            {"status": "completed", "step": "embedding"},
        ]

        class FakeTime:
            def __init__(self):
                self.now = 0

            def time(self):
                self.now += 1
                return self.now

            def sleep(self, _seconds):
                return None

        with patch("web.backend.routers.pipeline.get_task_state", side_effect=states):
            result = _wait_for_resume_etl_final_state("task-1", FakeTime())

        assert result["status"] == "completed"

    def test_wait_for_resume_etl_final_state_times_out(self):
        from web.backend.routers.pipeline import _wait_for_resume_etl_final_state

        class FakeTime:
            def __init__(self):
                self.now = 0

            def time(self):
                self.now += 601
                return self.now

            def sleep(self, _seconds):
                return None

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}):
            with pytest.raises(RuntimeError, match="timed out"):
                _wait_for_resume_etl_final_state("task-timeout", FakeTime())

    def test_write_resume_failure_state_updates_repo_and_redis(self):
        from web.backend.routers.pipeline import _write_resume_failure_state

        repo = MagicMock()
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow), \
             patch("web.backend.routers.pipeline._classify_failed_resume_upload", return_value=("failed_retryable", "Retry later", True)), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set_state:
            _write_resume_failure_state(
                "task-1",
                "upload-1",
                "hash-1",
                "fp-1",
                "owner-1",
                RuntimeError("boom"),
            )

        repo.update_resume_upload.assert_called_once()
        mock_set_state.assert_called_once()


class TestPipelineCoveragePushHelpers:
    def test_source_metadata_keywords_fetch_mode_and_provider_helpers(self):
        from web.backend.routers.pipeline import (
            _source_fetch_mode,
            _source_metadata,
            _source_option_keywords,
            _source_provider_name,
            _source_search_keywords,
        )

        cfg = SimpleNamespace(
            search_term="python",
            location="remote",
            country="us",
            options={"fetch_mode": "seed_website", "nested": {"team": "infra"}, "sites": ["a", "A"]},
        )

        assert _source_metadata("custom_board")["display_name"] == "Custom Board"
        assert _source_option_keywords(cfg.options) == ["fetch_mode", "seed_website", "nested", "team", "infra", "sites", "a"]
        assert "python" in _source_search_keywords(
            site_type="custom_board",
            display_name="Custom",
            seed_url="https://example.com/jobs",
            description="Jobs",
            tags=["backend"],
            scraper_cfg=cfg,
        )
        assert _source_fetch_mode("custom_board", cfg, "https://example.com/jobs") == "seed_website"
        assert _source_provider_name(
            "custom_board",
            "seed_website",
            {"configured": True},
        ) == "Worker seed fetcher"
        assert _source_provider_name("greenhouse", "ats_api").endswith("ATS")

    def test_resume_and_task_visibility_helpers(self):
        from web.backend.routers import pipeline

        redis = MagicMock()
        redis.get.return_value = b"resume-task"
        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "processing"}), patch(
            "web.backend.routers.pipeline._latest_resume_upload_uses_task",
            return_value=True,
        ):
            with pytest.raises(Exception, match="Resume is currently"):
                pipeline._guard_resume_not_uploading(redis, "owner-1")

        assert pipeline._resume_task_belongs_to_owner({"owner_id": "owner-1"}, "owner-1") is True
        assert pipeline._resume_task_belongs_to_owner({}, "owner-1") is False

        repo = MagicMock()
        repo.get_resume_upload.return_value = object()
        uow = MagicMock()
        uow.__enter__.return_value = repo
        uow.__exit__.return_value = False
        with patch("web.backend.routers.pipeline.job_uow", return_value=uow):
            assert pipeline._resume_task_belongs_to_owner({"upload_id": "upload-1"}, "owner-1") is True

        with patch("web.backend.routers.pipeline.job_uow", side_effect=RuntimeError("db down")):
            assert pipeline._resume_task_belongs_to_owner({"upload_id": "upload-1"}, "owner-1") is False

        latest_upload = SimpleNamespace(
            status=pipeline.RESUME_UPLOAD_IN_PROGRESS,
            processing_task_id="resume-task",
        )
        repo.get_latest_resume_upload.return_value = latest_upload
        with patch("web.backend.routers.pipeline.job_uow", return_value=uow):
            assert pipeline._latest_resume_upload_uses_task("not-a-uuid", "resume-task") is True
            latest_upload.processing_task_id = "other"
            assert pipeline._latest_resume_upload_uses_task("not-a-uuid", "resume-task") is False

        with patch("web.backend.routers.pipeline.get_task_state", return_value=None):
            with pytest.raises(Exception, match="not found"):
                pipeline._get_owned_resume_task_state("missing", "owner-1")
        with patch("web.backend.routers.pipeline.get_task_state", return_value={"task_type": "matching"}):
            with pytest.raises(Exception, match="not found"):
                pipeline._get_owned_resume_task_state("wrong-type", "owner-1")

    def test_public_status_warning_stats_and_failure_helpers(self):
        from web.backend.routers import pipeline

        assert pipeline._matching_phase_from_step("running", "matching") == "matching_jobs"
        assert pipeline._matching_phase_from_step("running", "persisting") == "initializing"
        assert pipeline._matching_phase_from_step("running", "notify") == "notifying"
        assert pipeline._resume_phase_from_step("processing", "embedding") == "embedding_resume"
        assert pipeline._resume_phase_from_step("processing", "extracting") == "extracting_resume"
        assert pipeline._progress_for_phase("unknown", status="running", phases=("one", "two"))["current_step"] == 1
        assert pipeline._progress_for_phase("two", status="completed", phases=("one", "two"))["percent"] == 100

        warnings = pipeline._safe_warnings_from_state(
            {
                "warnings": ["custom_warning", {"code": "custom_warning"}],
                "stale_due_to_newer_upload": True,
                "status": "completed",
                "task_type": "matching",
            },
            {"jobs_pending_extraction": 1, "matches_saved": 0, "jobs_ready_to_score": 0},
        )
        assert [warning["code"] for warning in warnings] == [
            "custom_warning",
            "stale_resume",
            "jobs_preparing",
            "no_jobs_ready",
        ]

        stats = pipeline._public_stats_from_state(
            {
                "result": {
                    "matches_count": 3,
                    "saved_count": 2,
                    "notified_count": 1,
                    "scraped_jobs": 4,
                    "jobs_processed": 5,
                    "extracted_count": 6,
                    "embedded_count": 7,
                }
            }
        )
        assert stats["jobs_imported"] == 4
        assert stats["jobs_embedded"] == 7

        assert pipeline._public_failure_from_state({"status": "cancelled"}, task_type="matching")["code"] == "cancelled"
        assert pipeline._public_failure_from_state({"status": "failed", "step": "embedding"}, task_type="resume_upload")["code"] == "resume_embedding_failed"
        assert pipeline._public_failure_from_state({"status": "failed", "step": "extracting"}, task_type="resume_upload")["code"] == "resume_parse_failed"
        assert pipeline._public_failure_from_state({"status": "failed", "step": "persisting"}, task_type="matching")["code"] == "matching_failed"
        assert pipeline._resume_status_message("processing", "embedding", None) == "Resume embedding is in progress."

    def test_resume_upload_reconciliation_and_status_helpers(self):
        from web.backend.routers import pipeline

        now = datetime.now(timezone.utc)
        stale_upload = SimpleNamespace(
            id="upload-1",
            owner_id="owner-1",
            status=pipeline.RESUME_UPLOAD_IN_PROGRESS,
            processing_task_id="task-1",
            created_at=now - timedelta(seconds=pipeline.STALE_RESUME_UPLOAD_TIMEOUT_SECONDS + 1),
            resume_hash="hash",
            resume_fingerprint="fp",
            user_safe_message=None,
            last_error=None,
        )
        assert pipeline._resume_upload_timed_out(stale_upload) is True
        assert pipeline._resume_upload_timed_out(SimpleNamespace(created_at=None)) is False

        ready = SimpleNamespace(
            status=pipeline.RESUME_UPLOAD_READY,
            user_safe_message=None,
            last_error=None,
        )
        assert pipeline._resume_status_from_upload("task-1", ready).status == "completed"

        failed = SimpleNamespace(
            status=pipeline.RESUME_UPLOAD_FAILED_RETRYABLE,
            user_safe_message="Retry later",
            last_error="raw",
        )
        assert pipeline._resume_status_from_upload("task-1", failed).status == "failed"
        assert pipeline._resume_status_from_upload("task-1", SimpleNamespace(status="processing")) is None

        repo = MagicMock()
        repo.get_resume_upload.return_value = SimpleNamespace(status=pipeline.RESUME_UPLOAD_READY)
        with patch("web.backend.routers.pipeline.get_task_state", return_value=None), patch(
            "web.backend.routers.pipeline._mark_resume_upload_failed_from_stale_task",
        ) as mark_failed:
            assert pipeline._reconcile_resume_upload_task(repo, stale_upload).status == pipeline.RESUME_UPLOAD_READY
        mark_failed.assert_called_once()

        repo.get_resume_upload.return_value = SimpleNamespace(status=pipeline.RESUME_UPLOAD_READY)
        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "completed"}):
            assert pipeline._reconcile_resume_upload_task(repo, stale_upload).status == pipeline.RESUME_UPLOAD_READY
            repo.update_resume_upload.assert_called()

        with patch("web.backend.routers.pipeline.get_task_state", side_effect=RuntimeError("redis down")):
            assert pipeline._reconcile_resume_upload_task(repo, stale_upload) is stale_upload

    def test_matching_marker_and_active_task_helpers(self):
        from web.backend.routers import pipeline

        assert pipeline._claim_active_task_id(None, "owner-1", "task-1") == "task-1"

        redis = MagicMock()
        redis.set.side_effect = [False, True]
        with patch("web.backend.routers.pipeline._active_matching_task_id", return_value="active-task"):
            assert pipeline._claim_active_task_id(redis, "owner-1", "task-1") == "active-task"
        with patch("web.backend.routers.pipeline._active_matching_task_id", return_value=None):
            assert pipeline._claim_active_task_id(redis, "owner-1", "task-2") == "task-2"

        marker = pipeline._matching_task_marker_payload(
            task_id="task-1",
            upload_id="upload-1",
            resume_fingerprint="fp",
            trigger="manual",
        )
        redis.get.return_value = marker.encode()
        assert pipeline._latest_matching_task_for_upload(
            redis,
            "owner-1",
            upload_id="upload-1",
            resume_fingerprint="fp",
        ) == "task-1"
        assert pipeline._latest_matching_task_for_upload(
            redis,
            "owner-1",
            upload_id="other",
            resume_fingerprint="fp",
        ) is None

        pipeline._store_latest_matching_task_marker(
            redis,
            "owner-1",
            task_id="task-1",
            upload_id="upload-1",
            resume_fingerprint="fp",
            trigger="manual",
        )
        pipeline._clear_latest_matching_task_marker(redis, "owner-1", "task-1")
        redis.delete.assert_called()

        redis.get.return_value = b"active-task"
        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}):
            with pytest.raises(Exception, match="already running"):
                pipeline._ensure_no_active_matching_task(redis, "owner-1")

        with patch("web.backend.routers.pipeline.get_redis_client", side_effect=RuntimeError("redis down")):
            assert pipeline._get_matching_redis_client() is None
            assert pipeline._active_task_id_for_owner("owner-1") is None

    def test_retry_resume_background_marks_ready_after_embed_only_retry(self):
        from web.backend.routers import pipeline

        repo = MagicMock()
        uow = MagicMock()
        uow.__enter__.return_value = repo
        uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.set_task_state") as set_state, patch(
            "web.backend.services.clients.orchestrator_client"
        ) as client, patch(
            "web.backend.routers.pipeline._wait_for_resume_etl_final_state",
            return_value={"status": "completed"},
        ) as wait_final, patch(
            "web.backend.routers.pipeline.job_uow",
            return_value=uow,
        ), patch(
            "web.backend.routers.pipeline._write_resume_ready_state"
        ) as write_ready:
            pipeline._retry_resume_background(
                "task-1",
                "upload-1",
                "owner-1",
                "fp-1",
                "hash-1",
            )

        set_state.assert_called_once()
        assert set_state.call_args.args[1]["phase"] == "embedding_resume"
        client.process_resume.assert_called_once_with(
            None,
            "task-1",
            upload_id="upload-1",
            owner_id="owner-1",
            resume_fingerprint="fp-1",
            mode="embed_only",
        )
        wait_final.assert_called_once()
        repo.update_resume_upload.assert_called_once_with(
            "upload-1",
            status=pipeline.RESUME_UPLOAD_READY,
            last_error=None,
            processing_task_id="task-1",
            retryable=False,
            user_safe_message=pipeline.RESUME_PROCESSING_COMPLETED_MESSAGE,
        )
        write_ready.assert_called_once_with(
            task_id="task-1",
            upload_id="upload-1",
            owner_id="owner-1",
            resume_hash="hash-1",
            resume_fingerprint="fp-1",
            trigger="resume_retry_ready",
        )

    def test_retry_resume_background_writes_failure_for_failed_retry(self):
        from web.backend.routers import pipeline

        with patch("web.backend.routers.pipeline.set_task_state"), patch(
            "web.backend.services.clients.orchestrator_client"
        ) as client, patch(
            "web.backend.routers.pipeline._write_resume_failure_state"
        ) as write_failure:
            client.process_resume.side_effect = RuntimeError("embed failed")

            pipeline._retry_resume_background(
                "task-1",
                "upload-1",
                "owner-1",
                "fp-1",
                "hash-1",
            )

        write_failure.assert_called_once()
        assert write_failure.call_args.args[:5] == (
            "task-1",
            "upload-1",
            "hash-1",
            "fp-1",
            "owner-1",
        )

    def test_get_pipeline_status_proxies_to_orchestrator_for_active_task(self):
        from web.backend.routers import pipeline

        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_task_state",
            return_value=None,
        ), patch(
            "web.backend.routers.pipeline._active_task_id_for_owner",
            return_value="task-1",
        ), patch(
            "web.backend.services.clients.orchestrator_client"
        ) as client:
            client.get_task_status.return_value = {
                "success": True,
                "status": "running",
                "current_stage": "matching",
                "result": {"matches_count": 2, "saved_count": 1},
            }

            response = pipeline.get_pipeline_status("task-1", user=object())

        assert response.status == "running"
        assert response.step == "vector_matching"
        assert response.matches_count == 2
        assert response.saved_count == 1

    def test_get_pipeline_status_returns_not_found_when_orchestrator_rejects_active_task(self):
        from web.backend.routers import pipeline

        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_task_state",
            return_value=None,
        ), patch(
            "web.backend.routers.pipeline._active_task_id_for_owner",
            return_value="task-1",
        ), patch(
            "web.backend.services.clients.orchestrator_client"
        ) as client:
            client.get_task_status.return_value = {"success": False}

            response = pipeline.get_pipeline_status("task-1", user=object())

        assert response.status_code == 404

    def test_get_pipeline_status_returns_lookup_failure_when_orchestrator_errors(self):
        from web.backend.routers import pipeline

        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_task_state",
            return_value=None,
        ), patch(
            "web.backend.routers.pipeline._active_task_id_for_owner",
            return_value="task-1",
        ), patch(
            "web.backend.services.clients.orchestrator_client"
        ) as client:
            client.get_task_status.side_effect = RuntimeError("orchestrator down")

            response = pipeline.get_pipeline_status("task-1", user=object())

        assert response.status_code == 500

    def test_get_active_task_covers_empty_terminal_and_active_states(self):
        from web.backend.routers import pipeline

        redis = MagicMock()
        redis.get.return_value = None
        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_redis_client",
            return_value=redis,
        ):
            assert pipeline._get_active_task(user=object()) is None

        redis.get.return_value = b"task-1"
        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_redis_client",
            return_value=redis,
        ), patch(
            "web.backend.routers.pipeline.get_task_state",
            return_value={"status": "completed"},
        ):
            assert pipeline._get_active_task(user=object()) is None

        with patch("web.backend.routers.pipeline.resolve_owner_id", return_value="owner-1"), patch(
            "web.backend.routers.pipeline.get_redis_client",
            return_value=redis,
        ), patch(
            "web.backend.routers.pipeline.get_task_state",
            return_value={"status": "running", "step": "matching", "owner_id": "owner-1"},
        ):
            active = pipeline._get_active_task(user=object())
        assert active.status == "running"
        assert active.step == "vector_matching"

        with patch("web.backend.routers.pipeline.resolve_owner_id", side_effect=RuntimeError("bad user")):
            assert pipeline._get_active_task(user=object()) is None

    def test_stream_orchestrator_sse_rejects_invalid_uuid_and_handles_connection_error(self):
        from web.backend.routers import pipeline

        async def collect_invalid():
            return [
                chunk
                async for chunk in pipeline._stream_orchestrator_sse(
                    "http://orchestrator",
                    "not-a-uuid",
                )
            ]

        assert asyncio.run(collect_invalid()) == []

        async def collect_connection_error():
            with patch("httpx.AsyncClient", side_effect=RuntimeError("network down")):
                return [
                    chunk
                    async for chunk in pipeline._stream_orchestrator_sse(
                        "http://orchestrator",
                        "00000000-0000-4000-8000-000000000401",
                    )
                ]

        chunks = asyncio.run(collect_connection_error())
        assert len(chunks) == 1
        assert "Failed to connect to pipeline service" in chunks[0]

    def test_preflight_task_check_raises_only_for_definitive_404(self):
        from web.backend.routers import pipeline

        class FakeProbe:
            def __init__(self, status_code=None, error=None, *args, **kwargs):
                self.status_code = status_code
                self.error = error

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def get(self, _url):
                if self.error:
                    raise self.error
                return SimpleNamespace(status_code=self.status_code)

        with patch("httpx.AsyncClient", return_value=FakeProbe(status_code=404)):
            with pytest.raises(HTTPException) as exc_info:
                asyncio.run(
                    pipeline._preflight_task_check(
                        "http://orchestrator",
                        "00000000-0000-4000-8000-000000000402",
                    )
                )
        assert exc_info.value.status_code == 404

        with patch(
            "httpx.AsyncClient",
            return_value=FakeProbe(error=RuntimeError("temporary outage")),
        ):
            assert asyncio.run(
                pipeline._preflight_task_check(
                    "http://orchestrator",
                    "00000000-0000-4000-8000-000000000403",
                )
            ) is None
