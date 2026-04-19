"""Unit tests for the legacy helper module in ``main.py``."""

from __future__ import annotations

import importlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

with patch("signal.signal"):
    legacy_main = importlib.import_module("main")


def _context_manager(value: object) -> MagicMock:
    manager = MagicMock()
    manager.__enter__.return_value = value
    manager.__exit__.return_value = False
    return manager


def test_signal_handler_sets_global_stop_event() -> None:
    event = threading.Event()

    with patch.object(legacy_main, "stop_event", event):
        legacy_main.signal_handler(None, None)

    assert event.is_set()


def test_load_resume_data_returns_structured_payload() -> None:
    parsed = SimpleNamespace(data={"name": "Test User"}, format="json", text="")

    with patch.object(legacy_main, "ResumeParser") as parser_cls:
        parser_cls.return_value.parse.return_value = parsed

        assert legacy_main.load_resume_data("resume.json") == {"name": "Test User"}


def test_load_resume_data_wraps_text_payload() -> None:
    parsed = SimpleNamespace(data=None, format="pdf", text="resume body")

    with patch.object(legacy_main, "ResumeParser") as parser_cls:
        parser_cls.return_value.parse.return_value = parsed

        assert legacy_main.load_resume_data("resume.pdf") == {"raw_text": "resume body"}


def test_load_resume_data_returns_none_for_file_not_found() -> None:
    with patch.object(legacy_main, "ResumeParser") as parser_cls:
        parser_cls.return_value.parse.side_effect = FileNotFoundError()

        assert legacy_main.load_resume_data("missing.json") is None


def test_load_resume_data_returns_none_for_value_error() -> None:
    with patch.object(legacy_main, "ResumeParser") as parser_cls:
        parser_cls.return_value.parse.side_effect = ValueError("bad format")
        parser_cls.get_supported_formats.return_value = [".json", ".yaml"]

        assert legacy_main.load_resume_data("broken.resume") is None


def test_load_resume_data_returns_none_for_unexpected_exception() -> None:
    with patch.object(legacy_main, "ResumeParser") as parser_cls:
        parser_cls.return_value.parse.side_effect = RuntimeError("boom")

        assert legacy_main.load_resume_data("broken.resume") is None


def test_run_job_etl_returns_immediately_when_stopped() -> None:
    ctx = SimpleNamespace(
        config=SimpleNamespace(scrapers=[SimpleNamespace(site_type="indeed")]),
        jobspy_client=MagicMock(),
        job_etl_service=MagicMock(),
    )
    stop_event = threading.Event()
    stop_event.set()

    legacy_main.run_job_etl(ctx, stop_event)

    ctx.jobspy_client.submit_scrape.assert_not_called()


def test_run_job_etl_happy_path_ingests_jobs_and_runs_orchestrator() -> None:
    scraper_cfg = SimpleNamespace(site_type="indeed", request_timeout=15)
    ctx = SimpleNamespace(
        config=SimpleNamespace(scrapers=[scraper_cfg]),
        jobspy_client=MagicMock(),
        job_etl_service=MagicMock(),
    )
    ctx.jobspy_client.submit_scrape.return_value = "scrape-task"
    ctx.jobspy_client.wait_for_result.return_value = [{"id": 1}, {"id": 2}]
    repo = MagicMock()

    with (
        patch.object(legacy_main, "job_uow", return_value=_context_manager(repo)),
        patch.object(
            legacy_main.orchestrator_client,
            "start_stage",
            side_effect=[
                {"success": True, "task_id": "extract-task"},
                {"success": True, "task_id": "embed-task"},
            ],
        ) as start_stage,
        patch.object(
            legacy_main.orchestrator_client,
            "wait_for_completion",
            side_effect=[
                {"status": "completed"},
                {"status": "completed"},
            ],
        ) as wait_for_completion,
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
    ):
        legacy_main.run_job_etl(ctx, threading.Event())

    wait_kwargs = ctx.jobspy_client.wait_for_result.call_args.kwargs
    assert wait_kwargs["request_timeout_s"] == 15
    assert isinstance(wait_kwargs["stop_event"], threading.Event)
    assert ctx.job_etl_service.ingest_one.call_count == 2
    start_stage.assert_any_call("extract", limit=200)
    start_stage.assert_any_call("embed", limit=100)
    assert wait_for_completion.call_count == 2


def test_run_job_etl_logs_scraper_and_ingest_failures_then_aborts_without_orchestrator_url() -> None:
    scraper_error = SimpleNamespace(site_type="linkedin", request_timeout=None)
    scraper_ingest = SimpleNamespace(site_type="indeed", request_timeout=None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(scrapers=[scraper_error, scraper_ingest]),
        jobspy_client=MagicMock(),
        job_etl_service=MagicMock(),
    )
    ctx.jobspy_client.submit_scrape.side_effect = [RuntimeError("scrape failed"), "scrape-task"]
    ctx.jobspy_client.wait_for_result.return_value = [{"id": 1}]
    ctx.job_etl_service.ingest_one.side_effect = RuntimeError("ingest failed")
    repo = MagicMock()

    with (
        patch.object(legacy_main, "job_uow", return_value=_context_manager(repo)),
        patch.object(legacy_main.logger, "exception") as log_exception,
        patch.object(legacy_main.orchestrator_client, "start_stage") as start_stage,
        patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False),
    ):
        legacy_main.run_job_etl(ctx, threading.Event())

    log_exception.assert_called_once()
    start_stage.assert_not_called()


def test_run_job_etl_aborts_when_extraction_stage_fails() -> None:
    scraper_cfg = SimpleNamespace(site_type="indeed", request_timeout=None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(scrapers=[scraper_cfg]),
        jobspy_client=MagicMock(),
        job_etl_service=MagicMock(),
    )
    ctx.jobspy_client.submit_scrape.return_value = "scrape-task"
    ctx.jobspy_client.wait_for_result.return_value = []

    with (
        patch.object(
            legacy_main.orchestrator_client,
            "start_stage",
            return_value={"success": False, "task_id": "extract-task"},
        ) as start_stage,
        patch.object(legacy_main.orchestrator_client, "wait_for_completion") as wait_for_completion,
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
    ):
        legacy_main.run_job_etl(ctx, threading.Event())

    start_stage.assert_called_once_with("extract", limit=200)
    wait_for_completion.assert_not_called()


def test_run_job_etl_aborts_when_embedding_stage_errors() -> None:
    scraper_cfg = SimpleNamespace(site_type="indeed", request_timeout=None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(scrapers=[scraper_cfg]),
        jobspy_client=MagicMock(),
        job_etl_service=MagicMock(),
    )
    ctx.jobspy_client.submit_scrape.return_value = "scrape-task"
    ctx.jobspy_client.wait_for_result.return_value = []

    with (
        patch.object(
            legacy_main.orchestrator_client,
            "start_stage",
            side_effect=[
                {"success": True, "task_id": "extract-task"},
                {"success": True, "task_id": "embed-task"},
            ],
        ),
        patch.object(
            legacy_main.orchestrator_client,
            "wait_for_completion",
            side_effect=[
                {"status": "completed"},
                RuntimeError("embed boom"),
            ],
        ),
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
    ):
        legacy_main.run_job_etl(ctx, threading.Event())


def test_run_resume_etl_logs_wrapper_messages() -> None:
    ctx = SimpleNamespace()

    with patch.object(legacy_main, "_run_resume_etl", return_value=True) as run_impl:
        legacy_main.run_resume_etl(ctx)

    run_impl.assert_called_once_with(ctx)


def test_run_resume_etl_returns_false_without_etl_config() -> None:
    ctx = SimpleNamespace(config=SimpleNamespace(etl=None))

    assert legacy_main._run_resume_etl(ctx) is False


def test_run_resume_etl_returns_false_without_resume_file() -> None:
    etl_config = SimpleNamespace(resume=None, resume_file=None)
    ctx = SimpleNamespace(config=SimpleNamespace(etl=etl_config))

    assert legacy_main._run_resume_etl(ctx) is False


def test_run_resume_etl_supports_backward_compatible_resume_file() -> None:
    etl_config = SimpleNamespace(resume=None, resume_file="resume.json")
    ctx = SimpleNamespace(config=SimpleNamespace(etl=etl_config))

    with (
        patch.dict("os.environ", {"EXTRACTION_URL": "http://extractor"}, clear=False),
        patch.object(legacy_main.os.path, "isabs", return_value=False),
        patch.object(legacy_main.os, "getcwd", return_value="/workspace"),
        patch.object(legacy_main.extraction_client, "extract_resume", return_value={"success": True}) as extract_resume,
    ):
        assert legacy_main._run_resume_etl(ctx) is True

    assert extract_resume.call_args.kwargs["resume_file"] == "/workspace/resume.json"
    assert extract_resume.call_args.kwargs["force_re_extraction"] is False


def test_run_resume_etl_passes_force_re_extraction_flag() -> None:
    resume_cfg = SimpleNamespace(
        resume_file="/tmp/resume.json",
        force_re_extraction=True,
    )
    etl_config = SimpleNamespace(resume=resume_cfg, resume_file=None)
    ctx = SimpleNamespace(config=SimpleNamespace(etl=etl_config))

    with (
        patch.dict("os.environ", {"EXTRACTION_URL": "http://extractor"}, clear=False),
        patch.object(legacy_main.extraction_client, "extract_resume", return_value={"success": True}) as extract_resume,
    ):
        assert legacy_main._run_resume_etl(ctx) is True

    assert extract_resume.call_args.kwargs["resume_file"] == "/tmp/resume.json"
    assert extract_resume.call_args.kwargs["force_re_extraction"] is True


def test_run_resume_etl_returns_false_without_extraction_url() -> None:
    resume_cfg = SimpleNamespace(resume_file="/tmp/resume.json", force_re_extraction=False)
    ctx = SimpleNamespace(config=SimpleNamespace(etl=SimpleNamespace(resume=resume_cfg, resume_file=None)))

    with patch.dict("os.environ", {"EXTRACTION_URL": ""}, clear=False):
        assert legacy_main._run_resume_etl(ctx) is False


def test_run_resume_etl_returns_false_when_extraction_client_raises() -> None:
    resume_cfg = SimpleNamespace(resume_file="/tmp/resume.json", force_re_extraction=False)
    ctx = SimpleNamespace(config=SimpleNamespace(etl=SimpleNamespace(resume=resume_cfg, resume_file=None)))

    with (
        patch.dict("os.environ", {"EXTRACTION_URL": "http://extractor"}, clear=False),
        patch.object(legacy_main.extraction_client, "extract_resume", side_effect=RuntimeError("boom")),
    ):
        assert legacy_main._run_resume_etl(ctx) is False


def test_run_matching_pipeline_returns_false_without_orchestrator_url() -> None:
    with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False):
        assert legacy_main.run_matching_pipeline(SimpleNamespace()) == (False, "")


def test_run_matching_pipeline_returns_false_when_start_fails() -> None:
    with (
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
        patch.object(
            legacy_main.orchestrator_client,
            "start_matching",
            return_value={"success": False, "message": "nope"},
        ),
    ):
        assert legacy_main.run_matching_pipeline(SimpleNamespace()) == (False, "")


def test_run_matching_pipeline_returns_false_when_start_raises() -> None:
    with (
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
        patch.object(
            legacy_main.orchestrator_client,
            "start_matching",
            side_effect=RuntimeError("boom"),
        ),
    ):
        assert legacy_main.run_matching_pipeline(SimpleNamespace()) == (False, "")


def test_run_matching_pipeline_delegates_to_wait_helper() -> None:
    with (
        patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://orchestrator"}, clear=False),
        patch.object(
            legacy_main.orchestrator_client,
            "start_matching",
            return_value={"success": True, "task_id": "task-1"},
        ),
        patch.object(legacy_main, "_wait_for_orchestrator_result", return_value=(True, "task-1")) as wait_helper,
    ):
        assert legacy_main.run_matching_pipeline(SimpleNamespace()) == (True, "task-1")

    wait_helper.assert_called_once_with("task-1")


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"status": "completed", "result": {"matches_count": 3}}, (True, "task-1")),
        ({"status": "failed", "result": {"error": "boom"}}, (False, "task-1")),
        ({"status": "cancelled"}, (False, "task-1")),
        ({}, (False, "task-1")),
    ],
)
def test_wait_for_orchestrator_result_statuses(result: dict[str, object], expected: tuple[bool, str]) -> None:
    with patch.object(legacy_main.orchestrator_client, "wait_for_completion", return_value=result):
        assert legacy_main._wait_for_orchestrator_result("task-1") == expected


def test_run_internal_sequential_cycle_runs_all_phases_and_cleans_up() -> None:
    ctx = SimpleNamespace(jobspy_client=MagicMock())
    config = SimpleNamespace(name="config")

    with (
        patch.object(legacy_main, "load_config", return_value=config),
        patch.object(legacy_main.AppContext, "build", return_value=ctx) as build_ctx,
        patch.object(legacy_main, "_run_job_etl_phase") as run_job,
        patch.object(legacy_main, "_run_resume_etl_phase") as run_resume,
        patch.object(legacy_main, "_run_matching_phase") as run_matching,
        patch.object(legacy_main, "_cleanup_jobspy_client") as cleanup,
    ):
        legacy_main.run_internal_sequential_cycle(mode="all")

    build_ctx.assert_called_once_with(config)
    run_job.assert_called_once()
    run_resume.assert_called_once()
    run_matching.assert_called_once()
    cleanup.assert_called_once_with(ctx)


def test_run_internal_sequential_cycle_stops_after_job_phase_when_requested() -> None:
    ctx = SimpleNamespace(jobspy_client=MagicMock())
    stop_event = threading.Event()

    def stop_after_job(_ctx: object, current_stop_event: threading.Event) -> None:
        current_stop_event.set()

    with (
        patch.object(legacy_main.AppContext, "build", return_value=ctx),
        patch.object(legacy_main, "_run_job_etl_phase", side_effect=stop_after_job),
        patch.object(legacy_main, "_run_resume_etl_phase") as run_resume,
        patch.object(legacy_main, "_run_matching_phase") as run_matching,
        patch.object(legacy_main, "_cleanup_jobspy_client"),
    ):
        legacy_main.run_internal_sequential_cycle(
            mode="all",
            stop_event=stop_event,
            config=SimpleNamespace(),
        )

    run_resume.assert_not_called()
    run_matching.assert_not_called()


def test_phase_helpers_unload_models_when_service_is_available() -> None:
    ctx = SimpleNamespace(job_etl_service=MagicMock())

    with patch.object(legacy_main, "run_job_etl") as run_job:
        legacy_main._run_job_etl_phase(ctx, threading.Event())
    run_job.assert_called_once()
    ctx.job_etl_service.unload_models.assert_called_once()

    ctx.job_etl_service.unload_models.reset_mock()
    with patch.object(legacy_main, "run_resume_etl") as run_resume:
        legacy_main._run_resume_etl_phase(ctx, threading.Event())
    run_resume.assert_called_once_with(ctx)
    ctx.job_etl_service.unload_models.assert_called_once()


def test_matching_phase_logs_failures_and_unloads_models_on_success() -> None:
    ctx = SimpleNamespace(job_etl_service=MagicMock())

    with patch.object(legacy_main, "run_matching_pipeline", return_value=(True, "task-1")) as run_matching:
        legacy_main._run_matching_phase(ctx, threading.Event())

    run_matching.assert_called_once()
    ctx.job_etl_service.unload_models.assert_called_once()


def test_phase_helpers_swallow_exceptions() -> None:
    ctx = SimpleNamespace(job_etl_service=MagicMock())

    with patch.object(legacy_main, "run_job_etl", side_effect=RuntimeError("boom")):
        legacy_main._run_job_etl_phase(ctx, threading.Event())

    with patch.object(legacy_main, "run_resume_etl", side_effect=RuntimeError("boom")):
        legacy_main._run_resume_etl_phase(ctx, threading.Event())

    with patch.object(legacy_main, "run_matching_pipeline", side_effect=RuntimeError("boom")):
        legacy_main._run_matching_phase(ctx, threading.Event())


def test_cleanup_jobspy_client_handles_close_exceptions() -> None:
    client = MagicMock()
    client.close.side_effect = RuntimeError("boom")
    ctx = SimpleNamespace(jobspy_client=client)

    legacy_main._cleanup_jobspy_client(ctx)

    client.close.assert_called_once()


def test_main_raises_system_exit_with_guidance() -> None:
    with pytest.raises(SystemExit, match="microservice stack"):
        legacy_main.main()
