from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.orchestrator.description_recovery import (
    ProviderJob,
    recover_missing_description_job,
    recover_missing_description_jobs,
    resolve_ats_binding,
)


def _job(**overrides):
    source = overrides.pop(
        "source",
        SimpleNamespace(
            id="source-1",
            site="greenhouse",
            source_job_id="123",
            job_url="https://job-boards.greenhouse.io/acme/jobs/123",
            job_url_direct=None,
            is_active=True,
            last_seen_at=None,
        ),
    )
    defaults = {
        "id": "job-1",
        "title": "Backend Engineer",
        "company": "Acme",
        "location_text": "Remote",
        "is_remote": True,
        "description": None,
        "extraction_status": "no_description",
        "description_recovery_attempts": 0,
        "raw_payload": {"source_provider": "greenhouse", "source_key": "acme"},
        "sources": [source] if source is not None else [],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _repo():
    repo = Mock()
    repo.mark_description_recovery_status.side_effect = (
        lambda job, *, status, reason, run_id=None, error=None, retryable=False: setattr(
            job,
            "description_recovery_status",
            "failed_retryable" if retryable else status,
        )
    )
    repo.mark_description_recovery_refreshing.side_effect = (
        lambda job, *, run_id: setattr(job, "description_recovery_status", "refreshing")
    )
    repo.mark_description_recovered.side_effect = (
        lambda job, *, run_id, reason="description_found": setattr(
            job,
            "description_recovery_status",
            "description_found",
        )
    )
    return repo


def test_greenhouse_recovery_saves_description_and_marks_recovered():
    job = _job()
    repo = _repo()
    provider_job = ProviderJob(
        source_job_id="123",
        title="Backend Engineer",
        company_name=None,
        location="Remote",
        description="Full backend engineer description.",
        job_url="https://job-boards.greenhouse.io/acme/jobs/123",
        job_url_direct="https://job-boards.greenhouse.io/acme/jobs/123",
        raw_provider="greenhouse",
    )

    with patch(
        "services.orchestrator.description_recovery.fetch_provider_jobs",
        return_value=[provider_job],
    ):
        result = recover_missing_description_job(repo, job, run_id="run-1")

    assert result["outcome"] == "description_found"
    repo.save_job_content.assert_called_once()
    saved_job_id, saved_payload = repo.save_job_content.call_args.args
    assert saved_job_id == "job-1"
    assert saved_payload["description"] == "Full backend engineer description."
    assert saved_payload["source_metadata"]["description_source"] == "ats_description_recovery"
    assert saved_payload["source_metadata"]["description_provider"] == "greenhouse"
    repo.mark_description_recovered.assert_called_once_with(job, run_id="run-1")


def test_page_recovery_groups_jobs_by_ats_source():
    job_one = _job()
    job_two = _job(
        id="job-2",
        source=SimpleNamespace(
            id="source-2",
            site="greenhouse",
            source_job_id="456",
            job_url="https://job-boards.greenhouse.io/acme/jobs/456",
            job_url_direct=None,
            is_active=True,
            last_seen_at=None,
        ),
    )
    repo = _repo()
    provider_jobs = [
        ProviderJob(
            source_job_id="123",
            title="Backend Engineer",
            company_name=None,
            location="Remote",
            description="First backend engineer description.",
            job_url="https://job-boards.greenhouse.io/acme/jobs/123",
            job_url_direct="https://job-boards.greenhouse.io/acme/jobs/123",
            raw_provider="greenhouse",
        ),
        ProviderJob(
            source_job_id="456",
            title="Platform Engineer",
            company_name=None,
            location="Remote",
            description="Second backend engineer description.",
            job_url="https://job-boards.greenhouse.io/acme/jobs/456",
            job_url_direct="https://job-boards.greenhouse.io/acme/jobs/456",
            raw_provider="greenhouse",
        ),
    ]

    with patch(
        "services.orchestrator.description_recovery.fetch_provider_jobs",
        return_value=provider_jobs,
    ) as fetch:
        stats = recover_missing_description_jobs(
            repo,
            [job_one, job_two],
            run_id="run-page",
        )

    assert fetch.call_count == 1
    assert stats["processed"] == 2
    assert stats["description_found"] == 2
    assert stats["description_found_job_ids"] == ["job-1", "job-2"]
    assert stats["provider_breakdown"]["greenhouse"]["description_found"] == 2
    assert repo.mark_description_recovery_refreshing.call_count == 2
    assert repo.save_job_content.call_count == 2


def test_absent_provider_posting_marks_source_not_found():
    source = SimpleNamespace(
        id="source-1",
        site="lever",
        source_job_id="gone",
        job_url="https://jobs.lever.co/acme/gone",
        job_url_direct=None,
        is_active=True,
        last_seen_at=None,
    )
    job = _job(
        source=source,
        raw_payload={"source_provider": "lever", "source_key": "acme"},
    )
    repo = _repo()

    with patch(
        "services.orchestrator.description_recovery.fetch_provider_jobs",
        return_value=[
            ProviderJob(
                source_job_id="different",
                title="Other",
                company_name=None,
                location=None,
                description="Other role",
                job_url="https://jobs.lever.co/acme/different",
                job_url_direct=None,
                raw_provider="lever",
            )
        ],
    ):
        result = recover_missing_description_job(repo, job, run_id="run-2")

    assert result["outcome"] == "posting_not_found"
    repo.mark_description_recovery_posting_not_found.assert_called_once_with(
        job,
        source=source,
        run_id="run-2",
    )
    repo.save_job_content.assert_not_called()


def test_prohibited_source_is_terminal_and_never_fetches_provider():
    job = _job(raw_payload={"source_provider": "tokyodev"}, sources=[])
    repo = _repo()

    with patch("services.orchestrator.description_recovery.fetch_provider_jobs") as fetch:
        result = recover_missing_description_job(repo, job, run_id="run-3")

    assert result["outcome"] == "source_prohibited"
    fetch.assert_not_called()
    repo.mark_description_recovery_status.assert_called_once_with(
        job,
        status="source_prohibited",
        reason="source_prohibited",
        run_id="run-3",
    )


def test_unmapped_ats_source_needs_configuration():
    source = SimpleNamespace(
        id="source-1",
        site="greenhouse",
        source_job_id=None,
        job_url=None,
        job_url_direct=None,
        is_active=True,
        last_seen_at=None,
    )
    job = _job(source=source, raw_payload={"source_provider": "greenhouse"})

    assert resolve_ats_binding(job, source) == "source_unmapped"


def test_workday_source_reports_adapter_missing_and_never_fetches_provider():
    job = _job(raw_payload={"source_provider": "workday"}, sources=[])
    repo = _repo()

    with patch("services.orchestrator.description_recovery.fetch_provider_jobs") as fetch:
        result = recover_missing_description_job(repo, job, run_id="run-4")

    assert result["outcome"] == "source_adapter_missing"
    fetch.assert_not_called()
    repo.mark_description_recovery_status.assert_called_once_with(
        job,
        status="source_adapter_missing",
        reason="source_adapter_missing",
        run_id="run-4",
    )
