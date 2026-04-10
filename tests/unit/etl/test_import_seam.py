from __future__ import annotations

from unittest.mock import Mock

from etl.import_models import ImportSourceDescriptor, NormalizedJobRecord
from etl.orchestrator import JobETLService


def test_import_record_prefers_existing_source_identity() -> None:
    service = JobETLService(ai_service=None)  # type: ignore[arg-type]
    repo = Mock()
    existing_job = Mock()
    existing_job.id = "job-123"
    repo.get_by_source.return_value = existing_job
    repo.get_by_fingerprint.return_value = None

    record = NormalizedJobRecord(
        title="Backend Engineer",
        company_name="Acme",
        location="Remote",
        description="Build APIs",
        source=ImportSourceDescriptor(
            provider="greenhouse",
            site_name="ats:test",
            source_key="board",
            external_job_id="job-1",
            source_url="https://example.com/jobs/1",
        ),
        tenant_id="tenant-123",
    )

    service.import_record(repo, record)

    repo.get_by_source.assert_called_once_with("ats:test", "https://example.com/jobs/1")
    repo.get_by_fingerprint.assert_not_called()
    repo.update_timestamp.assert_called_once_with(existing_job)
    repo.create_job_post.assert_not_called()


def test_import_record_passes_tenant_id_when_creating_job() -> None:
    service = JobETLService(ai_service=None)  # type: ignore[arg-type]
    repo = Mock()
    repo.get_by_source.return_value = None
    repo.get_by_fingerprint.return_value = None
    created_job = Mock()
    created_job.id = "job-456"
    repo.create_job_post.return_value = created_job

    record = NormalizedJobRecord(
        title="Platform Engineer",
        company_name="Acme",
        location="Seoul",
        description="Maintain cloud runtime",
        source=ImportSourceDescriptor(
            provider="ashby",
            site_name="ats:tenant-1",
            source_key="ashby-board",
            external_job_id="ashby-1",
        ),
        tenant_id="tenant-1",
    )

    service.import_record(repo, record)

    assert repo.create_job_post.call_args.kwargs["tenant_id"] == "tenant-1"
    repo.get_or_create_source.assert_called_once()
    repo.save_job_content.assert_called_once()
