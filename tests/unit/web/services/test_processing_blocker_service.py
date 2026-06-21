import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from web.backend.services.processing_blocker_service import ProcessingBlockerService


class _ExecuteResult:
    def __init__(self, *, rows=None, scalar=None):
        self.rows = rows or []
        self.scalar = scalar

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.scalar


class _FakeDb:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def execute(self, stmt):
        self.calls.append(stmt)
        return self.results.pop(0)


def _ready_job():
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=uuid.uuid4(),
        status="active",
        is_extracted=True,
        is_embedded=True,
        first_seen_at=now,
        last_seen_at=now,
    )


def test_matching_blockers_report_missing_resume_context():
    service = ProcessingBlockerService()
    job = _ready_job()
    db = _FakeDb([_ExecuteResult(rows=[job])])

    with patch(
        "web.backend.services.processing_blocker_service.JobRepository",
        return_value=SimpleNamespace(get_latest_ready_resume_fingerprint=Mock(return_value=None)),
    ):
        blockers = service.list_blockers(db, tenant_id=None, stage="matching", limit=10)

    assert len(blockers) == 1
    assert blockers[0].blocker_code == "missing_resume_context"
    assert "no ready resume context" in blockers[0].blocker_detail


def test_matching_blockers_distinguish_not_queued_from_stale_matches():
    service = ProcessingBlockerService()
    stale_job = _ready_job()
    not_queued_job = _ready_job()
    db = _FakeDb([
        _ExecuteResult(rows=[stale_job, not_queued_job]),
        _ExecuteResult(scalar=uuid.uuid4()),
        _ExecuteResult(scalar=None),
    ])

    with patch(
        "web.backend.services.processing_blocker_service.JobRepository",
        return_value=SimpleNamespace(get_latest_ready_resume_fingerprint=Mock(return_value="latest-fp")),
    ):
        blockers = service.list_blockers(db, tenant_id=None, stage="matching", limit=10)

    assert [blocker.blocker_code for blocker in blockers] == [
        "matching_stale",
        "matching_not_queued",
    ]
