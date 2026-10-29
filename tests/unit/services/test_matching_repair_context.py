"""Scheduled matching must discover and process each RLS owner independently."""

from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import Mock

import pytest

from database.database import current_database_user_id, worker_database_context
from services.orchestrator import repair


def test_cloud_discovery_pages_without_reading_private_resume_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repair, "database_context_signing_enabled", lambda: True)
    pages = [[{"cursor_value": str(i)} for i in range(100)], [{"cursor_value": "last"}]]
    session = Mock()
    session.execute.return_value.mappings.return_value.all.side_effect = pages

    @contextmanager
    def session_scope() -> Iterator[Mock]:
        yield session

    monkeypatch.setattr(repair, "db_session_scope", session_scope)
    monkeypatch.setattr(repair, "JobRepository", Mock(side_effect=AssertionError("private lookup")))
    assert len(list(repair._ready_matching_targets())) == 101
    calls = session.execute.call_args_list
    assert [call.args[1] for call in calls] == [{"cursor": None}, {"cursor": "99"}]
    assert all("jobscout_worker_ready_resume_candidates" in str(call.args[0]) for call in calls)


def test_matching_repair_uses_each_owner_and_propagates_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [
        {"owner_id": "owner-a", "tenant_id": "tenant-a", "resume_fingerprint": "fp-a", "resume_upload_id": "upload-a"},
        {"owner_id": "owner-b", "tenant_id": "tenant-b", "resume_fingerprint": "fp-b"},
    ]
    monkeypatch.setattr(repair, "_ready_matching_targets", lambda: iter(targets))
    contexts = []

    @contextmanager
    def session_scope() -> Iterator[Mock]:
        contexts.append(current_database_user_id())
        yield Mock()

    repo = Mock()
    repo.count_pending_matching_jobs.side_effect = [3, 0]

    def preferences(_repo: Any, owner_id: str) -> dict:
        assert current_database_user_id() == owner_id
        return {"remote_mode": owner_id}

    enqueue = Mock()
    monkeypatch.setattr(repair, "db_session_scope", session_scope)
    monkeypatch.setattr(repair, "JobRepository", lambda _session: repo)
    monkeypatch.setattr(repair, "_load_candidate_preferences", preferences)
    monkeypatch.setattr(repair, "enqueue_job", enqueue)
    with worker_database_context(user_id="previous-owner", tenant_id="previous-tenant"):
        assert repair._enqueue_matching_repairs("repair", {"pipeline_run_id": "run"}) == 3
        assert current_database_user_id() == "previous-owner"
    assert contexts == ["owner-a", "owner-b"]
    assert [call.kwargs["tenant_id"] for call in repo.count_pending_matching_jobs.call_args_list] == ["tenant-a", "tenant-b"]
    enqueue.assert_called_once_with("matching:jobs", {
        "task_id": "repair-match-0", "resume_fingerprint": "fp-a",
        "owner_id": "owner-a", "tenant_id": "tenant-a", "resume_upload_id": "upload-a",
        "pipeline_run_id": "run",
    })


def test_discovery_failure_is_not_silently_treated_as_empty_backlog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repair, "database_context_signing_enabled", lambda: True)
    session = Mock()
    session.execute.side_effect = RuntimeError("discovery unavailable")

    @contextmanager
    def session_scope() -> Iterator[Mock]:
        yield session

    monkeypatch.setattr(repair, "db_session_scope", session_scope)
    with pytest.raises(RuntimeError, match="discovery unavailable"):
        list(repair._ready_matching_targets())
