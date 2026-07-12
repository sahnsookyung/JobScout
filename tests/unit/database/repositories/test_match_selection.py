"""Unit tests for match selection repository publication semantics."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.match_selection.contracts import MatchSelectionPolicySnapshot
from database.repositories.match_selection import MatchSelectionRepository


def _policy_snapshot():
    return MatchSelectionPolicySnapshot(
        policy_snapshot_version="2026-04-09.v1",
        ranking_mode_used="balanced",
        ranking_config_version="cfg-1",
        stable_tie_break_key="match_id",
        fit_floor_used=55.0,
        required_coverage_floor_used=None,
        notification_fit_floor_used=70.0,
        top_k_used=25,
        candidate_pool_size=40,
        selected_count=25,
        alert_candidate_count=10,
        resume_resolution_reason="requested_resume_fingerprint",
        ranking_config_snapshot={},
    )


def _item_snapshot(job_id: str, rank_position: int = 1):
    return SimpleNamespace(
        job_id=job_id,
        rank_position=rank_position,
        fit_score_at_selection=80.0,
        preference_score_at_selection=70,
        job_similarity_at_selection=0.8,
        required_coverage_at_selection=0.9,
        alert_eligible=True,
        dominant_reason_code="balanced_blend",
        explanation_label="Balanced blend of preference and fit",
        ranking_snapshot={"ranking_mode_used": "balanced"},
    )


def test_publish_selection_run_is_idempotent_for_task_id():
    session = MagicMock()
    repo = MatchSelectionRepository(session)
    existing = SimpleNamespace(id="run-1")

    with patch.object(
        repo,
        "get_committed_run_for_task",
        return_value=existing,
    ) as mock_get_existing:
        result = repo.publish_selection_run(
            owner_id="user-1",
            resume_fingerprint="fp-1",
            policy_snapshot=_policy_snapshot(),
            item_snapshots=[_item_snapshot("job-1")],
            job_match_ids_by_job_id={"job-1": "match-1"},
            task_id="task-1",
        )

    assert result is existing
    mock_get_existing.assert_called_once_with(
        owner_id="user-1",
        resume_fingerprint="fp-1",
        task_id="task-1",
    )
    session.add.assert_not_called()


def test_publish_selection_run_marks_prior_current_run_superseded():
    session = MagicMock()
    repo = MatchSelectionRepository(session)

    with patch.object(repo, "get_committed_run_for_task", return_value=None):
        repo.publish_selection_run(
            owner_id="user-1",
            resume_fingerprint="fp-1",
            policy_snapshot=_policy_snapshot(),
            item_snapshots=[_item_snapshot("job-1")],
            job_match_ids_by_job_id={"job-1": "match-1"},
            task_id="task-1",
        )

    assert session.add.call_count == 2
    assert session.execute.call_count == 1
    update_stmt = session.execute.call_args.args[0]
    update_sql = str(update_stmt)
    assert "match_selection_run" in update_sql
    assert "owner_id" in update_sql
    created_run = session.add.call_args_list[0].args[0]
    assert created_run.lifecycle_status == "committed"
    assert created_run.is_current is True


def test_get_current_run_for_resume_executes_current_committed_query():
    session = MagicMock()
    expected = SimpleNamespace(id="run-1")
    session.execute.return_value.scalar_one_or_none.return_value = expected
    repo = MatchSelectionRepository(session)

    result = repo.get_current_run_for_resume("fp-1")

    assert result is expected
    assert "match_selection_run" in str(session.execute.call_args.args[0])


def test_get_latest_current_run_for_owner_executes_current_committed_query():
    session = MagicMock()
    expected = SimpleNamespace(id="run-1")
    session.execute.return_value.scalar_one_or_none.return_value = expected
    repo = MatchSelectionRepository(session)

    assert repo.get_latest_current_run_for_owner("user-1") is expected

def test_get_latest_current_run_for_owner_can_filter_by_tenant():
    session = MagicMock()
    expected = SimpleNamespace(id="run-1")
    session.execute.return_value.scalar_one_or_none.return_value = expected
    repo = MatchSelectionRepository(session)

    assert repo.get_latest_current_run_for_owner("user-1", tenant_id="tenant-1") is expected

    sql = str(session.execute.call_args.args[0])
    assert "job_post" in sql
    assert "tenant_id" in sql


def test_get_committed_run_for_task_executes_task_scoped_query():
    session = MagicMock()
    expected = SimpleNamespace(id="run-1")
    session.execute.return_value.scalar_one_or_none.return_value = expected
    repo = MatchSelectionRepository(session)

    assert repo.get_committed_run_for_task(
        owner_id="user-1",
        resume_fingerprint="fp-1",
        task_id="task-1",
    ) is expected


def test_get_items_for_run_returns_ordered_items():
    session = MagicMock()
    items = [SimpleNamespace(id="item-1")]
    session.execute.return_value.scalars.return_value.all.return_value = items
    repo = MatchSelectionRepository(session)

    assert repo.get_items_for_run("run-1") == items


def test_get_items_for_run_with_tier_all_omits_tier_filter():
    session = MagicMock()
    items = [SimpleNamespace(id="item-1"), SimpleNamespace(id="item-2")]
    session.execute.return_value.scalars.return_value.all.return_value = items
    repo = MatchSelectionRepository(session)

    result = repo.get_items_for_run("run-1", tier="all")

    assert result == items
    sql = str(session.execute.call_args.args[0])
    # Primary-only filter must NOT appear when tier='all'.
    assert "selection_tier" not in sql or "primary" not in sql


def test_get_items_for_run_default_tier_filters_to_primary():
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    repo = MatchSelectionRepository(session)

    repo.get_items_for_run("run-1")

    sql = str(session.execute.call_args.args[0])
    assert "selection_tier" in sql

def test_get_items_for_run_can_filter_by_tenant():
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    repo = MatchSelectionRepository(session)

    repo.get_items_for_run("run-1", tier="all", tenant_id="tenant-1")

    sql = str(session.execute.call_args.args[0])
    assert "job_post" in sql
    assert "tenant_id" in sql


def test_count_items_for_run_by_tier_groups_rows():
    session = MagicMock()
    session.execute.return_value.all.return_value = [("primary", 5), ("excluded", 7)]
    repo = MatchSelectionRepository(session)

    result = repo.count_items_for_run_by_tier("run-1")

    assert result == {"primary": 5, "excluded": 7}
    sql = str(session.execute.call_args.args[0])
    assert "match_selection_item" in sql.lower() or "selection_tier" in sql.lower()


def test_count_items_for_run_by_tier_handles_empty_run():
    session = MagicMock()
    session.execute.return_value.all.return_value = []
    repo = MatchSelectionRepository(session)
    assert repo.count_items_for_run_by_tier("run-empty") == {}


def test_count_excluded_items_by_reason_buckets_by_reason():
    session = MagicMock()
    session.execute.return_value.all.return_value = [
        ("below_min_fit", 4), ("beyond_top_k", 3), (None, 1),
    ]
    repo = MatchSelectionRepository(session)

    result = repo.count_excluded_items_by_reason("run-1")

    assert result == {"below_min_fit": 4, "beyond_top_k": 3, "unknown": 1}


def test_publish_selection_run_requires_saved_job_match_for_each_selected_item():
    session = MagicMock()
    repo = MatchSelectionRepository(session)

    try:
        repo.publish_selection_run(
            owner_id="user-1",
            resume_fingerprint="fp-1",
            policy_snapshot=_policy_snapshot(),
            item_snapshots=[_item_snapshot("job-without-saved-match")],
            job_match_ids_by_job_id={},
        )
    except ValueError as exc:
        assert "Missing saved job_match_id" in str(exc)
    else:
        raise AssertionError("expected missing job_match_id to fail publication")
