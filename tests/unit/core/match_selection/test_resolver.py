"""Unit tests for canonical selection-run resolution."""

from types import SimpleNamespace

from core.match_selection.resolver import resolve_canonical_resume_selection
from database.models import SYSTEM_OWNER_ID


def test_resolver_prefers_current_selection_run() -> None:
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id: SimpleNamespace(
                id="run-1",
                resume_fingerprint="fp-current",
            )
        ),
    )

    result = resolve_canonical_resume_selection(repo, "user-1")

    assert result is not None
    assert result.resume_fingerprint == "fp-current"
    assert result.selection_run_id == "run-1"
    assert result.resolution_reason == "current_selection_run"


def test_resolver_returns_none_without_committed_selection_run() -> None:
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id: None,
        ),
    )

    result = resolve_canonical_resume_selection(repo, "user-1")

    assert result is None


def test_resolver_global_path_uses_system_owner_run() -> None:
    seen_owner_ids = []
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id: (
                seen_owner_ids.append(owner_id)
                or SimpleNamespace(
                    id="run-global",
                    resume_fingerprint="fp-global",
                )
            )
            if owner_id == SYSTEM_OWNER_ID
            else seen_owner_ids.append(owner_id) or None
        ),
    )

    result = resolve_canonical_resume_selection(repo, None)

    assert seen_owner_ids == [SYSTEM_OWNER_ID]
    assert result is not None
    assert result.resume_fingerprint == "fp-global"
    assert result.selection_run_id == "run-global"
    assert result.resolution_reason == "current_selection_run_global"
