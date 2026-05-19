"""Unit tests for canonical selection-run resolution."""

from types import SimpleNamespace

from core.match_selection.resolver import resolve_canonical_resume_selection
from database.models import SYSTEM_OWNER_ID


def test_resolver_prefers_current_selection_run() -> None:
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id, tenant_id=None: SimpleNamespace(
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
            get_latest_current_run_for_owner=lambda owner_id, tenant_id=None: None,
        ),
    )

    result = resolve_canonical_resume_selection(repo, "user-1")

    assert result is None


def test_resolver_global_path_uses_system_owner_run() -> None:
    seen_owner_ids = []
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id, tenant_id=None: (
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

def test_resolver_passes_tenant_filter() -> None:
    calls = []
    repo = SimpleNamespace(
        match_selection=SimpleNamespace(
            get_latest_current_run_for_owner=lambda owner_id, tenant_id=None: (
                calls.append((owner_id, tenant_id))
                or SimpleNamespace(id="run-tenant", resume_fingerprint="fp-tenant")
            )
        ),
    )

    result = resolve_canonical_resume_selection(repo, "user-1", tenant_id="tenant-1")

    assert result is not None
    assert calls == [("user-1", "tenant-1")]
