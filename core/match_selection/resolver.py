"""Owner-scoped canonical selection-run resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from database.models import SYSTEM_OWNER_ID


@dataclass(frozen=True)
class CanonicalResumeSelection:
    resume_fingerprint: str
    resolution_reason: str
    selection_run_id: str


def resolve_canonical_resume_selection(
    repo,
    owner_id: Optional[Any],
) -> Optional[CanonicalResumeSelection]:
    selection_owner_id = owner_id or SYSTEM_OWNER_ID
    current_run = repo.match_selection.get_latest_current_run_for_owner(selection_owner_id)
    if current_run is None:
        return None

    return CanonicalResumeSelection(
        resume_fingerprint=current_run.resume_fingerprint,
        resolution_reason=(
            "current_selection_run"
            if owner_id is not None
            else "current_selection_run_global"
        ),
        selection_run_id=str(current_run.id),
    )
