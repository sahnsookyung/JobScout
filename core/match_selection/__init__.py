"""Core match-selection contracts and execution helpers."""

from core.match_selection.contracts import (
    MatchSelectionItemSnapshot,
    MatchSelectionPolicySnapshot,
    MatchSelectionResult,
)
from core.match_selection.engine import select_matches
from core.match_selection.resolver import CanonicalResumeSelection, resolve_canonical_resume_selection

__all__ = [
    "CanonicalResumeSelection",
    "MatchSelectionItemSnapshot",
    "MatchSelectionPolicySnapshot",
    "MatchSelectionResult",
    "resolve_canonical_resume_selection",
    "select_matches",
]
