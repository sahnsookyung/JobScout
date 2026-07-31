from typing import get_args, get_type_hints

from core.matcher.models import JobMatchPreliminary, RequirementMatchResult
from core.scorer.models import ScoredJobMatch
from database.models import JobPost


def test_job_match_preliminary_type_hints_resolve() -> None:
    hints = get_type_hints(JobMatchPreliminary)

    assert hints["job"] is JobPost
    assert get_args(hints["requirement_matches"]) == (RequirementMatchResult,)


def test_scored_job_match_type_hints_resolve() -> None:
    hints = get_type_hints(ScoredJobMatch)

    assert hints["job"] is JobPost
    assert get_args(hints["matched_requirements"]) == (RequirementMatchResult,)
    assert get_args(hints["missing_requirements"]) == (RequirementMatchResult,)
