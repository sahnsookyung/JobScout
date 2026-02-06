"""Matcher Module - Stage 1: Vector Retrieval."""

from core.matcher.service import MatcherService
from core.matcher.requirement_matcher import RequirementMatcher
from core.matcher.models import (
    RequirementMatchResult,
    JobMatchPreliminary,
)
from core.matcher.dto import (
    JobMatchDTO,
    MatchResultDTO,
    JobEvidenceDTO,
    RequirementMatchDTO,
    JobRequirementDTO,
    penalty_details_from_orm,
)

__all__ = [
    'MatcherService', 'RequirementMatcher',
    'RequirementMatchResult', 'JobMatchPreliminary',
    'JobMatchDTO', 'MatchResultDTO',
    'JobEvidenceDTO', 'RequirementMatchDTO', 'JobRequirementDTO',
    'penalty_details_from_orm',
]
