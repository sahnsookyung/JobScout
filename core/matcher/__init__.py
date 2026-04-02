"""Matcher Module - Stage 1: Vector Retrieval."""

from core.matcher.service import MatcherService
from core.matcher.requirement_matcher import RequirementMatcher
from core.matcher.models import (
    RequirementEvidenceCandidate,
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
    'RequirementEvidenceCandidate', 'RequirementMatchResult', 'JobMatchPreliminary',
    'JobMatchDTO', 'MatchResultDTO',
    'JobEvidenceDTO', 'RequirementMatchDTO', 'JobRequirementDTO',
    'penalty_details_from_orm',
]
