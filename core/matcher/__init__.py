"""Matcher Module - Stage 1: Vector Retrieval."""

from core.matcher.service import MatcherService
from core.matcher.requirement_matcher import RequirementMatcher
from core.matcher.models import (
    RequirementMatchResult,
    JobMatchPreliminary,
)

__all__ = [
    'MatcherService', 'RequirementMatcher',
    'RequirementMatchResult', 'JobMatchPreliminary',
]
