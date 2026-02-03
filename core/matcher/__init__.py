"""Matcher Module - Stage 1: Vector Retrieval with Preferences Support."""
from core.matcher.models import (
    ResumeEvidenceUnit, StructuredResumeProfile,
    RequirementMatchResult, PreferencesAlignmentScore, JobMatchPreliminary
)
from core.matcher.service import MatcherService
from core.matcher.resume_profiler import ResumeProfiler
from core.matcher.years_extractor import YearsExtractor
from core.matcher.requirement_matcher import RequirementMatcher
from core.matcher.preference_matcher import PreferenceMatcher
from core.matcher.similarity import SimilarityCalculator

__all__ = [
    'MatcherService', 'ResumeProfiler', 'YearsExtractor',
    'RequirementMatcher', 'PreferenceMatcher', 'SimilarityCalculator',
    'ResumeEvidenceUnit', 'StructuredResumeProfile',
    'RequirementMatchResult', 'PreferencesAlignmentScore', 'JobMatchPreliminary'
]
