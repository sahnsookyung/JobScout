#!/usr/bin/env python3
"""
Tests for the scoring subsystem.

Covers:
  - core/scorer/penalties.py  (extraction helpers, _calculate_experience_penalty)
  - core/scorer/service.py    (ScoringService, _prefetch_total_years)
  - core/config_loader.py     (ScorerConfig, ResultPolicy)
"""

import threading
import pytest
from decimal import Decimal
from unittest.mock import Mock, MagicMock

from core.config_loader import ScorerConfig, ResultPolicy
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer import ScoringService
from core.scorer.service import _prefetch_total_years
from core.scorer.penalties import (
    _extract_years_from_evidence,
    _extract_years_from_section,
    _calculate_best_experience_years,
    _calculate_experience_penalty,
)


# ---------------------------------------------------------------------------
# Shared factories
# ---------------------------------------------------------------------------

def _make_job(**kwargs):
    job = MagicMock()
    job.id = kwargs.get('id', 'job-1')
    job.title = kwargs.get('title', 'Software Engineer')
    job.company = kwargs.get('company', 'TestCorp')
    job.is_remote = kwargs.get('is_remote', True)
    job.salary_max = kwargs.get('salary_max', None)
    job.job_level = kwargs.get('job_level', None)
    return job


def _make_req_match(
    req_id='req-1', similarity=0.8, is_covered=True, req_type='required'
):
    req = MagicMock()
    req.id = req_id
    req.req_type = req_type
    req.text = f'Skill {req_id}'
    return RequirementMatchResult(
        requirement=req,
        evidence=None,
        similarity=similarity,
        is_covered=is_covered,
    )


def _make_preliminary(
    job=None, req_matches=None, missing=None, fingerprint='fp-test'
):
    return JobMatchPreliminary(
        job=job or _make_job(),
        job_similarity=0.75,
        requirement_matches=req_matches if req_matches is not None else [_make_req_match()],
        missing_requirements=missing or [],
        resume_fingerprint=fingerprint,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def scorer_config():
    return ScorerConfig(
        weight_required=0.7,
        weight_preferred=0.3,
        wants_remote=True,
        min_salary=5_000_000,
    )


@pytest.fixture
def mock_repo():
    return MagicMock()


@pytest.fixture
def scorer(mock_repo, scorer_config):
    return ScoringService(mock_repo, scorer_config)


@pytest.fixture
def penalty_config():
    cfg = Mock()
    cfg.penalty_experience_shortfall = 5.0
    return cfg


# ---------------------------------------------------------------------------
# _extract_years_from_evidence
# ---------------------------------------------------------------------------

class TestExtractYearsFromEvidence:

    def test_returns_years_value(self):
        assert _extract_years_from_evidence(Mock(years_value=5.5)) == 5.5

    def test_zero_years_value_returns_zero_not_none(self):
        # 0.0 is a valid value — must not be treated as falsy/None downstream
        assert _extract_years_from_evidence(Mock(years_value=0.0)) == 0.0

    def test_none_evidence_returns_none(self):
        assert _extract_years_from_evidence(None) is None

    def test_evidence_missing_attribute_returns_none(self):
        assert _extract_years_from_evidence(Mock(spec=[])) is None

    def test_evidence_years_value_none_returns_none(self):
        assert _extract_years_from_evidence(Mock(years_value=None)) is None


# ---------------------------------------------------------------------------
# _extract_years_from_section
# ---------------------------------------------------------------------------

class TestExtractYearsFromSection:

    def test_source_data_years_value_takes_priority_over_text(self):
        section = {
            'source_data': {'years_value': 7.5},
            'source_text': 'I have 7.5 years of experience',
        }
        years, source = _extract_years_from_section(section)
        assert years == 7.5
        assert source == 'I have 7.5 years of experience'

    def test_source_data_overrides_conflicting_text_value(self):
        section = {
            'source_data': {'years_value': 10.0},
            'source_text': 'I have 5 years of experience',
        }
        years, _ = _extract_years_from_section(section)
        assert years == 10.0

    def test_regex_plus_pattern(self):
        section = {'source_data': {}, 'source_text': '5+ years of Python experience'}
        years, _ = _extract_years_from_section(section)
        assert years == 5.0

    def test_regex_range_pattern_captures_trailing_number(self):
        # Pattern r'(\d+)\+?\s*(?:years?|...)' scans left-to-right:
        # "3" cannot be followed by "years" (hits "-"), so it matches "5 years"
        section = {'source_data': {}, 'source_text': '3-5 years experience required'}
        years, _ = _extract_years_from_section(section)
        assert years == 5.0

    def test_regex_over_pattern(self):
        section = {'source_data': {}, 'source_text': 'Over 10 years in software development'}
        years, _ = _extract_years_from_section(section)
        assert years == 10.0

    def test_regex_yrs_abbreviation(self):
        section = {'source_data': {}, 'source_text': '8 yrs experience'}
        years, _ = _extract_years_from_section(section)
        assert years == 8.0

    def test_regex_exp_abbreviation(self):
        section = {'source_data': {}, 'source_text': '6 years exp in management'}
        years, _ = _extract_years_from_section(section)
        assert years == 6.0

    def test_no_years_in_text_returns_zero(self):
        section = {'source_data': {}, 'source_text': 'Software engineer skilled in Python'}
        years, source = _extract_years_from_section(section)
        assert years == 0.0
        assert source == ''

    def test_empty_section_returns_zero(self):
        years, source = _extract_years_from_section({})
        assert years == 0.0
        assert source == ''


# ---------------------------------------------------------------------------
# _calculate_best_experience_years
# ---------------------------------------------------------------------------

class TestCalculateBestExperienceYears:

    def test_evidence_years_take_highest_priority_over_sections(self):
        req = Mock(evidence=Mock(years_value=8.0, text='8 years'), is_covered=True)
        sections = [{'source_data': {'years_value': 5.0}, 'source_text': '5 years', 'has_embedding': True}]
        best, _ = _calculate_best_experience_years(req, sections)
        assert best == 8.0

    def test_falls_back_to_sections_when_evidence_is_none(self):
        req = Mock(evidence=None, is_covered=True)
        sections = [{'source_data': {'years_value': 6.0}, 'source_text': '6 years', 'has_embedding': True}]
        best, _ = _calculate_best_experience_years(req, sections)
        assert best == 6.0

    def test_no_evidence_no_sections_returns_zero(self):
        req = Mock(evidence=None)
        best, source = _calculate_best_experience_years(req, None)
        assert best == 0.0
        assert source == ''

    def test_empty_sections_list_returns_zero(self):
        req = Mock(evidence=None)
        best, _ = _calculate_best_experience_years(req, [])
        assert best == 0.0

    def test_sections_without_embedding_flag_are_ignored(self):
        req = Mock(evidence=None)
        sections = [{'source_data': {}, 'source_text': '10 years experience', 'has_embedding': False}]
        best, _ = _calculate_best_experience_years(req, sections)
        assert best == 0.0

    def test_multiple_sections_returns_highest_value(self):
        req = Mock(evidence=None)
        sections = [
            {'source_data': {'years_value': 3.0}, 'source_text': '3 years', 'has_embedding': True},
            {'source_data': {'years_value': 7.0}, 'source_text': '7 years', 'has_embedding': True},
            {'source_data': {'years_value': 5.0}, 'source_text': '5 years', 'has_embedding': True},
        ]
        best, _ = _calculate_best_experience_years(req, sections)
        assert best == 7.0

    def test_mixed_embedding_flags_only_uses_flagged_sections(self):
        req = Mock(evidence=None)
        sections = [
            {'source_data': {'years_value': 10.0}, 'source_text': '10 years', 'has_embedding': False},
            {'source_data': {'years_value': 4.0}, 'source_text': '4 years', 'has_embedding': True},
        ]
        best, _ = _calculate_best_experience_years(req, sections)
        assert best == 4.0  # 10.0 must not be used


# ---------------------------------------------------------------------------
# _calculate_experience_penalty
#
# Production bug: guard was `if not req.evidence or not req.is_covered`
# which bails before the sections fallback can run.
# Fix: `if not req.is_covered` — evidence=None is a valid state.
# ---------------------------------------------------------------------------

class TestCalculateExperiencePenalty:

    def _req(
        self, req_id='req-1', min_years=5.0, evidence=None,
        is_covered=True, text='5 years required',
    ):
        r = Mock()
        r.requirement = Mock(id=req_id, text=text)
        r.requirement_row = Mock()
        r.requirement_row.unit = Mock(min_years=min_years)
        r.evidence = evidence
        r.is_covered = is_covered
        return r

    def test_no_penalty_when_evidence_years_meet_requirement(self, penalty_config):
        req = self._req(evidence=Mock(years_value=7.0))
        penalty, details = _calculate_experience_penalty([req], None, penalty_config, set())
        assert penalty == 0.0
        assert len(details) == 0

    def test_penalty_when_sections_show_insufficient_experience(self, penalty_config):
        """Core case: evidence=None must fall back to sections, not skip entirely."""
        req = self._req(evidence=None, min_years=5.0, text='5 years experience required')
        sections = [{'source_data': {'years_value': 2.0}, 'source_text': '2 years', 'has_embedding': True}]

        penalty, details = _calculate_experience_penalty([req], sections, penalty_config, set())

        assert penalty > 0.0
        assert len(details) == 1
        assert details[0]['type'] == 'experience_years_mismatch'
        assert details[0]['requirement_text'] == '5 years experience required'

    def test_no_penalty_when_sections_show_sufficient_experience(self, penalty_config):
        """Happy path of the fixed guard: evidence=None but sections satisfy requirement."""
        req = self._req(evidence=None, min_years=3.0)
        sections = [{'source_data': {'years_value': 5.0}, 'source_text': '5 years', 'has_embedding': True}]

        penalty, details = _calculate_experience_penalty([req], sections, penalty_config, set())

        assert penalty == 0.0
        assert len(details) == 0

    def test_penalty_capped_at_three_times_shortfall_rate(self, penalty_config):
        """Shortfall 20yr × 5.0 = 100.0 — capped at 3 × 5.0 = 15.0."""
        req = self._req(evidence=None, min_years=20.0, text='20 years experience required')
        sections = [{'source_data': {'years_value': 0.0}, 'source_text': 'No experience', 'has_embedding': True}]

        penalty, details = _calculate_experience_penalty([req], sections, penalty_config, set())

        assert penalty == 15.0
        assert details[0]['requirement_text'] == '20 years experience required'

    def test_penalty_when_evidence_years_value_is_zero(self, penalty_config):
        """years_value=0.0 is not None — evidence path must still trigger penalty."""
        req = self._req(evidence=Mock(years_value=0.0, text=''), min_years=5.0)

        penalty, _ = _calculate_experience_penalty([req], None, penalty_config, set())

        assert penalty > 0.0

    def test_no_penalty_for_uncovered_requirement(self, penalty_config):
        req = self._req(evidence=None, is_covered=False)
        penalty, details = _calculate_experience_penalty([req], None, penalty_config, set())
        assert penalty == 0.0

    def test_no_penalty_when_min_years_not_configured(self, penalty_config):
        req = self._req(min_years=None, evidence=Mock(years_value=5.0))
        penalty, details = _calculate_experience_penalty([req], None, penalty_config, set())
        assert penalty == 0.0

    def test_already_penalized_requirement_is_skipped(self, penalty_config):
        req = self._req(evidence=None, min_years=5.0)
        sections = [{'source_data': {'years_value': 2.0}, 'source_text': '2 years', 'has_embedding': True}]

        penalty, details = _calculate_experience_penalty(
            [req], sections, penalty_config, {'req-1'}
        )

        assert penalty == 0.0
        assert len(details) == 0

    def test_penalized_set_is_mutated_after_applying_penalty(self, penalty_config):
        """Side-effect: req ID must be added to set to prevent double-penalisation."""
        req = self._req(req_id='req-unique-99', evidence=None, min_years=5.0)
        sections = [{'source_data': {'years_value': 1.0}, 'source_text': '', 'has_embedding': True}]
        penalized = set()

        _calculate_experience_penalty([req], sections, penalty_config, penalized)

        assert 'req-unique-99' in penalized

    def test_no_penalty_when_requirement_row_is_none(self, penalty_config):
        """getattr guard: requirement_row=None must not raise, must produce 0 penalty."""
        req = self._req()
        req.requirement_row = None

        penalty, _ = _calculate_experience_penalty([req], None, penalty_config, set())

        assert penalty == 0.0

    def test_no_penalty_when_unit_is_none(self, penalty_config):
        """getattr guard: unit=None must not raise, must produce 0 penalty."""
        req = self._req()
        req.requirement_row.unit = None

        penalty, _ = _calculate_experience_penalty([req], None, penalty_config, set())

        assert penalty == 0.0


# ---------------------------------------------------------------------------
# ScorerConfig — YAML loading
# ---------------------------------------------------------------------------

class TestScorerConfigLoading:

    def test_scorer_config_stores_custom_weights(self):
        config = ScorerConfig(
            weight_required=0.8,
            weight_preferred=0.2,
            wants_remote=True,
        )
        assert config.weight_required == pytest.approx(0.8)
        assert config.weight_preferred == pytest.approx(0.2)
        assert config.wants_remote is True

    def test_scorer_config_defaults_are_valid(self):
        config = ScorerConfig()
        assert 0.0 < config.weight_required <= 1.0
        assert 0.0 < config.weight_preferred <= 1.0
        assert pytest.approx(config.weight_required + config.weight_preferred, abs=0.01) == 1.0



# ---------------------------------------------------------------------------
# ScoringService — initialisation
# ---------------------------------------------------------------------------

class TestScoringServiceInit:

    def test_stores_config_and_repo_on_construction(self, scorer, scorer_config, mock_repo):
        assert scorer.config.weight_required == 0.7
        assert scorer.config.weight_preferred == 0.3
        assert scorer.repo is mock_repo


# ---------------------------------------------------------------------------
# score_preliminary_match
# ---------------------------------------------------------------------------

class TestScorePreliminaryMatch:

    def test_scoring_output_values(self, scorer):
        """Verifies specific output fields for a known input."""
        preliminary = _make_preliminary(
            job=_make_job(salary_max=Decimal('150000'), job_level='senior'),
            req_matches=[_make_req_match(similarity=0.85)],
            fingerprint='test-fp-123',
        )
        scored = scorer.score_preliminary_match(preliminary)

        assert scored is not None
        assert 0 < scored.overall_score <= 100
        assert scored.jd_required_coverage == pytest.approx(0.85, abs=0.01)
        assert scored.jd_preferences_coverage == 0.0
        assert scored.match_type == 'requirements_only'
        assert scored.resume_fingerprint == 'test-fp-123'

    def test_overall_score_bounded_0_to_100(self, scorer):
        scored = scorer.score_preliminary_match(_make_preliminary())
        assert 0 <= scored.overall_score <= 100

    def test_candidate_total_years_is_no_op(self, scorer):
        """candidate_total_years is deprecated — scores must be identical either way."""
        preliminary = _make_preliminary(
            req_matches=[_make_req_match(similarity=0.85)]
        )
        with_years = scorer.score_preliminary_match(preliminary, candidate_total_years=6.0)
        without_years = scorer.score_preliminary_match(preliminary, candidate_total_years=None)

        assert with_years.jd_required_coverage == without_years.jd_required_coverage
        assert with_years.base_score == without_years.base_score


# ---------------------------------------------------------------------------
# _prefetch_total_years
# ---------------------------------------------------------------------------

class TestPrefetchTotalYears:

    def test_single_query_regardless_of_match_count(self):
        matches = [_make_preliminary(fingerprint=f'fp_{i}') for i in range(10)]
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = [
            ('fp_0', 5.0), ('fp_1', 3.5), ('fp_5', 7.0),
        ]

        result = _prefetch_total_years(matches, mock_db)

        assert mock_db.execute.call_count == 1
        assert len(result) == 3
        assert result['fp_0'] == 5.0
        assert result['fp_1'] == 3.5
        assert result['fp_5'] == 7.0

    def test_empty_fingerprints_skips_query_entirely(self):
        matches = [JobMatchPreliminary(
            job=MagicMock(id='job-1'), job_similarity=0.75,
            requirement_matches=[], missing_requirements=[],
            resume_fingerprint='',
        )]
        mock_db = MagicMock()

        result = _prefetch_total_years(matches, mock_db)

        mock_db.execute.assert_not_called()
        assert result == {}


# ---------------------------------------------------------------------------
# score_matches — batch behaviour
# ---------------------------------------------------------------------------

class TestScoreMatchesBatch:

    @pytest.mark.parametrize('num_matches', [1, 10, 50, 100])
    def test_constant_query_count_regardless_of_batch_size(
        self, scorer, mock_repo, num_matches
    ):
        """O(1) query pattern — no N+1."""
        matches = [_make_preliminary(fingerprint=f'fp_{i}') for i in range(num_matches)]
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []
        mock_repo.db = mock_db

        scorer.score_matches(matches)

        assert mock_db.execute.call_count == 1

    def test_results_sorted_descending_by_overall_score(self, scorer):
        matches = [
            _make_preliminary(
                job=_make_job(id=f'job-{i}'),
                req_matches=[_make_req_match(similarity=0.9 - i * 0.1)],
                fingerprint='',
            )
            for i in range(5)
        ]
        scored = scorer.score_matches(matches)

        assert len(scored) == 5
        for i in range(len(scored) - 1):
            assert scored[i].overall_score >= scored[i + 1].overall_score

    def test_stop_event_interrupts_mid_batch(self, scorer):
        matches = [_make_preliminary(fingerprint=f'fp_{i}') for i in range(5)]
        stop = threading.Event()
        stop.set()

        result = scorer.score_matches(matches, stop_event=stop)

        assert result == []


# ---------------------------------------------------------------------------
# ResultPolicy
# ---------------------------------------------------------------------------

class TestResultPolicy:

    def _matches(self, count=5, similarity=0.8):
        return [
            _make_preliminary(
                job=_make_job(id=f'job-{i}'),
                req_matches=[_make_req_match(similarity=similarity)],
                fingerprint='',
            )
            for i in range(count)
        ]

    def test_min_fit_filters_below_threshold(self, scorer):
        policy = ResultPolicy(min_fit=60, top_k=100)
        scored = scorer.score_matches(self._matches(), result_policy=policy)

        for s in scored:
            assert s.fit_score >= 60

    def test_top_k_truncates_to_limit(self, scorer):
        policy = ResultPolicy(min_fit=0, top_k=3)
        scored = scorer.score_matches(self._matches(count=10), result_policy=policy)

        assert len(scored) == 3

    def test_top_k_larger_than_results_returns_all(self, scorer):
        policy = ResultPolicy(min_fit=0, top_k=100)
        scored = scorer.score_matches(self._matches(count=5), result_policy=policy)

        assert len(scored) == 5

    def test_min_jd_required_coverage_filters_correctly(self, scorer):
        """similarity=0.8 → jd_required_coverage≈0.8, below 0.9 threshold."""
        policy = ResultPolicy(min_fit=0, top_k=100, min_jd_required_coverage=0.9)
        scored = scorer.score_matches(self._matches(similarity=0.8), result_policy=policy)

        for s in scored:
            assert s.jd_required_coverage >= 0.9


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
