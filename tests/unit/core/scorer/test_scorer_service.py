#!/usr/bin/env python3
"""
Unit tests for ScoringService.
"""

import unittest
from unittest.mock import MagicMock
from typing import List

from core.config_loader import ScorerConfig, ResultPolicy
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer import ScoringService
from core.scorer.service import _prefetch_total_years


class TestScorerService(unittest.TestCase):
    """Unit tests for ScoringService - no database required."""

    def setUp(self):
        """Set up services with mocks."""
        self.mock_repo = MagicMock()
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True,
            min_salary=5000000
        )
        self.scorer = ScoringService(self.mock_repo, self.scorer_config)

    def test_01_scorer_config_loading(self):
        """Test loading scorer config from YAML."""
        import os
        import tempfile
        from core.config_loader import load_config

        print("\n⚙️  UNIT Test 1: Scorer Config Loading")

        config_content = """
database:
  url: "postgresql://test:test@localhost:5432/test"

matching:
  enabled: true
  resume_file: "test_resume.json"

  matcher:
    enabled: true
    similarity_threshold: 0.6
    batch_size: 50

  scorer:
    enabled: true
    weight_required: 0.8
    weight_preferred: 0.2
    wants_remote: true
    min_salary: 50000

schedule:
  interval_seconds: 3600

scrapers: []
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name

        try:
            config = load_config(config_path)

            self.assertTrue(config.matching.scorer.enabled)
            self.assertEqual(config.matching.scorer.weight_required, 0.8)
            self.assertEqual(config.matching.scorer.weight_preferred, 0.2)
            self.assertEqual(config.matching.scorer.wants_remote, True)

            print(f"  ✓ Scorer config loaded successfully")
            print(f"  ✓ Weight required: {config.matching.scorer.weight_required}")
            print(f"  ✓ Weight preferred: {config.matching.scorer.weight_preferred}")

        finally:
            os.unlink(config_path)

    def test_02_scorer_initialization(self):
        """Test scorer service initialization."""
        print("\n🔧 UNIT Test 2: Scorer Initialization")

        self.assertIsNotNone(self.scorer)
        self.assertEqual(self.scorer.config.weight_required, 0.7)
        self.assertEqual(self.scorer.config.weight_preferred, 0.3)
        self.assertIsNotNone(self.scorer.repo)

        print(f"  ✓ Scorer initialized successfully")
        print(f"  ✓ Weight required: {self.scorer.config.weight_required}")
        print(f"  ✓ Weight preferred: {self.scorer.config.weight_preferred}")

    def test_03_scorer_complete_scoring(self):
        """Test complete scoring pipeline."""
        print("\n📊 UNIT Test 3: Complete Scoring")

        job = MagicMock()
        job.id = "job-123"
        job.title = "Test Job"
        job.company = "TestCorp"
        job.is_remote = True
        job.salary_max = None
        job.job_level = None

        req = MagicMock()
        req.id = "req-1"
        req.req_type = "required"
        req.text = "Python"

        req_match = RequirementMatchResult(
            requirement=req,
            evidence=None,
            similarity=0.8,
            is_covered=True
        )

        preliminary = JobMatchPreliminary(
            job=job,
            job_similarity=0.75,
            
            requirement_matches=[req_match],
            missing_requirements=[],
            resume_fingerprint="test_fp"
        )

        scored = self.scorer.score_preliminary_match(preliminary)

        self.assertIsNotNone(scored)
        self.assertGreater(scored.overall_score, 0)
        self.assertAlmostEqual(scored.jd_required_coverage, 0.5556, places=2)

        print(f"  ✓ Overall score: {scored.overall_score:.1f}")
        print(f"  ✓ Base score: {scored.base_score:.1f}")
        print(f"  ✓ JD Required coverage: {scored.jd_required_coverage*100:.0f}%")


class TestBatchPrefetch(unittest.TestCase):
    """Tests for batch prefetch behavior - verifies O(1) query pattern."""

    def setUp(self):
        self.mock_repo = MagicMock()
        self.scorer_config = ScorerConfig()
        self.scorer = ScoringService(self.mock_repo, self.scorer_config)

    def _create_preliminary_matches(self, count: int) -> List[JobMatchPreliminary]:
        """Helper to create N preliminary matches with unique fingerprints."""
        matches = []
        for i in range(count):
            job = MagicMock()
            job.id = f"job-{i}"
            job.title = f"Test Job {i}"
            job.company = "TestCorp"
            job.is_remote = True
            job.salary_max = None
            job.job_level = None

            req = MagicMock()
            req.id = f"req-{i}"
            req.req_type = "required"
            req.text = f"Python {i}"

            req_match = RequirementMatchResult(
                requirement=req,
                evidence=None,
                similarity=0.8,
                is_covered=True
            )

            preliminary = JobMatchPreliminary(
                job=job,
                job_similarity=0.75,
                
                requirement_matches=[req_match],
                missing_requirements=[],
                resume_fingerprint=f"fp_{i}"
            )
            matches.append(preliminary)
        return matches

    def test_prefetch_total_years_single_query(self):
        """Verify _prefetch_total_years executes single query regardless of match count."""
        print("\n🔍 Test: Prefetch total years with single query")

        matches = self._create_preliminary_matches(10)
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = [
            ("fp_0", 5.0),
            ("fp_1", 3.5),
            ("fp_5", 7.0),
        ]

        result = _prefetch_total_years(matches, mock_db)

        self.assertEqual(mock_db.execute.call_count, 1,
            f"Expected 1 query, got {mock_db.execute.call_count}")
        self.assertEqual(len(result), 3)
        self.assertEqual(result["fp_0"], 5.0)
        self.assertEqual(result["fp_1"], 3.5)
        self.assertEqual(result["fp_5"], 7.0)

        print(f"  ✓ Single query executed for {len(matches)} matches")
        print(f"  ✓ Retrieved {len(result)} candidate records")

    def test_prefetch_total_years_empty_fingerprints(self):
        """Verify handling when no fingerprints are provided."""
        print("\n🔍 Test: Prefetch with empty fingerprints")

        matches = [
            JobMatchPreliminary(
                job=MagicMock(id="job-1"),
                job_similarity=0.75,
                
                requirement_matches=[],
                missing_requirements=[],
                resume_fingerprint=""
            )
        ]

        mock_db = MagicMock()
        result = _prefetch_total_years(matches, mock_db)

        mock_db.execute.assert_not_called()
        self.assertEqual(len(result), 0)

        print(f"  ✓ No queries executed when no fingerprints present")

    def test_batch_scoring_eliminates_n_plus_one(self):
        """Verify batch scoring methods execute constant queries regardless of match count."""
        print("\n🚀 Test: Batch scoring eliminates N+1 queries")

        for num_matches in [1, 10, 50, 100]:
            with self.subTest(num_matches=num_matches):
                matches = self._create_preliminary_matches(num_matches)

                mock_db = MagicMock()
                self.mock_repo.db = mock_db
                self.scorer.repo = self.mock_repo

                mock_db.execute.return_value.fetchall.return_value = []

                self.scorer.score_matches(matches)

                query_count = mock_db.execute.call_count

                self.assertEqual(query_count, 1,
                    f"Expected 1 query for {num_matches} matches, got {query_count}")

            print(f"  ✓ {num_matches} matches: {query_count} queries (constant O(1))")


class TestScoreEquivalence(unittest.TestCase):
    """Tests verifying refactored code produces same results as original."""

    def setUp(self):
        self.scorer_config = ScorerConfig()
        self.mock_repo = MagicMock()

    def _create_test_data(self):
        """Create test fixtures for score comparison."""
        from decimal import Decimal

        job = MagicMock()
        job.id = "job-eq-1"
        job.title = "Senior Python Developer"
        job.company = "TechCorp"
        job.is_remote = True
        job.salary_max = Decimal("150000")
        job.job_level = "senior"

        req = MagicMock()
        req.id = "req-1"
        req.req_type = "required"
        req.text = "Python"
        req.min_years = 3

        req_match = RequirementMatchResult(
            requirement=req,
            evidence=None,
            similarity=0.85,
            is_covered=True
        )

        preliminary = JobMatchPreliminary(
            job=job,
            job_similarity=0.80,
            
            requirement_matches=[req_match],
            missing_requirements=[],
            resume_fingerprint="test-fp-123"
        )

        candidate_total_years = 6.0

        return preliminary, candidate_total_years

    def test_score_matches_with_and_without_total_years(self):
        """Verify scoring produces consistent results regardless of total years data presence."""
        print("\n✓ Test: Score consistency with/without total years data")

        preliminary, candidate_total_years = self._create_test_data()
        scorer = ScoringService(self.mock_repo, self.scorer_config)

        scored_with_data = scorer.score_preliminary_match(
            preliminary,
            candidate_total_years=candidate_total_years
        )

        scored_without_data = scorer.score_preliminary_match(
            preliminary,
            candidate_total_years=None
        )

        self.assertIsNotNone(scored_with_data)
        self.assertIsNotNone(scored_without_data)

        self.assertEqual(scored_with_data.jd_required_coverage, scored_without_data.jd_required_coverage)
        self.assertEqual(scored_with_data.base_score, scored_without_data.base_score)

        print(f"  ✓ Scores consistent: overall={scored_with_data.overall_score:.1f}")

    def test_score_with_total_years_matches_expected_values(self):
        """Verify scores match expected values with total years data."""
        print("\n✓ Test: Score matches expected values with total years")

        preliminary, candidate_total_years = self._create_test_data()
        scorer = ScoringService(self.mock_repo, self.scorer_config)

        scored = scorer.score_preliminary_match(
            preliminary,
            candidate_total_years=candidate_total_years
        )

        self.assertIsNotNone(scored)
        self.assertGreater(scored.overall_score, 0)
        self.assertLessEqual(scored.overall_score, 100)
        self.assertAlmostEqual(scored.jd_required_coverage, 0.6667, places=2)
        self.assertEqual(scored.jd_preferences_coverage, 0.0)
        self.assertEqual(scored.match_type, "requirements_only")
        self.assertEqual(scored.resume_fingerprint, "test-fp-123")

        print(f"  ✓ Score verified: overall={scored.overall_score:.1f}, base={scored.base_score:.1f}")

    def test_batch_scores_sorted_by_overall(self):
        """Verify batch scoring returns results sorted by overall score."""
        print("\n✓ Test: Batch scores sorted by overall score")

        scorer = ScoringService(self.mock_repo, self.scorer_config)

        jobs = []
        for i in range(5):
            job = MagicMock()
            job.id = f"job-{i}"
            job.title = f"Job {i}"
            job.company = "TestCorp"
            job.is_remote = True
            job.salary_max = None
            job.job_level = None
            jobs.append(job)

        matches = []
        for i, job in enumerate(jobs):
            similarity = 0.9 - (i * 0.1)
            req = MagicMock()
            req.id = f"req-{i}"
            req.req_type = "required"
            req.text = f"Skill {i}"

            req_match = RequirementMatchResult(
                requirement=req,
                evidence=None,
                similarity=similarity,
                is_covered=True
            )

            preliminary = JobMatchPreliminary(
                job=job,
                job_similarity=similarity,
                
                requirement_matches=[req_match],
                missing_requirements=[],
                resume_fingerprint=""
            )
            matches.append(preliminary)

        scored = scorer.score_matches(matches)

        self.assertEqual(len(scored), 5)
        for i in range(len(scored) - 1):
            self.assertGreaterEqual(scored[i].overall_score, scored[i + 1].overall_score)

        print(f"  ✓ Scores sorted correctly: {[s.overall_score for s in scored]}")


class TestResultPolicy(unittest.TestCase):
    """Tests for ResultPolicy application."""

    def setUp(self):
        self.scorer_config = ScorerConfig()
        self.mock_repo = MagicMock()

    def test_result_policy_filters_by_min_fit(self):
        """Verify ResultPolicy filters by min_fit threshold."""
        print("\n✓ Test: ResultPolicy filters by min_fit")

        scorer = ScoringService(self.mock_repo, self.scorer_config)

        jobs = []
        for i in range(5):
            job = MagicMock()
            job.id = f"job-{i}"
            job.title = f"Job {i}"
            job.company = "TestCorp"
            job.is_remote = True
            job.salary_max = None
            job.job_level = None
            jobs.append(job)

        matches = []
        for i, job in enumerate(jobs):
            req = MagicMock()
            req.id = f"req-{i}"
            req.req_type = "required"
            req.text = f"Skill {i}"

            req_match = RequirementMatchResult(
                requirement=req,
                evidence=None,
                similarity=0.8,
                is_covered=True
            )

            preliminary = JobMatchPreliminary(
                job=job,
                job_similarity=0.8,
                
                requirement_matches=[req_match],
                missing_requirements=[],
                resume_fingerprint=""
            )
            matches.append(preliminary)

        policy = ResultPolicy(min_fit=60, top_k=10)
        scored = scorer.score_matches(matches, result_policy=policy)

        for s in scored:
            self.assertGreaterEqual(s.fit_score, 60)

        print(f"  ✓ Filtered to {len(scored)} matches with fit >= 60")

    def test_result_policy_top_k_limits_results(self):
        """Verify ResultPolicy truncates to top_k."""
        print("\n✓ Test: ResultPolicy truncates to top_k")

        scorer = ScoringService(self.mock_repo, self.scorer_config)

        jobs = []
        for i in range(10):
            job = MagicMock()
            job.id = f"job-{i}"
            job.title = f"Job {i}"
            job.company = "TestCorp"
            job.is_remote = True
            job.salary_max = None
            job.job_level = None
            jobs.append(job)

        matches = []
        for i, job in enumerate(jobs):
            req = MagicMock()
            req.id = f"req-{i}"
            req.req_type = "required"
            req.text = f"Skill {i}"

            req_match = RequirementMatchResult(
                requirement=req,
                evidence=None,
                similarity=0.8,
                is_covered=True
            )

            preliminary = JobMatchPreliminary(
                job=job,
                job_similarity=0.8,
                
                requirement_matches=[req_match],
                missing_requirements=[],
                resume_fingerprint=""
            )
            matches.append(preliminary)

        policy = ResultPolicy(min_fit=0, top_k=3)
        scored = scorer.score_matches(matches, result_policy=policy)

        self.assertEqual(len(scored), 3)
        print(f"  ✓ Limited to top {len(scored)} matches")


if __name__ == '__main__':
    unittest.main(verbosity=2)
