#!/usr/bin/env python3
"""
Unit tests for ScoringService.
"""

import unittest
from unittest.mock import MagicMock

from core.config_loader import ScorerConfig
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer import ScoringService


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
  mode: "with_preferences"
  resume_file: "test_resume.json"
  preferences_file: "test_prefs.json"

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

        # Verify scorer was initialized correctly
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

        # Create preliminary match
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
            preferences_alignment=None,
            requirement_matches=[req_match],
            missing_requirements=[],
            resume_fingerprint="test_fp"
        )

        # Score
        scored = self.scorer.score_preliminary_match(preliminary)

        self.assertIsNotNone(scored)
        self.assertGreater(scored.overall_score, 0)
        self.assertEqual(scored.required_coverage, 1.0)

        print(f"  ✓ Overall score: {scored.overall_score:.1f}")
        print(f"  ✓ Base score: {scored.base_score:.1f}")
        print(f"  ✓ Required coverage: {scored.required_coverage*100:.0f}%")


if __name__ == '__main__':
    unittest.main(verbosity=2)
