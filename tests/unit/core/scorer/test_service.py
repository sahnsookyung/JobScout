#!/usr/bin/env python3
"""
Test suite for ScoringService integration.
"""

import unittest
from unittest.mock import Mock, MagicMock
from core.scorer import ScoringService
from core.config_loader import ScorerConfig
from core.matcher import JobMatchPreliminary, PreferencesAlignmentScore, RequirementMatchResult
from database.models import JobPost


class TestScoringService(unittest.TestCase):
    """Test ScoringService integration."""

    def setUp(self):
        """Set up test fixtures."""
        self.repo = Mock()
        # Mock database query results
        # For structured resume (scalar_one_or_none)
        self.repo.db.execute.return_value.scalar_one_or_none.return_value = None
        # For experience sections (scalars().all())
        self.repo.db.execute.return_value.scalars.return_value.all.return_value = []
        
        self.config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            preferences_boost_max=15.0
        )
        self.scorer = ScoringService(repo=self.repo, config=self.config)

    def test_score_preliminary_match_basic(self):
        """Test basic scoring of preliminary match."""
        # Create mock job
        job = MagicMock(spec=JobPost)
        job.id = "job-123"
        job.title = "Software Engineer"

        # Create mock preliminary match
        preliminary = Mock(spec=JobMatchPreliminary)
        preliminary.job = job
        preliminary.job_similarity = 0.85
        preliminary.resume_fingerprint = "resume-123"
        preliminary.requirement_matches = []
        preliminary.missing_requirements = []
        preliminary.preferences_alignment = None

        # Mock database query for structured resume
        self.repo.db.execute.return_value.scalar_one_or_none.return_value = None

        # Score the match
        scored = self.scorer.score_preliminary_match(preliminary)

        # Verify scored match structure
        self.assertIsNotNone(scored)
        self.assertEqual(scored.job, job)
        self.assertGreaterEqual(scored.overall_score, 0.0)
        self.assertLessEqual(scored.overall_score, 100.0)

    def test_score_matches_sorting(self):
        """Test that multiple matches are sorted by score."""
        from unittest.mock import Mock, MagicMock
        from database.models import JobPost

        mock_evidence = Mock()
        mock_evidence.text = "Test evidence"
        mock_req1 = Mock(spec=RequirementMatchResult)
        mock_req1.requirement = Mock(req_type='required', text="Python experience")
        mock_evidence = Mock()
        mock_evidence.text = "Test evidence"
        mock_req1.evidence = mock_evidence
        mock_req1.is_covered = True

        mock_req2 = Mock(spec=RequirementMatchResult)
        mock_req2.requirement = Mock(req_type='preferred', text="Django experience")
        mock_req2.evidence = mock_evidence
        mock_req2.is_covered = True

        job1 = MagicMock(spec=JobPost)
        job1.id = "job-1"
        job1.title = "Engineer 1"

        job2 = MagicMock(spec=JobPost)
        job2.id = "job-2"
        job2.title = "Engineer 2"

        prelim1 = Mock(spec=JobMatchPreliminary)
        prelim1.job = job1
        prelim1.job_similarity = 0.7
        prelim1.resume_fingerprint = "resume-123"
        prelim1.requirement_matches = [mock_req1]
        prelim1.missing_requirements = []
        prelim1.preferences_alignment = None

        prelim2 = Mock(spec=JobMatchPreliminary)
        prelim2.job = job2
        prelim2.job_similarity = 0.9
        prelim2.resume_fingerprint = "resume-123"
        prelim2.requirement_matches = [mock_req1, mock_req2]
        prelim2.missing_requirements = []
        prelim2.preferences_alignment = None

        self.repo.db.execute.return_value.fetchall.return_value = []
        self.repo.db.execute.return_value.scalars.return_value.all.return_value = []

        scored = self.scorer.score_matches([prelim1, prelim2])

        self.assertEqual(len(scored), 2)
        self.assertGreater(scored[0].overall_score, scored[1].overall_score)
        self.assertEqual(scored[0].job.id, "job-2")
        self.assertEqual(scored[1].job.id, "job-1")


if __name__ == '__main__':
    unittest.main()
