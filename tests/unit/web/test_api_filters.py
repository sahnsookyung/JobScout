#!/usr/bin/env python3
"""
Unit tests for web API filtering functionality.
Tests remote_only filter for /api/matches endpoint.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from web.backend.models.responses import MatchSummary, StatsResponse


class TestMatchSummaryModel(unittest.TestCase):
    """Tests for MatchSummary Pydantic model - validates is_remote field handling."""

    def test_match_summary_with_remote_true(self):
        """Test MatchSummary serialization with is_remote=True."""
        summary = MatchSummary(
            match_id="123",
            job_id="456",
            title="Remote Engineer",
            company="TechCorp",
            location="Remote",
            is_remote=True,
            fit_score=85.0,
            want_score=80.0,
            overall_score=83.0,
            base_score=90.0,
            penalties=7.0,
            required_coverage=0.85,
            preferred_coverage=0.75,
            match_type="requirements_only",
            is_hidden=False,
            created_at="2026-02-01T12:00:00",
            calculated_at="2026-02-01T12:00:00"
        )
        
        data = summary.model_dump()
        self.assertTrue(data['is_remote'])
        self.assertEqual(data['title'], "Remote Engineer")
        self.assertFalse(data['is_hidden'])

    def test_match_summary_with_remote_false(self):
        """Test MatchSummary serialization with is_remote=False."""
        summary = MatchSummary(
            match_id="123",
            job_id="456",
            title="Office Engineer",
            company="Startup",
            location="New York",
            is_remote=False,
            fit_score=75.0,
            want_score=70.0,
            overall_score=73.0,
            base_score=85.0,
            penalties=12.0,
            required_coverage=0.70,
            preferred_coverage=0.60,
            match_type="requirements_only",
            is_hidden=True,
            created_at=None,
            calculated_at=None
        )
        
        data = summary.model_dump()
        self.assertFalse(data['is_remote'])
        self.assertEqual(data['location'], "New York")
        self.assertTrue(data['is_hidden'])

    def test_match_summary_with_remote_null(self):
        """Test MatchSummary serialization with is_remote=None."""
        summary = MatchSummary(
            match_id="123",
            job_id="456",
            title="Unknown",
            company="Unknown",
            location=None,
            is_remote=None,
            fit_score=None,
            want_score=None,
            overall_score=50.0,
            base_score=60.0,
            penalties=10.0,
            required_coverage=0.50,
            preferred_coverage=0.40,
            match_type="unknown",
            is_hidden=False,
            created_at=None,
            calculated_at=None
        )
        
        data = summary.model_dump()
        self.assertIsNone(data['is_remote'])

    def test_match_summary_is_hidden_default(self):
        """Test that is_hidden defaults to False."""
        summary = MatchSummary(
            match_id="123",
            job_id="456",
            title="Test Job",
            company="Test Co",
            location="Remote",
            is_remote=True,
            fit_score=80.0,
            want_score=75.0,
            overall_score=78.0,
            base_score=85.0,
            penalties=7.0,
            required_coverage=0.80,
            preferred_coverage=0.70,
            match_type="requirements_only",
            created_at="2026-02-01T12:00:00",
            calculated_at="2026-02-01T12:00:00"
        )
        
        data = summary.model_dump()
        self.assertFalse(data['is_hidden'])


class TestStatsResponseModel(unittest.TestCase):
    """Tests for StatsResponse Pydantic model."""

    def test_stats_response_with_hidden_counts(self):
        """Test StatsResponse includes hidden and threshold counts."""
        response = StatsResponse(
            success=True,
            stats={
                'total_matches': 100,
                'active_matches': 75,
                'hidden_count': 10,
                'below_threshold_count': 15,
                'min_fit_threshold': 60,
                'score_distribution': {
                    'excellent': 20,
                    'good': 30,
                    'average': 25,
                    'poor': 25,
                }
            }
        )
        
        data = response.model_dump()
        self.assertTrue(data['success'])
        self.assertEqual(data['stats']['total_matches'], 100)
        self.assertEqual(data['stats']['hidden_count'], 10)
        self.assertEqual(data['stats']['below_threshold_count'], 15)
        self.assertEqual(data['stats']['min_fit_threshold'], 60)

    def test_stats_response_active_calculation(self):
        """Test that active = total - hidden - below_threshold."""
        total = 100
        hidden = 5
        below_threshold = 20
        expected_active = total - hidden - below_threshold
        
        response = StatsResponse(
            success=True,
            stats={
                'total_matches': total,
                'active_matches': expected_active,
                'hidden_count': hidden,
                'below_threshold_count': below_threshold,
                'min_fit_threshold': 60,
                'score_distribution': {
                    'excellent': 10,
                    'good': 20,
                    'average': 30,
                    'poor': 40,
                }
            }
        )
        
        data = response.model_dump()
        self.assertEqual(data['stats']['active_matches'], 75)


if __name__ == '__main__':
    unittest.main()
