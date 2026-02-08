#!/usr/bin/env python3
"""
Unit tests for web API filtering functionality.
Tests remote_only filter for /api/matches endpoint.
"""

import unittest


class TestMatchSummaryModel(unittest.TestCase):
    """Tests for MatchSummary Pydantic model - validates is_remote field handling."""

    def test_match_summary_with_remote_true(self):
        """Test MatchSummary serialization with is_remote=True."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'web', 'backend'))
        from app import MatchSummary
        
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
            match_type="with_preferences",
            created_at="2026-02-01T12:00:00",
            calculated_at="2026-02-01T12:00:00"
        )
        
        data = summary.model_dump()
        self.assertTrue(data['is_remote'])
        self.assertEqual(data['title'], "Remote Engineer")

    def test_match_summary_with_remote_false(self):
        """Test MatchSummary serialization with is_remote=False."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'web', 'backend'))
        from app import MatchSummary
        
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
            match_type="with_preferences",
            created_at=None,
            calculated_at=None
        )
        
        data = summary.model_dump()
        self.assertFalse(data['is_remote'])
        self.assertEqual(data['location'], "New York")

    def test_match_summary_with_remote_null(self):
        """Test MatchSummary serialization with is_remote=None."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'web', 'backend'))
        from app import MatchSummary
        
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
            created_at=None,
            calculated_at=None
        )
        
        data = summary.model_dump()
        self.assertIsNone(data['is_remote'])


if __name__ == '__main__':
    unittest.main()
