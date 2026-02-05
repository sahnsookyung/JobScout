#!/usr/bin/env python3
"""
Test Resume Models and Matcher Models.

Tests the dataclasses in etl/resume/models.py and core/matcher/models.py:
- ResumeEvidenceUnit from etl/resume/models.py
- RequirementMatchResult from core/matcher/models.py  
- JobMatchPreliminary from core/matcher/models.py
"""
import unittest
from etl.resume import ResumeEvidenceUnit
from core.matcher.models import (
    RequirementMatchResult,
    JobMatchPreliminary
)
from unittest.mock import Mock


class TestResumeEvidenceUnit(unittest.TestCase):
    """Test ResumeEvidenceUnit dataclass."""
    
    def test_basic_creation(self):
        """Test creating a basic ResumeEvidenceUnit."""
        unit = ResumeEvidenceUnit(
            id="reu_001",
            text="Python development experience",
            source_section="experience"
        )

        self.assertEqual(unit.id, "reu_001")
        self.assertEqual(unit.text, "Python development experience")
        self.assertEqual(unit.source_section, "experience")
        self.assertIsNone(unit.embedding)
        self.assertEqual(unit.tags, {})

    def test_with_all_fields(self):
        """Test creating a ResumeEvidenceUnit with all fields."""
        unit = ResumeEvidenceUnit(
            id="reu_002",
            text="Managed a team of 5 engineers",
            source_section="experience",
            tags={"role": "Engineering Manager", "team_size": 5},
            embedding=[0.1] * 1024,
            years_value=3.0,
            years_context="managed team",
            is_total_years_claim=False
        )

        self.assertEqual(unit.id, "reu_002")
        self.assertEqual(unit.tags["role"], "Engineering Manager")
        self.assertEqual(unit.years_value, 3.0)
        assert unit.embedding is not None
        self.assertEqual(len(unit.embedding), 1024)


class TestRequirementMatchResult(unittest.TestCase):
    """Test RequirementMatchResult dataclass."""

    def test_basic_creation(self):
        """Test creating a requirement match result."""
        from database.models import JobRequirementUnit

        mock_req = Mock(spec=JobRequirementUnit)
        mock_req.id = "req-001"

        result = RequirementMatchResult(
            requirement=mock_req,
            evidence=None,
            similarity=0.85,
            is_covered=True
        )

        self.assertEqual(result.similarity, 0.85)
        self.assertTrue(result.is_covered)
        self.assertIsNone(result.evidence)


class TestJobMatchPreliminary(unittest.TestCase):
    """Test JobMatchPreliminary dataclass."""

    def test_basic_creation(self):
        """Test creating a job match preliminary result."""
        from database.models import JobPost

        mock_job = Mock(spec=JobPost)
        mock_job.id = "job-001"

        result = JobMatchPreliminary(
            job=mock_job,
            job_similarity=0.75,
            requirement_matches=[],
            missing_requirements=[],
            resume_fingerprint="abc123"
        )

        self.assertEqual(result.job_similarity, 0.75)
        self.assertEqual(result.resume_fingerprint, "abc123")


if __name__ == '__main__':
    unittest.main()
