#!/usr/bin/env python3
"""
Test Matcher Models.

Tests the dataclasses in core/matcher/models.py and etl/resume/models.py.
"""
import unittest
from datetime import datetime, date
from etl.resume import (
    ResumeEvidenceUnit,
    StructuredResumeProfile,
)
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
        self.assertEqual(len(unit.embedding), 1024)


class TestStructuredResumeProfile(unittest.TestCase):
    """Test StructuredResumeProfile dataclass."""

    def test_basic_creation(self):
        """Test creating a basic profile."""
        profile = StructuredResumeProfile(
            raw_data={"skills": ["Python", "SQL"]}
        )

        self.assertEqual(profile.raw_data["skills"], ["Python", "SQL"])
        self.assertIsNone(profile.calculated_total_years)

    def test_calculate_experience_from_dates(self):
        """Test calculating experience from date ranges."""
        profile = StructuredResumeProfile(
            raw_data={},
            experience_entries=[
                {"start_date": "2020-01", "end_date": "2022-12", "is_current": False},
                {"start_date": "2022-01", "end_date": "2024-06", "is_current": True}
            ]
        )

        years = profile.calculate_experience_from_dates()

        self.assertGreater(years, 4.0)  # At least 4 years

    def test_validate_experience_claim_valid(self):
        """Test validating a plausible experience claim."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=5.0,
            claimed_total_years=5.5,
            experience_entries=[{"start_date": "2019-01", "end_date": "2024-01"}]
        )

        is_valid, message = profile.validate_experience_claim()

        self.assertTrue(is_valid)

    def test_validate_experience_claim_suspicious(self):
        """Test detecting a suspicious experience claim."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=2.0,
            claimed_total_years=10.0,
            experience_entries=[{"start_date": "2022-01", "end_date": "2024-01"}]
        )

        is_valid, message = profile.validate_experience_claim()

        self.assertFalse(is_valid)
        self.assertIn("suspicious", message.lower())

    def test_validate_experience_claim_no_claim(self):
        """Test when no explicit claim was made."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=5.0,
            claimed_total_years=None
        )

        is_valid, message = profile.validate_experience_claim()

        self.assertTrue(is_valid)


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
