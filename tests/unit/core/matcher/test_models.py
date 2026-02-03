#!/usr/bin/env python3
"""
Test Matcher Models.

Tests the dataclasses in core/matcher/models.py.
"""
import unittest
from datetime import datetime, date
from core.matcher.models import (
    ResumeEvidenceUnit,
    StructuredResumeProfile,
    RequirementMatchResult,
    PreferencesAlignmentScore,
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
        self.assertEqual(unit.tags, {})
        self.assertIsNone(unit.embedding)
        self.assertIsNone(unit.years_value)
        self.assertIsNone(unit.years_context)
        self.assertFalse(unit.is_total_years_claim)
    
    def test_with_all_fields(self):
        """Test creating ResumeEvidenceUnit with all fields."""
        unit = ResumeEvidenceUnit(
            id="reu_002",
            text="5 years of AWS experience",
            source_section="skills",
            tags={"type": "cloud", "level": "expert"},
            embedding=[0.1, 0.2, 0.3],
            years_value=5.0,
            years_context="AWS",
            is_total_years_claim=False
        )
        
        self.assertEqual(unit.years_value, 5.0)
        self.assertEqual(unit.years_context, "AWS")
        self.assertFalse(unit.is_total_years_claim)


class TestStructuredResumeProfile(unittest.TestCase):
    """Test StructuredResumeProfile dataclass."""
    
    def test_basic_creation(self):
        """Test creating a basic profile."""
        profile = StructuredResumeProfile(
            raw_data={"name": "Test User"},
            calculated_total_years=5.0,
            claimed_total_years=6.0
        )
        
        self.assertEqual(profile.calculated_total_years, 5.0)
        self.assertEqual(profile.claimed_total_years, 6.0)
    
    def test_calculate_experience_from_dates(self):
        """Test experience calculation from date ranges."""
        profile = StructuredResumeProfile(
            raw_data={},
            experience_entries=[
                {
                    "start_date": "2020-01",
                    "end_date": "2022-12",
                    "is_current": False
                },
                {
                    "start_date": "2018-06",
                    "end_date": "2019-12",
                    "is_current": False
                }
            ]
        )
        
        years = profile.calculate_experience_from_dates()
        # 2020-01 to 2022-12 = 3 years
        # 2018-06 to 2019-12 = 1.5 years
        # Total = 4.5 years
        self.assertAlmostEqual(years, 4.5, delta=0.1)
    
    def test_validate_experience_claim_valid(self):
        """Test validation when claim is within tolerance."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=5.0,
            claimed_total_years=5.5  # Within 20% tolerance
        )
        
        is_valid, msg = profile.validate_experience_claim()
        self.assertTrue(is_valid)
        self.assertIn("valid", msg.lower())
    
    def test_validate_experience_claim_suspicious(self):
        """Test validation when claim exceeds tolerance."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=3.0,
            claimed_total_years=10.0  # Far exceeds tolerance
        )
        
        is_valid, msg = profile.validate_experience_claim()
        self.assertFalse(is_valid)
        self.assertIn("suspicious", msg.lower())
    
    def test_validate_no_claim(self):
        """Test validation when no claim is made."""
        profile = StructuredResumeProfile(
            raw_data={},
            calculated_total_years=5.0,
            claimed_total_years=None
        )
        
        is_valid, msg = profile.validate_experience_claim()
        self.assertTrue(is_valid)
        self.assertIn("no explicit claim", msg.lower())


class TestRequirementMatchResult(unittest.TestCase):
    """Test RequirementMatchResult dataclass."""
    
    def test_basic_creation(self):
        """Test creating a match result."""
        req = Mock()
        evidence = ResumeEvidenceUnit(
            id="reu_001",
            text="Python experience",
            source_section="skills"
        )
        
        result = RequirementMatchResult(
            requirement=req,
            evidence=evidence,
            similarity=0.85,
            is_covered=True
        )
        
        self.assertTrue(result.is_covered)
        self.assertEqual(result.similarity, 0.85)
        self.assertEqual(result.evidence.id, "reu_001")


class TestPreferencesAlignmentScore(unittest.TestCase):
    """Test PreferencesAlignmentScore dataclass."""
    
    def test_creation(self):
        """Test creating alignment score."""
        score = PreferencesAlignmentScore(
            overall_score=0.75,
            location_match=0.9,
            company_size_match=0.8,
            industry_match=0.7,
            role_match=0.6,
            details={"remote": True, "location": "Tokyo"}
        )
        
        self.assertEqual(score.overall_score, 0.75)
        self.assertEqual(score.location_match, 0.9)


if __name__ == '__main__':
    unittest.main()
