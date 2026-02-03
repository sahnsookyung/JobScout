#!/usr/bin/env python3
"""
Comprehensive tests for preferences-based job matching.

Tests the integration of preferences into the matching and scoring pipeline.
"""

import unittest
import json
import os
from typing import Dict, Any
from unittest.mock import MagicMock

# Services
from core.matcher import (
    MatcherService, ResumeEvidenceUnit,
    JobMatchPreliminary, PreferencesAlignmentScore
)
from tests.mocks.matcher_mocks import MockMatcherService
from core.scorer import ScoringService, ScoredJobMatch
from core.config_loader import MatcherConfig, ScorerConfig
from core.scorer import preferences, penalties

class TestPreferencesMatching(unittest.TestCase):
    """Test preferences-based matching functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data."""
        cls.sample_preferences = {
            "job_preferences": {
                "wants_remote": True,
                "location_preferences": {
                    "preferred_locations": ["Tokyo", "Osaka", "Remote"],
                    "avoid_locations": ["Rural areas"]
                }
            },
            "compensation": {
                "salary": {
                    "minimum": 5000000,
                    "target": 8000000,
                    "currency": "JPY"
                }
            },
            "career_preferences": {
                "seniority_level": "mid",
                "role_types": ["Software Engineer", "Backend Developer"],
                "avoid_roles": ["Manager", "Team Lead"]
            },
            "technical_preferences": {
                "primary_languages": ["Python", "Java"],
                "avoid_technologies": ["PHP"]
            },
            "company_preferences": {
                "company_size": {
                    "employee_count": {"minimum": 10, "maximum": 500}
                },
                "industry": {
                    "preferred": ["SaaS", "Fintech"],
                    "avoid": ["Gaming"]
                }
            }
        }
        
        cls.resume_data = {
            "name": "Test User",
            "sections": [
                {
                    "title": "Skills",
                    "items": [{"description": "Python, Java, AWS", "highlights": []}]
                },
                {
                    "title": "Experience",
                    "items": [{
                        "company": "TechCorp",
                        "role": "Senior Engineer",
                        "description": "Built microservices",
                        "highlights": ["Led team"]
                    }]
                }
            ]
        }
    
    def setUp(self):
        """Set up services."""
        self.mock_repo = MagicMock()
        self.mock_ai = MagicMock()
        self.mock_ai.generate_embedding = MagicMock(return_value=[0.1] * 1024)
        
        self.matcher_config = MatcherConfig(similarity_threshold=0.3)
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True,
            min_salary=5000000
        )
        
        self.matcher = MockMatcherService(self.mock_repo, self.mock_ai, self.matcher_config)
        self.scorer = ScoringService(self.mock_repo, self.scorer_config)
    
    # ============ TEST 1: Location Preferences ============
    
    def test_01_location_match_remote_job(self):
        """Test location matching for remote job when user wants remote."""
        print("\n📍 Test 1: Location Match - Remote Job")
        
        # Create mock job - remote
        job = MagicMock()
        job.location_text = "Remote"
        job.is_remote = True
        
        score, details = self.matcher.calculate_location_match(job, self.sample_preferences)
        
        self.assertEqual(score, 1.0, "Remote job should be perfect match when user wants remote")
        self.assertTrue(details['job_is_remote'])
        
        print(f"  ✓ Score: {score:.2f} (expected: 1.0)")
        print(f"  ✓ Location: {details['job_location']}")
    
    def test_02_location_match_non_remote_preferred_location(self):
        """Test location matching for non-remote job in preferred location."""
        print("\n📍 Test 2: Location Match - Preferred Location (Non-Remote)")
        
        job = MagicMock()
        job.location_text = "Tokyo, Japan"
        job.is_remote = False
        
        score, details = self.matcher.calculate_location_match(job, self.sample_preferences)
        
        self.assertGreater(score, 0.5, "Tokyo should be good match")
        self.assertLess(score, 1.0, "Non-remote should not be perfect")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Tokyo is in preferred locations")
    
    def test_03_location_match_avoided_location(self):
        """Test location matching for avoided location."""
        print("\n📍 Test 3: Location Match - Avoided Location")
        
        # Create preferences with avoided location
        import copy
        preferences = copy.deepcopy(self.sample_preferences)
        preferences['job_preferences']['location_preferences']['avoid_locations'] = ['New York']
        
        job = MagicMock()
        job.location_text = "New York, USA"
        job.is_remote = False
        
        score, details = self.matcher.calculate_location_match(job, preferences)
        
        self.assertEqual(score, 0.0, "Avoided location should score 0")
        
        print(f"  ✓ Score: {score:.2f} (expected: 0.0)")
        print(f"  ✓ New York is in avoided locations")
    
    # ============ TEST 2: Company Size Preferences ============
    
    def test_04_company_size_match_ideal(self):
        """Test company size matching for ideal size."""
        print("\n🏢 Test 4: Company Size Match - Ideal")
        
        job = MagicMock()
        job.company_num_employees = "50"
        
        score, details = self.matcher.calculate_company_size_match(job, self.sample_preferences)
        
        self.assertEqual(score, 1.0, "50 employees should be perfect match")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Company size: 50 (range: 10-500)")
    
    def test_05_company_size_match_too_small(self):
        """Test company size matching for too small company."""
        print("\n🏢 Test 5: Company Size Match - Too Small")
        
        job = MagicMock()
        job.company_num_employees = "5"
        
        score, details = self.matcher.calculate_company_size_match(job, self.sample_preferences)
        
        self.assertLess(score, 0.5, "5 employees should be low score")
        self.assertGreater(score, 0.0, "Should not be zero")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Company size: 5 (minimum: 10)")
    
    def test_06_company_size_match_too_large(self):
        """Test company size matching for too large company."""
        print("\n🏢 Test 6: Company Size Match - Too Large")
        
        job = MagicMock()
        job.company_num_employees = "10000"
        
        score, details = self.matcher.calculate_company_size_match(job, self.sample_preferences)
        
        self.assertLess(score, 0.5, "10000 employees should be low score")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Company size: 10000 (maximum: 500)")
    
    # ============ TEST 3: Industry Preferences ============
    
    def test_07_industry_match_preferred(self):
        """Test industry matching for preferred industry."""
        print("\n🏭 Test 7: Industry Match - Preferred")
        
        job = MagicMock()
        job.company_industry = "SaaS"
        
        score, details = self.matcher.calculate_industry_match(job, self.sample_preferences)
        
        self.assertEqual(score, 1.0, "SaaS should be perfect match")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Industry: SaaS (preferred)")
    
    def test_08_industry_match_avoided(self):
        """Test industry matching for avoided industry."""
        print("\n🏭 Test 8: Industry Match - Avoided")
        
        job = MagicMock()
        job.company_industry = "Gaming"
        
        score, details = self.matcher.calculate_industry_match(job, self.sample_preferences)
        
        self.assertEqual(score, 0.0, "Gaming should be avoided")
        
        print(f"  ✓ Score: {score:.2f} (expected: 0.0)")
        print(f"  ✓ Industry: Gaming (avoided)")
    
    def test_09_industry_match_neutral(self):
        """Test industry matching for neutral industry."""
        print("\n🏭 Test 9: Industry Match - Neutral")
        
        job = MagicMock()
        job.company_industry = "Healthcare"
        
        score, details = self.matcher.calculate_industry_match(job, self.sample_preferences)
        
        self.assertEqual(score, 0.5, "Unknown industry should be neutral")
        
        print(f"  ✓ Score: {score:.2f} (neutral)")
    
    # ============ TEST 4: Role Preferences ============
    
    def test_10_role_match_preferred(self):
        """Test role matching for preferred role."""
        print("\n💼 Test 10: Role Match - Preferred")
        
        job = MagicMock()
        job.title = "Software Engineer - Backend"
        job.job_level = "Mid-level"
        
        score, details = self.matcher.calculate_role_match(job, self.sample_preferences)
        
        self.assertEqual(score, 1.0, "Software Engineer should be perfect match")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Title: {job.title}")
    
    def test_11_role_match_avoided(self):
        """Test role matching for avoided role."""
        print("\n💼 Test 11: Role Match - Avoided")
        
        job = MagicMock()
        job.title = "Engineering Manager"
        job.job_level = "Senior"
        
        score, details = self.matcher.calculate_role_match(job, self.sample_preferences)
        
        self.assertEqual(score, 0.0, "Manager should be avoided")
        
        print(f"  ✓ Score: {score:.2f} (expected: 0.0)")
        print(f"  ✓ Title: {job.title} (avoided)")
    
    def test_12_role_match_seniority_match(self):
        """Test role matching with seniority alignment."""
        print("\n💼 Test 12: Role Match - Seniority Match")
        
        job = MagicMock()
        job.title = "Developer"
        job.job_level = "Mid-level"
        
        score, details = self.matcher.calculate_role_match(job, self.sample_preferences)
        
        self.assertGreaterEqual(score, 0.8, "Mid-level should match seniority preference")
        
        print(f"  ✓ Score: {score:.2f}")
        print(f"  ✓ Seniority match: {job.job_level}")
    
    # ============ TEST 5: Overall Preferences Alignment ============
    
    def test_13_overall_preferences_alignment(self):
        """Test overall preferences alignment calculation."""
        print("\n🎯 Test 13: Overall Preferences Alignment")
        
        # Create perfect match job
        job = MagicMock()
        job.location_text = "Remote"
        job.is_remote = True
        job.company_num_employees = "100"
        job.company_industry = "SaaS"
        job.title = "Software Engineer"
        job.job_level = "Mid-level"
        
        alignment = self.matcher.calculate_preferences_alignment(job, self.sample_preferences)
        
        self.assertIsNotNone(alignment)
        self.assertGreater(alignment.overall_score, 0.8, "Perfect job should have high alignment")
        self.assertEqual(alignment.location_match, 1.0)
        self.assertEqual(alignment.industry_match, 1.0)
        
        print(f"  ✓ Overall alignment: {alignment.overall_score:.2f}")
        print(f"  ✓ Location: {alignment.location_match:.2f}")
        print(f"  ✓ Industry: {alignment.industry_match:.2f}")
        print(f"  ✓ Company size: {alignment.company_size_match:.2f}")
        print(f"  ✓ Role: {alignment.role_match:.2f}")
    
    def test_14_preferences_alignment_no_preferences(self):
        """Test preferences alignment with no preferences provided."""
        print("\n🎯 Test 14: Preferences Alignment - No Preferences")
        
        job = MagicMock()
        
        alignment = self.matcher.calculate_preferences_alignment(job, None)
        
        self.assertIsNone(alignment, "Should return None when no preferences")
        
        print(f"  ✓ Returns None when no preferences provided")
    
    # ============ TEST 6: Scoring with Preferences ============
    
    def test_15_scoring_preferences_boost(self):
        """Test that good preferences alignment gives score boost."""
        print("\n📊 Test 15: Scoring - Preferences Boost")
        
        # Create alignment score
        alignment = PreferencesAlignmentScore(
            overall_score=0.95,
            location_match=1.0,
            company_size_match=0.9,
            industry_match=1.0,
            role_match=0.9,
            details={}
        )
        
        # Calculate boost
        boost, details = preferences.calculate_preferences_boost(alignment, self.scorer_config)
        
        self.assertGreater(boost, 0, "Good alignment should give boost")
        self.assertEqual(boost, 15.0, "0.95 alignment should get max boost")
        
        print(f"  ✓ Boost: +{boost:.1f} points")
        print(f"  ✓ Alignment: 0.95")
    
    def test_16_scoring_preferences_penalty(self):
        """Test that bad preferences alignment gives penalties."""
        print("\n📊 Test 16: Scoring - Preferences Penalty")
        
        # Create bad alignment
        alignment = PreferencesAlignmentScore(
            overall_score=0.2,
            location_match=0.0,
            company_size_match=0.5,
            industry_match=0.0,  # Avoided industry
            role_match=0.0,  # Avoided role
            details={
                'industry': {'job_industry': 'Gaming'},
                'role': {'job_title': 'Manager'}
            }
        )
        
        job = MagicMock()
        job.is_remote = False
        job.location_text = "New York"
        job.salary_max = None
        job.job_level = None
        
        # Test penalties from alignment
        penalties_score, details = penalties.calculate_penalties(
            job, 1.0, [], [], self.scorer_config, alignment, repo=self.scorer.repo
        )
        
        # Should have industry and role penalties
        industry_penalty = next((p for p in details if p['type'] == 'industry_mismatch'), None)
        role_penalty = next((p for p in details if p['type'] == 'role_mismatch'), None)
        
        self.assertIsNotNone(industry_penalty, "Should have industry penalty")
        self.assertIsNotNone(role_penalty, "Should have role penalty")
        
        print(f"  ✓ Total penalties: {penalties_score:.1f}")
        print(f"  ✓ Industry penalty: {industry_penalty['amount']}")
        print(f"  ✓ Role penalty: {role_penalty['amount']}")
    
    def test_17_scoring_without_preferences(self):
        """Test scoring without preferences (baseline)."""
        print("\n📊 Test 17: Scoring - Without Preferences")
        
        # Calculate boost with no alignment
        boost, details = preferences.calculate_preferences_boost(None, self.scorer_config)
        
        self.assertEqual(boost, 0.0, "No preferences should give no boost")
        
        print(f"  ✓ Boost: {boost:.1f} (expected: 0.0)")
        print(f"  ✓ Reason: {details['reason']}")
    
    # ============ TEST 7: End-to-End with Preferences ============
    
    def test_18_end_to_end_matching_with_preferences(self):
        """Test complete matching pipeline with preferences."""
        print("\n🔄 Test 18: End-to-End Matching with Preferences")
        
        # Extract evidence
        evidence_units = self.matcher.extract_resume_evidence(self.resume_data)
        self.matcher.embed_evidence_units(evidence_units)
        
        # Create mock job
        job = MagicMock()
        job.id = "job-123"
        job.title = "Software Engineer"
        job.company = "TestCorp"
        job.location_text = "Remote"
        job.is_remote = True
        job.company_num_employees = "50"
        job.company_industry = "SaaS"
        job.job_level = "Mid-level"
        job.summary_embedding = [0.1] * 1024
        job.salary_max = None
        job.requirements = []
        
        # Match with preferences
        preliminary = self.matcher.match_resume_to_job(
            evidence_units=evidence_units,
            job=job,
            resume_fingerprint="test-fp",
            preferences=self.sample_preferences
        )
        
        self.assertIsNotNone(preliminary)
        self.assertIsNotNone(preliminary.preferences_alignment)
        self.assertGreater(preliminary.preferences_alignment.overall_score, 0.8)
        
        # Score with preferences
        scored = self.scorer.score_preliminary_match(
            preliminary,
            match_type="with_preferences"
        )
        
        self.assertIsNotNone(scored)
        self.assertEqual(scored.match_type, "with_preferences")
        self.assertGreater(scored.preferences_boost, 0)
        
        print(f"  ✓ Overall score: {scored.overall_score:.1f}")
        print(f"  ✓ Base score: {scored.base_score:.1f}")
        print(f"  ✓ Preferences boost: +{scored.preferences_boost:.1f}")
        print(f"  ✓ Penalties: {scored.penalties:.1f}")
        print(f"  ✓ Match type: {scored.match_type}")
    
    def test_19_compare_with_and_without_preferences(self):
        """Compare scoring with and without preferences."""
        print("\n🔄 Test 19: Compare With/Without Preferences")
        
        # Extract evidence
        evidence_units = self.matcher.extract_resume_evidence(self.resume_data)
        self.matcher.embed_evidence_units(evidence_units)
        
        # Create perfect match job
        job = MagicMock()
        job.id = "job-456"
        job.title = "Software Engineer"
        job.company = "PerfectCorp"
        job.location_text = "Remote"
        job.is_remote = True
        job.company_num_employees = "100"
        job.company_industry = "SaaS"
        job.job_level = "Mid-level"
        job.summary_embedding = [0.1] * 1024
        job.salary_max = 10000000
        job.requirements = []
        
        # Match without preferences
        preliminary_no_prefs = self.matcher.match_resume_to_job(
            evidence_units, job, "fp1", preferences=None
        )
        scored_no_prefs = self.scorer.score_preliminary_match(
            preliminary_no_prefs, "requirements_only"
        )
        
        # Match with preferences
        preliminary_with_prefs = self.matcher.match_resume_to_job(
            evidence_units, job, "fp2", preferences=self.sample_preferences
        )
        scored_with_prefs = self.scorer.score_preliminary_match(
            preliminary_with_prefs, "with_preferences"
        )
        
        # With preferences should have higher score due to boost
        self.assertGreater(
            scored_with_prefs.overall_score,
            scored_no_prefs.overall_score,
            "Perfect preferences match should score higher"
        )
        
        print(f"  ✓ Without preferences: {scored_no_prefs.overall_score:.1f}")
        print(f"  ✓ With preferences: {scored_with_prefs.overall_score:.1f}")
        print(f"  ✓ Difference: +{scored_with_prefs.overall_score - scored_no_prefs.overall_score:.1f}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
