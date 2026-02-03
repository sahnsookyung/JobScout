#!/usr/bin/env python3
"""
Test suite for preferences boost calculations.
"""

import unittest
from unittest.mock import MagicMock, Mock
from core.scorer import preferences
from core.scorer.preferences import calculate_preferences_boost
from core.config_loader import ScorerConfig
from core.matcher import PreferencesAlignmentScore


class TestPreferencesCalculations(unittest.TestCase):
    """Test preferences calculation functions."""

    def setUp(self):
        """Set up test configuration."""
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True,
            min_salary=5000000
        )

    def test_01_preferences_remote_alignment(self):
        """Test preferences alignment for remote jobs (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 1: Remote Preferences Alignment")

        # Create preferences alignment for remote job
        alignment = PreferencesAlignmentScore(
            overall_score=0.85,
            location_match=1.0,  # Remote match
            company_size_match=0.7,
            industry_match=0.8,
            role_match=0.9,
            details={'location': {'remote_match': True}}
        )

        # Calculate boost
        boost, details = calculate_preferences_boost(alignment, self.scorer_config)

        # Should have good boost for high location match
        self.assertGreater(boost, 0.0)
        self.assertIn('alignment_breakdown', details)

        print(f"  ✓ Preferences boost: {boost:.1f}")
        print(f"  ✓ Location match: {alignment.location_match:.1f}")

    def test_02_preferences_no_remote_alignment(self):
        """Test preferences alignment for non-remote jobs when wanting remote (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 2: No Remote Preferences Alignment")

        # Create preferences alignment with low location match
        alignment = PreferencesAlignmentScore(
            overall_score=0.45,
            location_match=0.2,  # Poor remote match
            company_size_match=0.6,
            industry_match=0.7,
            role_match=0.8,
            details={'location': {'remote_match': False}}
        )

        # Calculate boost
        boost, details = calculate_preferences_boost(alignment, self.scorer_config)

        # Should have no boost for low alignment (< 0.5)
        self.assertEqual(boost, 0.0)

        print(f"  ✓ Preferences boost: {boost:.1f}")
        print(f"  ✓ Location match: {alignment.location_match:.1f} (low, no boost)")

    def test_03_preferences_salary_alignment(self):
        """Test preferences alignment for salary (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 3: Salary Preferences Alignment")

        # Note: Salary alignment is handled via penalty calculations, not preferences boost
        # The preferences boost focuses on location, company size, industry, and role alignment

        # Create preferences alignment with good overall score
        alignment = PreferencesAlignmentScore(
            overall_score=0.90,
            location_match=1.0,
            company_size_match=0.9,
            industry_match=1.0,
            role_match=0.8,
            details={
                'location': {'remote_match': True},
                'industry': {'match': True}
            }
        )

        # Set high boost max to see the effect
        config = ScorerConfig(preferences_boost_max=15.0)

        # Calculate boost - should get max boost for high alignment (>= 0.9)
        boost, details = calculate_preferences_boost(alignment, config)

        # Should have max boost for high alignment
        self.assertEqual(boost, 15.0)
        self.assertIn('alignment_breakdown', details)

        print(f"  ✓ Preferences boost: {boost:.1f} (max boost)")
        print(f"  ✓ Overall alignment: {alignment.overall_score:.2f}")

    def test_calculate_preferences_boost_max(self):
        """Test boost for perfect alignment."""
        config = ScorerConfig(preferences_boost_max=15.0)
        alignment = PreferencesAlignmentScore(
            overall_score=0.95,
            location_match=1.0,
            company_size_match=0.9,
            industry_match=1.0,
            role_match=0.9,
            details={}
        )

        boost, details = calculate_preferences_boost(alignment, config)

        self.assertEqual(boost, 15.0)  # Max boost
        self.assertIn('boost', details)
        self.assertIn('alignment_breakdown', details)

    def test_calculate_preferences_boost_medium(self):
        """Test boost for medium alignment."""
        config = ScorerConfig(preferences_boost_max=15.0)
        alignment = PreferencesAlignmentScore(
            overall_score=0.8,
            location_match=1.0,
            company_size_match=0.8,
            industry_match=0.8,
            role_match=0.7,
            details={}
        )

        boost, details = calculate_preferences_boost(alignment, config)

        self.assertGreater(boost, 0.0)
        self.assertLess(boost, 15.0)

    def test_calculate_preferences_boost_none(self):
        """Test boost when no preferences provided."""
        config = ScorerConfig(preferences_boost_max=15.0)

        boost, details = calculate_preferences_boost(None, config)

        self.assertEqual(boost, 0.0)
        self.assertIn('reason', details)
        self.assertEqual(details['boost'], 0.0)


if __name__ == '__main__':
    unittest.main()
