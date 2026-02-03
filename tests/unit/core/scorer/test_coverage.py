#!/usr/bin/env python3
"""
Test suite for coverage calculations.
"""

import unittest
from unittest.mock import MagicMock, Mock
from core.scorer import coverage
from core.scorer.coverage import calculate_coverage, calculate_base_score
from core.config_loader import ScorerConfig
from core.matcher import RequirementMatchResult


class TestCoverageCalculations(unittest.TestCase):
    """Test coverage calculation functions."""

    def setUp(self):
        """Set up test configuration."""
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3
        )

    def test_01_coverage_calculation(self):
        """Test coverage calculation with mixed requirements (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 1: Coverage Calculation")

        # Create mock requirement matches
        req1 = MagicMock(spec=RequirementMatchResult)
        req1.requirement = MagicMock(req_type="required")
        req1.is_covered = True

        req2 = MagicMock(spec=RequirementMatchResult)
        req2.requirement = MagicMock(req_type="required")
        req2.is_covered = False

        req3 = MagicMock(spec=RequirementMatchResult)
        req3.requirement = MagicMock(req_type="preferred")
        req3.is_covered = True

        matched = [req1, req3]
        missing = [req2]

        required_cov, preferred_cov = coverage.calculate_coverage(matched, missing)

        self.assertEqual(required_cov, 0.5)  # 1 of 2 required
        self.assertEqual(preferred_cov, 1.0)  # 1 of 1 preferred

        print(f"  ✓ Required coverage: {required_cov*100:.0f}%")
        print(f"  ✓ Preferred coverage: {preferred_cov*100:.0f}%")

    def test_02_base_score_full_coverage(self):
        """Test base score calculation with full coverage (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 2: Base Score - Full Coverage")

        # Full coverage
        score = coverage.calculate_base_score(1.0, 1.0, self.scorer_config)
        self.assertEqual(score, 100.0)

        print(f"  ✓ Full coverage score: {score}")

    def test_03_base_score_partial_coverage(self):
        """Test base score calculation with partial coverage (from TestMatchingUnit)."""
        print("\n📊 UNIT Test 3: Base Score - Partial Coverage")

        # Partial coverage
        score = coverage.calculate_base_score(0.5, 1.0, self.scorer_config)
        expected = 100 * (0.7 * 0.5 + 0.3 * 1.0)
        self.assertAlmostEqual(score, expected, places=2)

        print(f"  ✓ Partial coverage score: {score:.1f}")
        print(f"  ✓ Expected: {expected:.1f}")

    def test_calculate_coverage_all_required_covered(self):
        """Test coverage when all required skills are covered."""
        matched = [
            Mock(spec=RequirementMatchResult, requirement=Mock(req_type='required'))
        ]
        missing = [
            Mock(spec=RequirementMatchResult, requirement=Mock(req_type='required'))
        ]

        req_cov, pref_cov = calculate_coverage(matched, missing)

        self.assertEqual(req_cov, 0.5)  # 1 out of 2 required covered

    def test_calculate_coverage_no_required(self):
        """Test coverage when no required skills exist."""
        matched = [
            Mock(spec=RequirementMatchResult, requirement=Mock(req_type='preferred'))
        ]
        missing = [
            Mock(spec=RequirementMatchResult, requirement=Mock(req_type='preferred'))
        ]

        req_cov, pref_cov = calculate_coverage(matched, missing)

        self.assertEqual(req_cov, 0.0)  # No required skills
        self.assertEqual(pref_cov, 0.5)  # 1 out of 2 preferred covered

    def test_calculate_base_score(self):
        """Test base score calculation."""
        config = ScorerConfig(weight_required=0.7, weight_preferred=0.3)

        base = calculate_base_score(required_coverage=0.8, preferred_coverage=0.5, config=config)

        expected = 100 * (0.7 * 0.8 + 0.3 * 0.5)
        self.assertAlmostEqual(base, expected, places=2)


if __name__ == '__main__':
    unittest.main()
