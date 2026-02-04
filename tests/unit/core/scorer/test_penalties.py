#!/usr/bin/env python3
"""
Unit tests for scorer penalty calculations.
"""

import unittest
from unittest.mock import MagicMock

from core.config_loader import ScorerConfig
from core.scorer import penalties


class TestPenaltyCalculations(unittest.TestCase):
    """Unit tests for penalty calculation functions."""

    def setUp(self):
        """Set up test configuration."""
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True,
            min_salary=5000000
        )

    def test_01_penalty_location_mismatch(self):
        """Test location penalty for non-remote job when wanting remote."""
        print("\n📊 UNIT Test 1: Location Mismatch Penalty")

        # Create non-remote job
        job = MagicMock()
        job.is_remote = False
        job.location_text = "Tokyo"
        job.salary_max = None
        job.job_level = None

        penalties_score, details = penalties.calculate_penalties(
            job, [], [], self.scorer_config
        )

        # Should have location penalty (config wants remote)
        self.assertGreater(penalties_score, 0)
        location_penalty = next((d for d in details if d['type'] == 'location_mismatch'), None)
        self.assertIsNotNone(location_penalty)

        print(f"  ✓ Penalties: {penalties_score:.1f}")
        print(f"  ✓ Has location penalty: {location_penalty is not None}")

    def test_02_penalty_no_location_for_remote_job(self):
        """Test no location penalty for remote job when wanting remote."""
        print("\n📊 UNIT Test 2: No Location Penalty for Remote Job")

        # Create remote job
        job = MagicMock()
        job.is_remote = True
        job.location_text = "Remote"
        job.salary_max = None
        job.job_level = None

        penalties_score, details = penalties.calculate_penalties(
            job, [], [], self.scorer_config
        )

        # Should not have location penalty
        location_penalty = next((d for d in details if d['type'] == 'location_mismatch'), None)
        self.assertIsNone(location_penalty)

        print(f"  ✓ Penalties: {penalties_score:.1f}")
        print(f"  ✓ No location penalty: {location_penalty is None}")

    def test_03_penalty_salary_too_low(self):
        """Test salary penalty when job salary below minimum."""
        print("\n📊 UNIT Test 3: Salary Penalty")

        # Create job with low salary
        job = MagicMock()
        job.is_remote = True
        job.location_text = "Remote"
        job.salary_max = 4000000  # Below min_salary of 5000000
        job.job_level = None

        penalties_score, details = penalties.calculate_penalties(
            job, [], [], self.scorer_config
        )

        # Should have salary/penalty_compensation_mismatch penalty (not 'salary_too_low')
        salary_penalty = next((d for d in details if d['type'] == 'compensation_mismatch'), None)
        self.assertIsNotNone(salary_penalty)

        print(f"  ✓ Penalties: {penalties_score:.1f}")
        print(f"  ✓ Has salary penalty: {salary_penalty is not None}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
