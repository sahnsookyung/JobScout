#!/usr/bin/env python3
"""
Unit tests for Fit/Want scoring modules.
"""

import unittest
import numpy as np
from unittest.mock import Mock, patch

from core.scorer.fit_score import calculate_fit_score
from core.scorer.want_score import calculate_want_score
from core.config_loader import ScorerConfig, FacetWeights


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
        return 0.0
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def normalize_similarity(sim: float) -> float:
    """Map cosine similarity from [-1, 1] to [0, 1]."""
    return max(0.0, (sim + 1.0) / 2.0)


class TestCosineSimilarity(unittest.TestCase):
    """Tests for cosine_similarity function."""

    def test_identical_vectors(self):
        """Identical vectors should have similarity of 1.0"""
        a = np.array([1.0, 0.0, 0.0])
        b = np.array([1.0, 0.0, 0.0])
        self.assertAlmostEqual(cosine_similarity(a, b), 1.0)

    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity of 0.0"""
        a = np.array([1.0, 0.0, 0.0])
        b = np.array([0.0, 1.0, 0.0])
        self.assertAlmostEqual(cosine_similarity(a, b), 0.0)

    def test_opposite_vectors(self):
        """Opposite vectors should have similarity of -1.0"""
        a = np.array([1.0, 0.0, 0.0])
        b = np.array([-1.0, 0.0, 0.0])
        self.assertAlmostEqual(cosine_similarity(a, b), -1.0)

    def test_zero_vector(self):
        """Zero vector should return 0.0"""
        a = np.array([0.0, 0.0, 0.0])
        b = np.array([1.0, 2.0, 3.0])
        self.assertEqual(cosine_similarity(a, b), 0.0)

    def test_normalize_similarity(self):
        """Test similarity normalization from [-1, 1] to [0, 1]"""
        self.assertEqual(normalize_similarity(1.0), 1.0)
        self.assertEqual(normalize_similarity(0.0), 0.5)
        self.assertEqual(normalize_similarity(-1.0), 0.0)


class TestFitScore(unittest.TestCase):
    """Tests for calculate_fit_score function."""

    def setUp(self):
        self.config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            penalty_missing_required=15.0,
            penalty_seniority_mismatch=10.0,
            penalty_compensation_mismatch=10.0,
            penalty_experience_shortfall=15.0
        )

    def test_fit_score_capped_at_100(self):
        """Fit score should never exceed 100"""
        fit_score, components = calculate_fit_score(
            job_similarity=0.95,
            required_coverage=0.95,
            preferred_coverage=0.95,
            fit_penalties=0,
            config=self.config
        )
        self.assertLessEqual(fit_score, 100.0)

    def test_fit_score_zero_penalties_perfect_match(self):
        """Perfect match with zero penalties should give high score"""
        fit_score, components = calculate_fit_score(
            job_similarity=1.0,
            required_coverage=1.0,
            preferred_coverage=1.0,
            fit_penalties=0,
            config=self.config
        )
        self.assertGreater(fit_score, 90.0)

    def test_fit_score_with_missing_required(self):
        """Missing required skills should reduce fit score"""
        fit_score_no_missing, _ = calculate_fit_score(
            job_similarity=0.8,
            required_coverage=1.0,
            preferred_coverage=0.8,
            fit_penalties=0,
            config=self.config
        )
        fit_score_missing, _ = calculate_fit_score(
            job_similarity=0.8,
            required_coverage=0.5,
            preferred_coverage=0.8,
            fit_penalties=0,
            config=self.config
        )
        self.assertGreater(fit_score_no_missing, fit_score_missing)

    def test_fit_score_negative_penalties_clamped(self):
        """Negative fit penalties should not artificially inflate score"""
        fit_score, components = calculate_fit_score(
            job_similarity=0.5,
            required_coverage=0.5,
            preferred_coverage=0.5,
            fit_penalties=-10,
            config=self.config
        )
        self.assertLessEqual(fit_score, 100.0)
        self.assertGreaterEqual(fit_score, 0.0)

    def test_fit_score_components_contain_expected_keys(self):
        """Fit score components should contain expected keys"""
        fit_score, components = calculate_fit_score(
            job_similarity=0.8,
            required_coverage=0.9,
            preferred_coverage=0.7,
            fit_penalties=10,
            config=self.config
        )
        self.assertIn('job_similarity', components)
        self.assertIn('required_coverage', components)
        self.assertIn('preferred_coverage', components)
        self.assertIn('fit_penalties', components)
        self.assertIn('fit_score', components)


class TestWantScore(unittest.TestCase):
    """Tests for calculate_want_score function."""

    def setUp(self):
        self.facet_weights = FacetWeights(
            remote_flexibility=0.15,
            compensation=0.20,
            learning_growth=0.15,
            company_culture=0.15,
            work_life_balance=0.15,
            tech_stack=0.10,
            visa_sponsorship=0.10
        )

    def test_want_score_no_user_wants(self):
        """Empty user wants should result in zero want score"""
        want_score, components = calculate_want_score(
            user_want_embeddings=[],
            job_facet_embeddings={'remote_flexibility': np.random.rand(1024)},
            facet_weights=self.facet_weights
        )
        self.assertEqual(want_score, 0.0)

    def test_want_score_no_job_facets(self):
        """Missing job facet embeddings should result in zero want score"""
        want_score, components = calculate_want_score(
            user_want_embeddings=[np.random.rand(1024)],
            job_facet_embeddings={},
            facet_weights=self.facet_weights
        )
        self.assertEqual(want_score, 0.0)

    def test_want_score_perfect_match(self):
        """Perfect match should give high want score"""
        embedding = np.random.rand(1024)
        want_score, components = calculate_want_score(
            user_want_embeddings=[embedding, embedding],
            job_facet_embeddings={
                'remote_flexibility': embedding,
                'compensation': embedding,
                'tech_stack': embedding
            },
            facet_weights=self.facet_weights
        )
        self.assertGreater(want_score, 80.0)

    def test_want_score_poor_match(self):
        """Poor match should give low want score"""
        embedding1 = np.random.rand(1024)
        embedding2 = -embedding1  # Opposite vectors
        want_score, components = calculate_want_score(
            user_want_embeddings=[embedding1],
            job_facet_embeddings={
                'remote_flexibility': embedding2,
                'compensation': embedding2
            },
            facet_weights=self.facet_weights
        )
        self.assertLess(want_score, 50.0)

    def test_want_score_components_contain_expected_keys(self):
        """Want score components should contain expected keys"""
        embedding = np.random.rand(1024)
        want_score, components = calculate_want_score(
            user_want_embeddings=[embedding],
            job_facet_embeddings={'remote_flexibility': embedding},
            facet_weights=self.facet_weights
        )
        self.assertIn('num_wants', components)
        self.assertIn('num_facets', components)
        self.assertIn('aggregate_similarity', components)
        self.assertIn('want_score', components)

    def test_want_score_capped_at_100(self):
        """Want score should never exceed 100"""
        embedding = np.random.rand(1024)
        want_score, components = calculate_want_score(
            user_want_embeddings=[embedding] * 10,
            job_facet_embeddings={'remote_flexibility': embedding},
            facet_weights=self.facet_weights
        )
        self.assertLessEqual(want_score, 100.0)


class TestOverallScore(unittest.TestCase):
    """Tests for overall score calculation (Fit + Want weights)."""

    def test_overall_weighted_combination(self):
        """Overall should be weighted combination of Fit and Want"""
        fit_score = 80.0
        want_score = 70.0
        fit_weight = 0.7
        want_weight = 0.3

        overall = min(100.0, fit_weight * fit_score + want_weight * want_score)
        self.assertAlmostEqual(overall, 77.0)

    def test_overall_clamped_to_100(self):
        """High individual scores should not exceed 100 overall"""
        overall = min(100.0, 0.7 * 100 + 0.3 * 100)
        self.assertEqual(overall, 100.0)

    def test_overall_with_zero_fit(self):
        """Overall with zero fit should equal weighted want"""
        overall = min(100.0, 0.7 * 0 + 0.3 * 80)
        self.assertEqual(overall, 24.0)

    def test_overall_with_zero_want(self):
        """Overall with zero want should equal weighted fit"""
        overall = min(100.0, 0.7 * 80 + 0.3 * 0)
        self.assertEqual(overall, 56.0)


if __name__ == '__main__':
    unittest.main()
