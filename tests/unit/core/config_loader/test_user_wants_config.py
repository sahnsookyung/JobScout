#!/usr/bin/env python3
"""
Unit tests for user_wants_file configuration loading.

Tests the MatchingConfig model and load_config() function
for proper handling of the user_wants_file option.
"""

import unittest
import os
import yaml
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path

from core.config_loader import (
    load_config, 
    AppConfig, 
    MatchingConfig, 
    FacetWeights,
    ScorerConfig
)


class TestMatchingConfigUserWants(unittest.TestCase):
    """Tests for MatchingConfig user_wants_file field."""

    def test_user_wants_file_default_none(self):
        """Default user_wants_file should be None."""
        config = MatchingConfig()
        self.assertIsNone(config.user_wants_file)

    def test_user_wants_file_explicit_path(self):
        """Should accept explicit file path."""
        config = MatchingConfig(user_wants_file="my_wants.txt")
        self.assertEqual(config.user_wants_file, "my_wants.txt")

    def test_user_wants_file_absolute_path(self):
        """Should accept absolute file path."""
        config = MatchingConfig(user_wants_file="/home/user/wants.txt")
        self.assertEqual(config.user_wants_file, "/home/user/wants.txt")

    def test_user_wants_file_relative_path(self):
        """Should accept relative file path."""
        config = MatchingConfig(user_wants_file="./config/wants.txt")
        self.assertEqual(config.user_wants_file, "./config/wants.txt")

    def test_user_wants_file_empty_string(self):
        """Empty string should be valid (though not recommended)."""
        config = MatchingConfig(user_wants_file="")
        self.assertEqual(config.user_wants_file, "")

    def test_user_wants_file_with_spaces(self):
        """Should handle paths with spaces."""
        config = MatchingConfig(user_wants_file="my wants file.txt")
        self.assertEqual(config.user_wants_file, "my wants file.txt")

    def test_user_wants_file_unicode_path(self):
        """Should handle Unicode paths."""
        config = MatchingConfig(user_wants_file="願い.txt")
        self.assertEqual(config.user_wants_file, "願い.txt")

    def test_user_wants_file_special_chars(self):
        """Should handle paths with special characters."""
        config = MatchingConfig(user_wants_file="wants-v2.0_final.txt")
        self.assertEqual(config.user_wants_file, "wants-v2.0_final.txt")


class TestLoadConfigUserWants(unittest.TestCase):
    """Tests for load_config with user_wants_file."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "database": {"url": "postgresql://user:pass@localhost/db"},
            "schedule": {"interval_seconds": 3600},
            "scrapers": []
        }

    def test_load_config_without_user_wants_file(self):
        """Config without user_wants_file should have None."""
        config_yaml = yaml.dump(self.base_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertIsNone(config.matching.user_wants_file)

    def test_load_config_with_user_wants_file(self):
        """Should load user_wants_file from config."""
        self.base_config["matching"] = {
            "user_wants_file": "my_wants.txt"
        }
        config_yaml = yaml.dump(self.base_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertEqual(config.matching.user_wants_file, "my_wants.txt")

    def test_load_config_with_nested_matching_config(self):
        """Should load user_wants_file in nested matching config."""
        self.base_config["matching"] = {
            "enabled": True,
            "user_wants_file": "config/wants.txt",
            "scorer": {
                "fit_weight": 0.6,
                "want_weight": 0.4
            }
        }
        config_yaml = yaml.dump(self.base_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertEqual(config.matching.user_wants_file, "config/wants.txt")
        self.assertEqual(config.matching.scorer.fit_weight, 0.6)
        self.assertEqual(config.matching.scorer.want_weight, 0.4)

    def test_load_config_user_wants_file_none_explicit(self):
        """Explicit null should result in None."""
        self.base_config["matching"] = {
            "user_wants_file": None
        }
        config_yaml = yaml.dump(self.base_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertIsNone(config.matching.user_wants_file)

    def test_load_config_preserves_other_matching_settings(self):
        """Loading user_wants_file should not affect other matching settings."""
        self.base_config["matching"] = {
            "enabled": False,
            "user_wants_file": "wants.txt",
            "invalidate_on_job_change": False,
            "recalculate_existing": True
        }
        config_yaml = yaml.dump(self.base_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertFalse(config.matching.enabled)
        self.assertEqual(config.matching.user_wants_file, "wants.txt")
        self.assertFalse(config.matching.invalidate_on_job_change)
        self.assertTrue(config.matching.recalculate_existing)


class TestFacetWeightsConfig(unittest.TestCase):
    """Tests for FacetWeights configuration."""

    def test_default_facet_weights(self):
        """Default facet weights should sum to 1.0."""
        weights = FacetWeights()
        total = (
            weights.remote_flexibility +
            weights.compensation +
            weights.learning_growth +
            weights.company_culture +
            weights.work_life_balance +
            weights.tech_stack +
            weights.visa_sponsorship
        )
        self.assertAlmostEqual(total, 1.0, places=2)

    def test_custom_facet_weights(self):
        """Should accept custom facet weights."""
        weights = FacetWeights(
            remote_flexibility=0.5,
            compensation=0.1,
            learning_growth=0.1,
            company_culture=0.1,
            work_life_balance=0.1,
            tech_stack=0.05,
            visa_sponsorship=0.05
        )
        self.assertEqual(weights.remote_flexibility, 0.5)
        self.assertEqual(weights.compensation, 0.1)

    def test_facet_weights_from_config(self):
        """Should load facet weights from config."""
        config_dict = {
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "matching": {
                "scorer": {
                    "facet_weights": {
                        "remote_flexibility": 0.4,
                        "compensation": 0.3
                    }
                }
            },
            "scrapers": []
        }
        config_yaml = yaml.dump(config_dict)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        self.assertEqual(config.matching.scorer.facet_weights.remote_flexibility, 0.4)
        self.assertEqual(config.matching.scorer.facet_weights.compensation, 0.3)


class TestScorerConfigFitWantWeights(unittest.TestCase):
    """Tests for Fit/Want weight configuration."""

    def test_default_fit_want_weights(self):
        """Default fit/want weights should be 0.7/0.3."""
        scorer = ScorerConfig()
        self.assertEqual(scorer.fit_weight, 0.7)
        self.assertEqual(scorer.want_weight, 0.3)

    def test_custom_fit_want_weights(self):
        """Should accept custom fit/want weights."""
        scorer = ScorerConfig(fit_weight=0.5, want_weight=0.5)
        self.assertEqual(scorer.fit_weight, 0.5)
        self.assertEqual(scorer.want_weight, 0.5)

    def test_fit_want_weights_sum_validation(self):
        """Weights don't need to sum to 1.0 (not enforced in model)."""
        scorer = ScorerConfig(fit_weight=1.0, want_weight=1.0)
        self.assertEqual(scorer.fit_weight, 1.0)
        self.assertEqual(scorer.want_weight, 1.0)


class TestUserWantsFilePathResolution(unittest.TestCase):
    """Tests for user_wants_file path resolution behavior."""

    def test_relative_path_resolution(self):
        """Relative paths should be resolved relative to config location."""
        # This documents the expected behavior - actual resolution happens at runtime
        config = MatchingConfig(user_wants_file="wants.txt")
        
        # The path is stored as-is, resolution happens when loading
        self.assertEqual(config.user_wants_file, "wants.txt")

    def test_path_with_variables(self):
        """Should handle paths with environment variable syntax (stored as-is)."""
        config = MatchingConfig(user_wants_file="$HOME/wants.txt")
        self.assertEqual(config.user_wants_file, "$HOME/wants.txt")


class TestUserWantsIntegrationWithMatching(unittest.TestCase):
    """Integration tests for user_wants_file with full matching config."""

    def test_full_matching_config_with_wants(self):
        """Should handle complete matching config with user_wants_file."""
        config_dict = {
            "database": {"url": "postgresql://localhost/db"},
            "matching": {
                "enabled": True,
                "user_wants_file": "config/user_wants.txt",
                "matcher": {
                    "similarity_threshold": 0.6,
                    "batch_size": 100
                },
                "scorer": {
                    "fit_weight": 0.65,
                    "want_weight": 0.35,
                    "facet_weights": {
                        "remote_flexibility": 0.2,
                        "compensation": 0.25
                    }
                },
                "result_policy": {
                    "min_fit": 60.0,
                    "top_k": 50
                }
            },
            "schedule": {"interval_seconds": 3600},
            "scrapers": []
        }
        config_yaml = yaml.dump(config_dict)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy.yaml")
                
        # Verify all nested config loaded correctly
        self.assertTrue(config.matching.enabled)
        self.assertEqual(config.matching.user_wants_file, "config/user_wants.txt")
        self.assertEqual(config.matching.matcher.similarity_threshold, 0.6)
        self.assertEqual(config.matching.scorer.fit_weight, 0.65)
        self.assertEqual(config.matching.scorer.want_weight, 0.35)
        self.assertEqual(config.matching.result_policy.top_k, 50)


if __name__ == '__main__':
    unittest.main()
