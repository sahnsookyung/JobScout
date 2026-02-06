import unittest
import os
import yaml
from unittest.mock import patch, mock_open
from core.config_loader import load_config, AppConfig, LlmConfig

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        self.sample_config = {
            "database": {"url": "postgresql://user:pass@localhost:5432/db"},
            "jobspy": {"url": "http://localhost:8000"},
            "etl": {
                "llm": {
                    "base_url": "http://ollama:11434",
                    "extraction_type": "ollama",
                    "extraction_model": "qwen3:14b"
                }
            },
            "schedule": {"interval_seconds": 3600},
            "scrapers": []
        }
        self.config_yaml = yaml.dump(self.sample_config)

    def test_load_config_default(self):
        with patch("builtins.open", mock_open(read_data=self.config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy_path.yaml")
                self.assertIsInstance(config, AppConfig)
                self.assertEqual(config.database.url, "postgresql://user:pass@localhost:5432/db")
                self.assertEqual(config.etl.llm.extraction_model, "qwen3:14b")

    def test_env_var_override_database(self):
        with patch("builtins.open", mock_open(read_data=self.config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, {"DATABASE_URL": "postgresql://env:pass@envhost:5432/db"}):
                    config = load_config("dummy_path.yaml")
                    self.assertEqual(config.database.url, "postgresql://env:pass@envhost:5432/db")

    def test_env_var_override_jobspy(self):
        with patch("builtins.open", mock_open(read_data=self.config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, {"JOBSPY_URL": "http://env-jobspy:8000"}):
                    config = load_config("dummy_path.yaml")
                    self.assertEqual(config.jobspy.url, "http://env-jobspy:8000")

    def test_llm_config_defaults(self):
        # Test loading a config without explicit LLM settings (should use defaults from pydantic model)
        minimal_config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })
        with patch("builtins.open", mock_open(read_data=minimal_config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")
                # Defaults from LlmConfig
                self.assertEqual(config.etl.llm.extraction_type, "openai")
                self.assertEqual(config.etl.llm.extraction_model, "gpt-4o-mini")

    def test_notification_config_loading(self):
        """Test loading notification configuration from YAML."""
        config_with_notifications = {
            "database": {"url": "postgresql://user:pass@localhost:5432/db"},
            "schedule": {"interval_seconds": 3600},
            "notifications": {
                "enabled": True,
                "user_id": "test_user_123",
                "min_score_threshold": 75.0,
                "notify_on_new_match": True,
                "notify_on_batch_complete": False,
                "channels": {
                    "email": {"enabled": True, "recipient": "test@example.com"},
                    "discord": {"enabled": True, "recipient": "https://discord.com/webhook"},
                    "telegram": {"enabled": False}
                },
                "deduplication_enabled": True,
                "resend_interval_hours": 12
            },
            "scrapers": []
        }
        config_yaml = yaml.dump(config_with_notifications)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")
                
                self.assertIsNotNone(config.notifications)
                self.assertTrue(config.notifications.enabled)
                self.assertEqual(config.notifications.user_id, "test_user_123")
                self.assertEqual(config.notifications.min_score_threshold, 75.0)
                self.assertTrue(config.notifications.notify_on_new_match)
                self.assertFalse(config.notifications.notify_on_batch_complete)
                self.assertTrue(config.notifications.deduplication_enabled)
                self.assertEqual(config.notifications.resend_interval_hours, 12)
                
                # Check channels
                self.assertIn("email", config.notifications.channels)
                self.assertIn("discord", config.notifications.channels)
                self.assertIn("telegram", config.notifications.channels)
                
                self.assertTrue(config.notifications.channels["email"].enabled)
                self.assertEqual(config.notifications.channels["email"].recipient, "test@example.com")
                self.assertFalse(config.notifications.channels["telegram"].enabled)

    def test_notification_config_defaults(self):
        """Test notification configuration defaults when not specified."""
        minimal_config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })
        
        with patch("builtins.open", mock_open(read_data=minimal_config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")
                
                # Notifications should have default config when not specified (enabled=False by default)
                self.assertIsNotNone(config.notifications)
                self.assertFalse(config.notifications.enabled)

    def test_notification_config_partial(self):
        """Test loading partial notification configuration."""
        partial_config = {
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "notifications": {
                "enabled": True,
                "user_id": "user456"
                # Other fields will use defaults
            },
            "scrapers": []
        }
        config_yaml = yaml.dump(partial_config)
        
        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")
                
                self.assertTrue(config.notifications.enabled)
                self.assertEqual(config.notifications.user_id, "user456")
                # Check defaults
                self.assertEqual(config.notifications.min_score_threshold, 70.0)  # Default
                self.assertTrue(config.notifications.notify_on_new_match)  # Default
                self.assertTrue(config.notifications.deduplication_enabled)  # Default

if __name__ == "__main__":
    unittest.main()
