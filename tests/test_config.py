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
                "mock": False,
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

if __name__ == "__main__":
    unittest.main()
