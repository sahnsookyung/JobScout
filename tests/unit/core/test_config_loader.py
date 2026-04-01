import unittest
import os
import yaml
from unittest.mock import patch, mock_open
from core.config_loader import AppConfig, LlmConfig, ScorerConfig, load_config

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        self.sample_config = {
            "database": {"url": "postgresql://user:pass@localhost:5432/db"},
            "jobspy": {"url": "http://localhost:8000"},  # NOSONAR - local test service
            "etl": {
                "llm": {
                    "base_url": "http://ollama:11434",  # NOSONAR - local test service
                    "extraction_type": "ollama",
                    "extraction_model": "qwen3:14b"
                }
            },
            "preferences": {
                "default_mode": "semantic_rerank",
                "allowed_modes": ["semantic_rerank", "llm_judge"],
                "parser": {
                    "base_url": "http://preferences-llm:11434/v1",
                    "model": "qwen3:14b"
                }
            },
            "schedule": {"interval_seconds": 3600},
            "scrapers": []
        }
        self.config_yaml = yaml.dump(self.sample_config)

    def test_load_config_default(self):
        # Clear env vars to test YAML loading without environment overrides
        env_to_clear = [
            "ETL_LLM_EXTRACTION_MODEL",
            "ETL_LLM_EXTRACTION_BASE_URL",
            "ETL_LLM_EXTRACTION_API_KEY",
            "ETL_LLM_EXTRACTION_API_SECRET",
            "ETL_EMBEDDING_MODEL",
            "ETL_EMBEDDING_BASE_URL",
            "ETL_EMBEDDING_API_KEY",
            "ETL_EMBEDDING_API_SECRET",
            "DATABASE_URL",
            "JOBSPY_URL",
            "REDIS_URL",
            "NOTIFICATION_EMAIL",
            "EMAIL",
            "DISCORD_WEBHOOK_URL",
            "TELEGRAM_CHAT_ID",
            "NOTIFICATION_WEBHOOK_URL",
            "TELEGRAM_BOT_TOKEN",
            "SMTP_SERVER",
            "SMTP_PORT",
            "SMTP_USERNAME",
            "SMTP_PASSWORD",
            "SMTP_USE_TLS",
            "FROM_EMAIL",
            "NOTIFICATION_DRY_RUN",
        ]
        with patch("builtins.open", mock_open(read_data=self.config_yaml)):
            with patch("os.path.exists", return_value=True):
                # Save and clear env vars
                saved_env = {k: os.environ.pop(k, None) for k in env_to_clear}
                try:
                    config = load_config("dummy_path.yaml")
                    self.assertIsInstance(config, AppConfig)
                    self.assertEqual(config.database.url, "postgresql://user:pass@localhost:5432/db")
                    self.assertEqual(config.etl.llm.extraction_model, "qwen3:14b")
                    self.assertEqual(config.preferences.default_mode, "semantic_rerank")
                    self.assertEqual(config.preferences.parser.model, "qwen3:14b")
                finally:
                    # Restore env vars
                    for k, v in saved_env.items():
                        if v is not None:
                            os.environ[k] = v

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
        env_to_clear = [
            "ETL_LLM_EXTRACTION_MODEL",
            "ETL_LLM_EXTRACTION_BASE_URL",
            "ETL_LLM_EXTRACTION_API_KEY",
            "ETL_LLM_EXTRACTION_API_SECRET",
            "ETL_EMBEDDING_MODEL",
            "ETL_EMBEDDING_BASE_URL",
            "ETL_EMBEDDING_API_KEY",
            "ETL_EMBEDDING_API_SECRET",
        ]
        with patch("builtins.open", mock_open(read_data=minimal_config_yaml)):
            with patch("os.path.exists", return_value=True):
                # Save and clear env vars
                saved_env = {k: os.environ.pop(k, None) for k in env_to_clear}
                try:
                    config = load_config("dummy")
                    # Defaults from LlmConfig
                    self.assertEqual(config.etl.llm.extraction_type, "openai")
                    self.assertEqual(config.etl.llm.extraction_model, "gpt-4o-mini")
                finally:
                    # Restore env vars
                    for k, v in saved_env.items():
                        if v is not None:
                            os.environ[k] = v

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

    def test_notification_env_overrides_are_loaded_into_shared_config(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "notifications": {
                "channels": {
                    "email": {"enabled": True, "recipient": "yaml@example.com"},
                    "discord": {"enabled": True, "recipient": "https://yaml.example/hook"},
                }
            },
            "scrapers": []
        })

        env = {
            "NOTIFICATION_EMAIL": "env@example.com",
            "DISCORD_WEBHOOK_URL": "https://env.example/discord",
            "TELEGRAM_CHAT_ID": "@env_channel",
            "NOTIFICATION_WEBHOOK_URL": "https://env.example/webhook",
            "TELEGRAM_BOT_TOKEN": "env-token",
            "BASE_URL": "https://jobscout.example",
            "NOTIFICATION_RATE_LIMIT_MAX_WAIT": "17",
            "SMTP_SERVER": "smtp.env.example",
            "SMTP_PORT": "2525",
            "SMTP_USERNAME": "mailer",
            "SMTP_PASSWORD": "secret",
            "SMTP_USE_TLS": "false",
            "FROM_EMAIL": "noreply@env.example",
            "NOTIFICATION_DRY_RUN": "true",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=False):
                    config = load_config("dummy")

        self.assertEqual(config.notifications.channels["email"].recipient, "env@example.com")
        self.assertEqual(
            config.notifications.channels["discord"].recipient,
            "https://env.example/discord",
        )
        self.assertEqual(config.notifications.channels["telegram"].recipient, "@env_channel")
        self.assertEqual(
            config.notifications.channels["webhook"].recipient,
            "https://env.example/webhook",
        )
        self.assertEqual(config.notifications.telegram_bot_token, "env-token")
        self.assertEqual(config.notifications.smtp.server, "smtp.env.example")
        self.assertEqual(config.notifications.smtp.port, 2525)
        self.assertEqual(config.notifications.smtp.username, "mailer")
        self.assertEqual(config.notifications.smtp.password, "secret")
        self.assertFalse(config.notifications.smtp.use_tls)
        self.assertEqual(config.notifications.smtp.from_email, "noreply@env.example")
        self.assertEqual(config.notifications.base_url, "https://jobscout.example")
        self.assertEqual(config.notifications.rate_limit_max_wait_seconds, 17)
        self.assertTrue(config.notifications.dry_run)

    def test_preference_env_overrides_use_independent_namespace(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "preferences": {
                "default_mode": "semantic_rerank",
                "parser": {"model": "yaml-parser"},
            },
            "scrapers": []
        })

        env = {
            "PREFERENCES_DEFAULT_MODE": "llm_judge",
            "PREFERENCES_PARSER_MODEL": "env-parser",
            "ETL_LLM_EXTRACTION_MODEL": "etl-model",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=False):
                    config = load_config("dummy")

        self.assertEqual(config.preferences.default_mode, "llm_judge")
        self.assertEqual(config.preferences.parser.model, "env-parser")
        self.assertEqual(config.etl.llm.extraction_model, "etl-model")

    def test_semantic_fit_defaults_are_loaded(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")

        self.assertTrue(config.matching.matcher.hybrid_retrieval_enabled)
        self.assertTrue(config.matching.scorer.semantic_fit.enabled)
        self.assertEqual(config.matching.scorer.semantic_fit.default_mode, "cross_encoder")
        self.assertEqual(config.matching.scorer.semantic_fit.recall_top_k, 5)

    def test_legacy_semantic_fit_flags_sync_into_nested_config(self):
        scorer_config = ScorerConfig(
            semantic_fit_enabled=False,
            semantic_fit_fallback_to_threshold=False,
        )

        self.assertFalse(scorer_config.semantic_fit.enabled)
        self.assertFalse(scorer_config.semantic_fit.threshold_fallback_enabled)
        self.assertFalse(scorer_config.semantic_fit_enabled)
        self.assertFalse(scorer_config.semantic_fit_fallback_to_threshold)

    def test_semantic_fit_env_overrides_use_fit_namespace(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })

        env = {
            "FIT_SEMANTIC_DEFAULT_MODE": "llm",
            "FIT_SEMANTIC_RECALL_TOP_K": "7",
            "FIT_LLM_BASE_URL": "https://fit-llm.example/v1",
            "FIT_LLM_API_KEY": "fit-key",
            "FIT_LLM_MODEL": "fit-gpt",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=False):
                    config = load_config("dummy")

        self.assertEqual(config.matching.scorer.semantic_fit.default_mode, "llm")
        self.assertEqual(config.matching.scorer.semantic_fit.recall_top_k, 7)
        self.assertEqual(config.matching.scorer.semantic_fit.llm.base_url, "https://fit-llm.example/v1")
        self.assertEqual(config.matching.scorer.semantic_fit.llm.api_key, "fit-key")
        self.assertEqual(config.matching.scorer.semantic_fit.llm.model, "fit-gpt")

if __name__ == "__main__":
    unittest.main()
