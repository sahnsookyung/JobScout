import unittest
import json
import os
import yaml
from unittest.mock import patch, mock_open
from pydantic import ValidationError

from core.config_loader import (
    AppConfig,
    LlmJudgeProviderRuntimeConfig,
    LlmJudgeRuntimeConfig,
    MatcherConfig,
    PreferencesConfig,
    ScorerConfig,
    SemanticFitConfig,
    SemanticFitCrossEncoderLocalConfig,
    SemanticFitCrossEncoderRemoteConfig,
    SemanticFitLlmConfig,
    apply_env_overrides,
    load_config,
    resolve_config_path,
)

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        self.sample_config = {
            "database": {"url": "postgresql://user:pass@localhost:5432/db"},
            "jobspy": {"url": "http://localhost:8000"},  # NOSONAR - local test service
            "etl": {
                "llm": {
                    "base_url": "http://ollama:11434",  # NOSONAR - local test service
                    "provider": "openai_compatible",
                    "extraction_model": "qwen3:14b"
                }
            },
            "preferences": {
                "default_mode": "semantic_rerank",
                "allowed_modes": ["semantic_rerank", "llm_judge"],
                "parser": {
                    "provider": "openai_compatible",
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
            "ETL_LLM_EXTRACTION_STRUCTURED_OUTPUT_MODE",
            "ETL_LLM_STRUCTURED_OUTPUT_MODE",
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
            "LLM_AS_A_JUDGE_BASE_URL",
            "LLM_AS_A_JUDGE_API_KEY",
            "LLM_AS_A_JUDGE_PROVIDERS_JSON",
            "MATCH_LLM_JUDGE_AUTO_ENQUEUE_ENABLED",
            "NVIDIA_API_KEY",
            "NVIDIA_MODEL",
            "NVIDIA_MAX_CONTEXT",
            "NVIDIA_REQUESTS_PER_MINUTE",
            "NVIDIA_RATE_LIMIT_MAX_WAIT_SECONDS",
            "NVIDIA_FALLBACK_ON_RATE_LIMIT",
            "CEREBRAS_API_KEY",
            "GROQ_API_KEY",
            "LLM_AS_A_JUDGE_MODEL",
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

    def test_blank_jobspy_env_disables_optional_api(self):
        with patch("builtins.open", mock_open(read_data=self.config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, {"JOBSPY_URL": ""}, clear=False):
                    config = load_config("dummy_path.yaml")

        self.assertIsNone(config.jobspy)

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
            "ETL_LLM_EXTRACTION_STRUCTURED_OUTPUT_MODE",
            "ETL_LLM_STRUCTURED_OUTPUT_MODE",
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
                    self.assertEqual(config.etl.llm.provider, "openai_compatible")
                    self.assertEqual(config.etl.llm.extraction_model, "gpt-4o-mini")
                    self.assertIsNone(config.etl.llm.structured_output_mode)
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
                "min_fit_for_alerts": 75.0,
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
                self.assertEqual(config.notifications.min_fit_for_alerts, 75.0)
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
                self.assertEqual(config.notifications.min_fit_for_alerts, 70.0)  # Default
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
                with patch.dict(os.environ, env, clear=True):
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
            "PREFERENCES_PARSER_PROVIDER": "openai_compatible",
            "ETL_LLM_EXTRACTION_MODEL": "etl-model",
            "ETL_LLM_PROVIDER": "openai_compatible",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        self.assertEqual(config.preferences.default_mode, "llm_judge")
        self.assertEqual(config.preferences.parser.model, "env-parser")
        self.assertEqual(config.preferences.parser.provider, "openai_compatible")
        self.assertEqual(config.etl.llm.extraction_model, "etl-model")
        self.assertEqual(config.etl.llm.provider, "openai_compatible")

    def test_etl_structured_output_mode_env_override(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "etl": {"llm": {"extraction_model": "yaml-model"}},
            "scrapers": []
        })

        env = {
            "ETL_LLM_STRUCTURED_OUTPUT_MODE": "json_object",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        self.assertEqual(config.etl.llm.structured_output_mode, "json_object")

    def test_semantic_fit_defaults_are_loaded(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "schedule": {"interval_seconds": 60},
            "matching": {
                "scorer": {
                    "semantic_fit": {
                        "deploy_allowed_modes": ["cross_encoder"],
                        "baseline_allowed_modes": ["cross_encoder"],
                    }
                }
            },
            "scrapers": []
        })

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                config = load_config("dummy")

        self.assertTrue(config.matching.matcher.hybrid_retrieval_enabled)
        self.assertTrue(config.matching.scorer.semantic_fit.enabled)
        self.assertEqual(config.matching.scorer.semantic_fit.default_mode, "cross_encoder")
        self.assertEqual(config.matching.scorer.semantic_fit.recall_top_k, 5)
        self.assertEqual(
            config.matching.scorer.semantic_fit.cross_encoder.local.runtime,
            "auto",
        )
        self.assertEqual(
            config.matching.scorer.semantic_fit.cross_encoder.local.model_name,
            "BAAI/bge-reranker-v2-m3",
        )

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
            "matching": {
                "scorer": {
                    "semantic_fit": {
                        "deploy_allowed_modes": ["cross_encoder", "llm"],
                        "baseline_allowed_modes": ["cross_encoder", "llm"],
                        "llm": {"enabled": True},
                    }
                }
            },
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })

        env = {
            "FIT_SEMANTIC_DEFAULT_MODE": "llm",
            "FIT_SEMANTIC_RECALL_TOP_K": "7",
            "FIT_CROSS_ENCODER_LOCAL_RUNTIME": "flag_embedding",
            "FIT_CROSS_ENCODER_LOCAL_MODEL": "BAAI/bge-reranker-v2-gemma",
            "FIT_LLM_PROVIDER": "openai_compatible",
            "FIT_LLM_BASE_URL": "https://fit-llm.example/v1",
            "FIT_LLM_API_KEY": "fit-key",
            "FIT_LLM_MODEL": "fit-gpt",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        self.assertEqual(config.matching.scorer.semantic_fit.default_mode, "llm")
        self.assertEqual(config.matching.scorer.semantic_fit.recall_top_k, 7)
        self.assertEqual(
            config.matching.scorer.semantic_fit.cross_encoder.local.runtime,
            "flag_embedding",
        )
        self.assertEqual(
            config.matching.scorer.semantic_fit.cross_encoder.local.model_name,
            "BAAI/bge-reranker-v2-gemma",
        )
        self.assertEqual(config.matching.scorer.semantic_fit.llm.provider, "openai_compatible")
        self.assertEqual(config.matching.scorer.semantic_fit.llm.base_url, "https://fit-llm.example/v1")
        self.assertEqual(config.matching.scorer.semantic_fit.llm.api_key, "fit-key")
        self.assertEqual(config.matching.scorer.semantic_fit.llm.model, "fit-gpt")

    def test_match_llm_judge_env_overrides_use_dedicated_groq_namespace(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "matching": {
                "llm_judge": {
                    "enabled": False,
                }
            },
            "schedule": {"interval_seconds": 60},
            "scrapers": []
        })

        env = {
            "MATCH_LLM_JUDGE_ENABLED": "true",
            "MATCH_LLM_JUDGE_AUTO_ENQUEUE_ENABLED": "false",
            "MATCH_LLM_JUDGE_TOP_N_DEFAULT": "3",
            "LLM_AS_A_JUDGE_BASE_URL": "https://api.groq.com/openai/v1",
            "GROQ_API_KEY": "groq-key",
            "LLM_AS_A_JUDGE_MODEL": "openai/gpt-oss-20b",
            "LLM_AS_A_JUDGE_STRUCTURED_OUTPUT_MODE": "auto",
            "MATCH_LLM_JUDGE_REQUIREMENT_TEXT_MAX_CHARS": "420",
        }

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        runtime = config.matching.llm_judge.runtime
        self.assertTrue(config.matching.llm_judge.enabled)
        self.assertFalse(config.matching.llm_judge.auto_enqueue_enabled)
        self.assertEqual(config.matching.llm_judge.top_n_default, 3)
        self.assertEqual(runtime.provider, "openai_compatible")
        self.assertEqual(runtime.base_url, "https://api.groq.com/openai/v1")
        self.assertEqual(runtime.api_key, "groq-key")
        self.assertEqual(runtime.model, "openai/gpt-oss-20b")
        self.assertEqual(runtime.structured_output_mode, "auto")
        self.assertEqual(config.matching.llm_judge.requirement_text_max_chars, 420)

    def test_preference_semantic_reranker_top_n_bounds_and_clamping(self):
        config = PreferencesConfig.model_validate(
            {
                "semantic_reranker": {
                    "top_n_default": 250,
                    "top_n_min": 10,
                    "top_n_max": 100,
                }
            }
        )

        self.assertEqual(
            config.preference_rerank_top_n_bounds(),
            {"min": 10, "max": 100, "default": 100},
        )
        self.assertEqual(config.resolve_preference_rerank_top_n(None), 100)
        self.assertEqual(config.resolve_preference_rerank_top_n(1), 10)
        self.assertEqual(config.resolve_preference_rerank_top_n(500), 100)
        self.assertEqual(config.resolve_preference_rerank_top_n(25), 25)

    def test_match_llm_judge_env_does_not_match_groq_lookalike_host(self):
        config_yaml = yaml.dump({
            "database": {"url": "test"},
            "matching": {
                "llm_judge": {
                    "enabled": True,
                    "runtime": {
                        "base_url": "https://api.groq.com.evil.test/openai/v1",
                    },
                }
            },
            "schedule": {"interval_seconds": 60},
            "scrapers": [],
        })
        env = {"GROQ_API_KEY": "groq-key"}

        with patch("builtins.open", mock_open(read_data=config_yaml)):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        runtime = config.matching.llm_judge.runtime
        self.assertEqual(runtime.base_url, "https://api.groq.com.evil.test/openai/v1")
        self.assertIsNone(runtime.api_key)

    def test_semantic_fit_raises_when_default_mode_not_in_deploy_allowed(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                deploy_allowed_modes=["cross_encoder"],
                baseline_allowed_modes=["cross_encoder"],
                default_mode="llm",
            )

        self.assertIn("default_mode must be included in deploy_allowed_modes", str(ctx.exception))

    def test_semantic_fit_raises_when_default_mode_not_in_baseline_allowed(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                deploy_allowed_modes=["cross_encoder", "llm"],
                baseline_allowed_modes=["cross_encoder"],
                default_mode="llm",
                llm={
                    "enabled": True,
                    "base_url": "https://fit-llm.example/v1",
                    "model": "fit-gpt",
                },
            )

        self.assertIn("default_mode must be included in baseline_allowed_modes", str(ctx.exception))

    def test_semantic_fit_raises_for_local_route_without_local_provider(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "local",
                    "local": {"enabled": False},
                }
            )

        self.assertIn("route_policy='local' requires local cross-encoder", str(ctx.exception))

    def test_semantic_fit_raises_for_remote_route_without_remote_provider(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "remote",
                    "remote": {"enabled": False},
                }
            )

        self.assertIn("route_policy='remote' requires remote cross-encoder", str(ctx.exception))

    def test_semantic_fit_raises_for_remote_route_without_remote_base_url(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "remote",
                    "remote": {"enabled": True, "base_url": None},
                }
            )

        self.assertIn("remote.base_url is required", str(ctx.exception))

    def test_semantic_fit_raises_when_llm_is_deploy_allowed_but_disabled(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                deploy_allowed_modes=["cross_encoder", "llm"],
                baseline_allowed_modes=["cross_encoder", "llm"],
                llm={"enabled": False},
            )

        self.assertIn("deploy_allowed_modes includes 'llm' but llm semantic fit is disabled", str(ctx.exception))

    def test_semantic_fit_raises_when_llm_is_enabled_without_base_url(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                llm={
                    "enabled": True,
                    "base_url": None,
                }
            )

        self.assertIn("llm.base_url is required", str(ctx.exception))

    def test_semantic_fit_raises_for_non_positive_serialization_budget(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                serialization={
                    "evidence_text_max_chars": 0,
                }
            )

        self.assertIn("serialization.evidence_text_max_chars must be positive", str(ctx.exception))

    def test_semantic_fit_raises_for_non_positive_recall_top_k(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(recall_top_k=0)

        self.assertIn("semantic_fit.recall_top_k must be positive", str(ctx.exception))

    def test_semantic_fit_raises_for_non_positive_remote_promote_pair_count(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "remote_promote_pair_count": 0,
                }
            )

        self.assertIn("remote_promote_pair_count must be positive", str(ctx.exception))

    def test_semantic_fit_disabled_allows_incomplete_provider_config(self):
        config = SemanticFitConfig(
            enabled=False,
            deploy_allowed_modes=["cross_encoder"],
            baseline_allowed_modes=["cross_encoder"],
            cross_encoder={
                "route_policy": "local",
                "local": {"enabled": False},
            },
            llm={"enabled": True, "base_url": None, "model": "fit-gpt"},
        )

        self.assertFalse(config.enabled)

    def test_semantic_fit_accepts_empty_deploy_allowed_by_falling_back_to_default(self):
        config = SemanticFitConfig(
            deploy_allowed_modes=[],
            baseline_allowed_modes=["cross_encoder"],
            default_mode="cross_encoder",
        )

        self.assertEqual(config.deploy_allowed_modes, ["cross_encoder"])

    def test_semantic_fit_raises_for_invalid_baseline_mode(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                deploy_allowed_modes=["cross_encoder"],
                baseline_allowed_modes=["cross_encoder", "llm"],
            )

        self.assertIn("baseline_allowed_modes contains modes that are not deploy-allowed", str(ctx.exception))

    def test_semantic_fit_raises_for_blank_local_model_name(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "local",
                    "local": {"enabled": True, "model_name": "   "},
                }
            )

        self.assertIn("local.model_name is required", str(ctx.exception))

    def test_semantic_fit_raises_for_blank_remote_model(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "remote",
                    "remote": {
                        "enabled": True,
                        "base_url": "https://fit.example.com",
                        "model": "   ",
                    },
                }
            )

        self.assertIn("remote.model is required", str(ctx.exception))

    def test_semantic_fit_raises_for_auto_route_without_any_provider(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                cross_encoder={
                    "route_policy": "auto",
                    "local": {"enabled": False},
                    "remote": {"enabled": False},
                }
            )

        self.assertIn("route_policy='auto' requires at least one cross-encoder provider", str(ctx.exception))

    def test_matcher_config_raises_for_invalid_limits(self):
        invalid_cases = [
            {"similarity_threshold": 1.5},
            {"batch_size": 0},
            {"lexical_limit": 0},
            {"fusion_rank_constant": 0},
            {"lexical_query_token_limit": 0},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValidationError):
                    MatcherConfig(**overrides)

    def test_cross_encoder_local_config_raises_for_non_positive_limits(self):
        invalid_cases = [
            {"max_batch_size": 0},
            {"max_concurrency": 0},
            {"timeout_ms": 0},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValidationError):
                    SemanticFitCrossEncoderLocalConfig(**overrides)

    def test_cross_encoder_remote_config_raises_for_non_positive_limits(self):
        invalid_cases = [
            {"timeout_ms": 0},
            {"max_batch_size": 0},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValidationError):
                    SemanticFitCrossEncoderRemoteConfig(**overrides)

    def test_llm_config_raises_for_non_positive_limits(self):
        invalid_cases = [
            {"timeout_seconds": 0},
            {"max_input_tokens": 0},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValidationError):
                    SemanticFitLlmConfig(**overrides)

    def test_match_llm_judge_runtime_raises_for_non_positive_limits(self):
        invalid_cases = [
            {"timeout_seconds": 0},
            {"max_input_tokens": 0},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValidationError):
                    LlmJudgeRuntimeConfig(**overrides)

    def test_match_llm_judge_runtime_accepts_groq_provider_alias(self):
        config = LlmJudgeRuntimeConfig(
            provider="groq",
            api_key="groq-key",
            model="llama-3.1-8b-instant",
        )

        self.assertEqual(config.provider, "groq")
        self.assertEqual(config.base_url, "https://api.groq.com/openai/v1")

    def test_match_llm_judge_runtime_accepts_cerebras_provider_alias(self):
        config = LlmJudgeRuntimeConfig(
            provider="cerebras",
            api_key="cerebras-key",
        )

        self.assertEqual(config.provider, "cerebras")
        self.assertEqual(config.base_url, "https://api.cerebras.ai/v1")
        self.assertEqual(config.model, "gpt-oss-120b")
        self.assertEqual(config.timeout_seconds, 60)
        self.assertEqual(config.structured_output_mode, "json_object")
        self.assertEqual(config.max_input_tokens, 24000)

    def test_match_llm_judge_runtime_defaults_provider_chain_from_paired_env_keys(self):
        with patch.dict(
            os.environ,
            {
                "NVIDIA_API_KEY": "nvidia-key",
                "NVIDIA_MODEL": "nvidia-model",
                "NVIDIA_MAX_CONTEXT": "12000",
                "NVIDIA_REQUESTS_PER_MINUTE": "37",
                "NVIDIA_RATE_LIMIT_MAX_WAIT_SECONDS": "12",
                "NVIDIA_FALLBACK_ON_RATE_LIMIT": "true",
                "GROQ_API_KEY": "groq-key",
                "CEREBRAS_API_KEY": "cerebras-key",
            },
            clear=True,
        ):
            config = LlmJudgeRuntimeConfig()

        providers = config.providers
        self.assertEqual([provider.name for provider in providers], ["nvidia", "groq", "cerebras"])
        self.assertEqual(providers[0].provider, "nvidia")
        self.assertEqual(providers[0].base_url, "https://integrate.api.nvidia.com/v1")
        self.assertEqual(providers[0].api_key, "nvidia-key")
        self.assertEqual(providers[0].model, "nvidia-model")
        self.assertEqual(providers[0].max_input_tokens, 12000)
        self.assertEqual(providers[0].requests_per_minute, 37)
        self.assertEqual(providers[0].rate_limit_max_wait_seconds, 12)
        self.assertTrue(providers[0].fallback_on_rate_limit)
        self.assertEqual(providers[1].api_key, "groq-key")
        self.assertEqual(providers[2].api_key, "cerebras-key")

    def test_match_llm_judge_runtime_defaults_nvidia_to_nemotron_ultra(self):
        with patch.dict(os.environ, {"NVIDIA_API_KEY": "nvidia-key"}, clear=True):
            config = LlmJudgeRuntimeConfig()

        nvidia = config.providers[0]
        self.assertEqual(nvidia.provider, "nvidia")
        self.assertEqual(nvidia.model, "nvidia/nemotron-3-ultra-550b-a55b")
        self.assertEqual(nvidia.max_input_tokens, 262144)
        self.assertEqual(nvidia.requests_per_minute, 40)
        self.assertEqual(nvidia.rate_limit_max_wait_seconds, 90)
        self.assertFalse(nvidia.fallback_on_rate_limit)

    def test_match_llm_judge_runtime_respects_explicit_nvidia_context_cap(self):
        with patch.dict(os.environ, {"NVIDIA_API_KEY": "nvidia-key"}, clear=True):
            provider = LlmJudgeProviderRuntimeConfig(
                name="nvidia",
                provider="nvidia",
                max_input_tokens=65536,
            )

        self.assertEqual(provider.max_input_tokens, 65536)

    def test_match_llm_judge_provider_entry_rejects_invalid_rate_limits(self):
        invalid_cases = [
            {"requests_per_minute": 0},
            {"rate_limit_max_wait_seconds": -1},
        ]

        for overrides in invalid_cases:
            with self.subTest(overrides=overrides):
                with patch.dict(os.environ, {}, clear=True):
                    with self.assertRaises(ValidationError):
                        LlmJudgeProviderRuntimeConfig(
                            name="nvidia",
                            provider="nvidia",
                            api_key="nvidia-key",
                            **overrides,
                        )

    def test_match_llm_judge_provider_entry_rejects_known_provider_base_url_mismatch(self):
        with self.assertRaises(ValidationError) as ctx:
            LlmJudgeProviderRuntimeConfig(
                name="groq",
                provider="groq",
                base_url="https://api.cerebras.ai/v1",
                api_key="groq-key",
                model="groq-model",
            )

        self.assertIn("base_url host must be api.groq.com", str(ctx.exception))

    def test_match_llm_judge_env_pairs_key_with_explicit_base_url_before_provider_alias(self):
        data = {
            "matching": {
                "llm_judge": {
                    "runtime": {
                        "provider": "cerebras",
                        "base_url": "https://api.groq.com/openai/v1",
                    }
                }
            }
        }
        env = {
            "GROQ_API_KEY": "groq-key",
            "CEREBRAS_API_KEY": "cerebras-key",
        }

        with patch.dict(os.environ, env, clear=True):
            updated = apply_env_overrides(data)

        runtime = updated["matching"]["llm_judge"]["runtime"]
        self.assertEqual(runtime["api_key"], "groq-key")

    def test_match_llm_judge_env_accepts_groq_provider(self):
        data = {"database": {"url": "test"}, "schedule": {"interval_seconds": 60}, "scrapers": []}
        env = {
            "MATCH_LLM_JUDGE_ENABLED": "true",
            "LLM_AS_A_JUDGE_PROVIDER": "groq",
            "GROQ_API_KEY": "groq-key",
            "LLM_AS_A_JUDGE_MODEL": "llama-3.1-8b-instant",
        }

        with patch("builtins.open", mock_open(read_data=yaml.dump(data))):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        self.assertTrue(config.matching.llm_judge.enabled)
        self.assertEqual(config.matching.llm_judge.runtime.provider, "groq")
        self.assertEqual(config.matching.llm_judge.runtime.api_key, "groq-key")
        self.assertEqual(config.matching.llm_judge.runtime.base_url, "https://api.groq.com/openai/v1")

    def test_match_llm_judge_env_accepts_cerebras_provider(self):
        data = {"database": {"url": "test"}, "schedule": {"interval_seconds": 60}, "scrapers": []}
        env = {
            "MATCH_LLM_JUDGE_ENABLED": "true",
            "LLM_AS_A_JUDGE_PROVIDER": "cerebras",
            "CEREBRAS_API_KEY": "cerebras-key",
        }

        with patch("builtins.open", mock_open(read_data=yaml.dump(data))):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        self.assertTrue(config.matching.llm_judge.enabled)
        self.assertEqual(config.matching.llm_judge.runtime.provider, "cerebras")
        self.assertEqual(config.matching.llm_judge.runtime.api_key, "cerebras-key")
        self.assertEqual(config.matching.llm_judge.runtime.base_url, "https://api.cerebras.ai/v1")
        self.assertEqual(config.matching.llm_judge.runtime.model, "gpt-oss-120b")
        self.assertEqual(config.matching.llm_judge.job_description_max_chars, 128000)
        self.assertEqual(config.matching.llm_judge.evidence_units_max_count, 200)
        self.assertEqual(config.matching.llm_judge.resume_summary_max_chars, 64000)

    def test_match_llm_judge_env_accepts_provider_chain_json(self):
        data = {"database": {"url": "test"}, "schedule": {"interval_seconds": 60}, "scrapers": []}
        providers = [
            {
                "name": "nvidia",
                "provider": "nvidia",
                "api_key_env": "NVIDIA_API_KEY",
                "model": "nvidia-model",
                "max_input_tokens": 16000,
            },
            {
                "name": "groq",
                "provider": "groq",
                "api_key_env": "GROQ_API_KEY",
                "model": "groq-model",
            },
        ]
        env = {
            "MATCH_LLM_JUDGE_ENABLED": "true",
            "LLM_AS_A_JUDGE_PROVIDERS_JSON": json.dumps(providers),
            "NVIDIA_API_KEY": "nvidia-key",
        }

        with patch("builtins.open", mock_open(read_data=yaml.dump(data))):
            with patch("os.path.exists", return_value=True):
                with patch.dict(os.environ, env, clear=True):
                    config = load_config("dummy")

        runtime = config.matching.llm_judge.runtime
        self.assertEqual(len(runtime.providers), 2)
        self.assertEqual(runtime.providers[0].api_key, "nvidia-key")
        self.assertEqual(runtime.providers[0].model, "nvidia-model")
        self.assertEqual(runtime.providers[0].max_input_tokens, 16000)
        self.assertEqual(runtime.providers[0].requests_per_minute, 40)

    def test_semantic_fit_raises_for_blank_llm_model(self):
        with self.assertRaises(ValidationError) as ctx:
            SemanticFitConfig(
                deploy_allowed_modes=["llm"],
                baseline_allowed_modes=["llm"],
                default_mode="llm",
                llm={
                    "enabled": True,
                    "base_url": "https://fit-llm.example/v1",
                    "model": "   ",
                },
            )

        self.assertIn("llm.model is required", str(ctx.exception))

    def test_resolve_config_path_prefers_existing_path(self):
        with patch("pathlib.Path.exists", side_effect=[True]):
            resolved = resolve_config_path("present.yaml", fallback_path="fallback.yaml")

        self.assertEqual(str(resolved), "present.yaml")

    def test_resolve_config_path_prefers_existing_fallback(self):
        with patch("pathlib.Path.exists", side_effect=[False, True]):
            resolved = resolve_config_path("missing.yaml", fallback_path="fallback.yaml")

        self.assertEqual(str(resolved), "fallback.yaml")

    def test_apply_env_overrides_applies_header_mappings(self):
        data = {"matching": {"scorer": {"semantic_fit": {"llm": {}}}}}
        env = {
            "FIT_LLM_HEADER_ENV_VARS": '{"Authorization":"FIT_API_TOKEN"}',
            "FIT_API_TOKEN": "Bearer token",
        }

        with patch.dict(os.environ, env, clear=False):
            updated = apply_env_overrides(data)

        self.assertEqual(
            updated["matching"]["scorer"]["semantic_fit"]["llm"]["headers"],
            {"Authorization": "Bearer token"},
        )

    def test_apply_env_overrides_applies_llm_judge_header_mapping(self):
        data = {"matching": {"llm_judge": {"runtime": {}}}}
        env = {
            "LLM_AS_A_JUDGE_HEADER_ENV_VARS": '{"X-API-Key":"JUDGE_TOKEN"}',
            "JUDGE_TOKEN": "secret-token",
        }

        with patch.dict(os.environ, env, clear=False):
            updated = apply_env_overrides(data)

        self.assertEqual(
            updated["matching"]["llm_judge"]["runtime"]["headers"],
            {"X-API-Key": "secret-token"},
        )

if __name__ == "__main__":
    unittest.main()
