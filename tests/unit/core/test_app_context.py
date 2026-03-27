"""Focused tests for AppContext AI provider wiring."""

from unittest.mock import patch

import pytest

from core.app_context import AppContext
from core.config_loader import LlmConfig
from core.llm.fake_service import FakeLLMService


def test_build_ai_service_uses_fake_provider_in_test_env(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_ENV", "test")
    monkeypatch.setenv("JOBSCOUT_FAKE_AI", "1")

    with patch("core.app_context.OpenAIService") as mock_openai:
        service = AppContext._build_ai_service(LlmConfig(embedding_dimensions=16))

    assert isinstance(service, FakeLLMService)
    assert service.embedding_dimensions == 16
    mock_openai.assert_not_called()


def test_build_ai_service_rejects_fake_provider_outside_dev_or_test(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_ENV", "production")
    monkeypatch.setenv("JOBSCOUT_FAKE_AI", "1")

    with pytest.raises(RuntimeError, match="JOBSCOUT_FAKE_AI is only allowed"):
        AppContext._build_ai_service(LlmConfig())


def test_fake_llm_service_zero_vector_normalizes_last_slot_for_custom_dimensions():
    service = FakeLLMService(embedding_dimensions=8)

    vector = service.generate_embedding("")

    assert len(vector) == 8
    assert vector[-1] == pytest.approx(1.0)
    assert sum(abs(value) for value in vector[:-1]) == pytest.approx(0.0)
