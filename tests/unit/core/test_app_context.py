"""Focused tests for AppContext AI provider wiring."""

from unittest.mock import patch

import pytest

from core.app_context import AppContext
from core.config_loader import LlmConfig
from core.llm.fake_service import FakeLLMService


def test_build_ai_service_uses_shared_provider_factory():
    sentinel = FakeLLMService(embedding_dimensions=16)

    with patch("core.app_context.build_llm_provider", return_value=sentinel) as mock_build:
        service = AppContext._build_ai_service(LlmConfig(embedding_dimensions=16))

    assert service is sentinel
    mock_build.assert_called_once()


def test_build_ai_service_rejects_legacy_fake_env(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_FAKE_AI", "1")

    with pytest.raises(RuntimeError, match="JOBSCOUT_FAKE_AI has been removed"):
        AppContext._build_ai_service(LlmConfig())


def test_fake_llm_service_zero_vector_normalizes_last_slot_for_custom_dimensions():
    service = FakeLLMService(embedding_dimensions=8)

    vector = service.generate_embedding("")

    assert len(vector) == 8
    assert vector[-1] == pytest.approx(1.0)
    assert sum(abs(value) for value in vector[:-1]) == pytest.approx(0.0)
