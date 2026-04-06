"""Tests for core/llm/interfaces.py — LLMProvider abstract class."""

import pytest
from core.llm.interfaces import LLMProvider


class ConcreteProvider(LLMProvider):
    """Concrete implementation that delegates to super() to cover abstract method bodies."""

    def extract_structured_data(self, text, schema_spec, system_prompt=None, user_message=None):
        return super().extract_structured_data(text, schema_spec, system_prompt, user_message)

    def extract_resume_data(self, text):
        return super().extract_resume_data(text)

    def extract_requirements_data(self, text):
        return super().extract_requirements_data(text)

    def generate_embedding(self, text):
        return super().generate_embedding(text)


class RealProvider(LLMProvider):
    """Provider with a real generate_embedding for testing batch default."""

    def extract_structured_data(self, text, schema_spec, system_prompt=None, user_message=None):
        return {}

    def extract_resume_data(self, text):
        return {}

    def extract_requirements_data(self, text):
        return {}

    def generate_embedding(self, text):
        return [0.1, 0.2, 0.3]


class TestLLMProviderAbstractMethods:
    def test_extract_structured_data_pass_returns_none(self):
        provider = ConcreteProvider()
        result = provider.extract_structured_data("text", {})
        assert result is None

    def test_extract_resume_data_pass_returns_none(self):
        provider = ConcreteProvider()
        result = provider.extract_resume_data("resume text")
        assert result is None

    def test_extract_requirements_data_pass_returns_none(self):
        provider = ConcreteProvider()
        result = provider.extract_requirements_data("job description")
        assert result is None

    def test_generate_embedding_pass_returns_none(self):
        provider = ConcreteProvider()
        result = provider.generate_embedding("text")
        assert result is None

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            LLMProvider()


class TestGenerateEmbeddingsBatchDefault:
    def test_calls_generate_embedding_for_each_text(self):
        provider = RealProvider()
        result = provider.generate_embeddings_batch(["text1", "text2", "text3"])
        assert len(result) == 3
        assert result[0] == [0.1, 0.2, 0.3]
        assert result[1] == [0.1, 0.2, 0.3]
        assert result[2] == [0.1, 0.2, 0.3]

    def test_empty_list_returns_empty(self):
        provider = RealProvider()
        result = provider.generate_embeddings_batch([])
        assert result == []

    def test_single_text(self):
        provider = RealProvider()
        result = provider.generate_embeddings_batch(["only text"])
        assert result == [[0.1, 0.2, 0.3]]
