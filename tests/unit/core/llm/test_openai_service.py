"""
Unit tests for OpenAI service schema handling.

Tests verify:
- Schema unwrapping helper works correctly
- extract_structured_data sends proper JSON schema to LLM
- extract_job_facets sends proper JSON schema to LLM
- Guardrails catch invalid schemas
"""
import pytest
from unittest.mock import MagicMock
import json

from core.llm.openai_service import OpenAIService, _unwrap_schema_spec
from core.llm.schema_models import EXTRACTION_SCHEMA, FACET_EXTRACTION_SCHEMA_FOR_WANTS


class TestUnwrapSchemaSpec:
    """Tests for the schema unwrapping helper."""

    def test_wrapper_schema_returns_name_strict_and_inner_schema(self):
        """Wrapped schemas should return name, strict flag, and inner schema."""
        name, strict, raw_schema = _unwrap_schema_spec(EXTRACTION_SCHEMA)

        assert name == "job_extraction_schema"
        assert strict is True
        assert isinstance(raw_schema, dict)
        assert raw_schema.get("type") == "object"
        assert "properties" in raw_schema

    def test_raw_schema_passes_through_unchanged(self):
        """Raw JSON schemas should pass through with defaults."""
        raw = {"type": "object", "properties": {"foo": {"type": "string"}}}
        name, strict, result = _unwrap_schema_spec(raw)

        assert name == "extraction_response"
        assert strict is False
        assert result == raw

    def test_facet_schema_unwrapping(self):
        """FACET_EXTRACTION_SCHEMA_FOR_WANTS should unwrap correctly."""
        name, strict, raw_schema = _unwrap_schema_spec(FACET_EXTRACTION_SCHEMA_FOR_WANTS)

        assert name == "facet_extraction_schema"
        assert strict is True
        assert "properties" in raw_schema
        assert "remote_flexibility" in raw_schema["properties"]

    def test_wrapper_missing_strict_defaults_to_false(self):
        """Wrapper without strict key should default to False."""
        wrapped = {"name": "test", "schema": {"type": "object", "properties": {}}}
        name, strict, raw_schema = _unwrap_schema_spec(wrapped)

        assert name == "test"
        assert strict is False


class TestExtractStructuredData:
    """Tests for extract_structured_data method."""

    @pytest.fixture
    def service(self):
        """Create service with mocked client."""
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()

        mock_message = MagicMock()
        mock_message.content = json.dumps({"result": "test"})
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        svc.client.chat.completions.create.return_value = mock_response

        return svc

    def test_extract_with_wrapper_schema_sends_unwrapped_json_schema(self, service):
        """Wrapped schema should result in proper JSON schema sent to LLM."""
        service.extract_structured_data("test text", EXTRACTION_SCHEMA)

        call_kwargs = service.client.chat.completions.create.call_args[1]
        json_schema = call_kwargs['response_format']['json_schema']

        assert json_schema['schema'].get("type") == "object"
        assert "properties" in json_schema['schema']
        assert "name" not in json_schema['schema']
        assert "strict" not in json_schema['schema']

    def test_extract_with_raw_schema_sends_schema_directly(self, service):
        """Raw schema should be sent as-is."""
        raw_schema = {"type": "object", "properties": {"foo": {"type": "string"}}}
        service.extract_structured_data("test text", raw_schema)

        call_kwargs = service.client.chat.completions.create.call_args[1]
        json_schema = call_kwargs['response_format']['json_schema']

        assert json_schema['schema'] == raw_schema

    def test_extract_preserves_strict_setting_from_wrapper(self, service):
        """Strict=True from wrapper should be passed through."""
        service.extract_structured_data("test text", EXTRACTION_SCHEMA)

        call_kwargs = service.client.chat.completions.create.call_args[1]
        assert call_kwargs['response_format']['json_schema']['strict'] is True

    def test_extract_uses_name_from_wrapper(self, service):
        """Name from wrapper should be used in json_schema."""
        service.extract_structured_data("test text", EXTRACTION_SCHEMA)

        call_kwargs = service.client.chat.completions.create.call_args[1]
        assert call_kwargs['response_format']['json_schema']['name'] == "job_extraction_schema"

    def test_extract_raises_on_invalid_schema(self, service):
        """Invalid schema should raise ValueError with helpful message."""
        invalid = {"not": "a valid json schema"}

        with pytest.raises(ValueError, match="Not a valid JSON Schema object"):
            service.extract_structured_data("test text", invalid)


class TestExtractJobFacets:
    """Tests for extract_facet_data method."""

    @pytest.fixture
    def service(self):
        """Create service with mocked client."""
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()

        mock_message = MagicMock()
        mock_message.content = json.dumps({"remote_flexibility": "remote"})
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        svc.client.chat.completions.create.return_value = mock_response

        return svc

    def test_facets_extraction_sends_unwrapped_schema(self, service):
        """Facet extraction should use unwrapped schema, not wrapper."""
        service.extract_facet_data("test text")

        call_kwargs = service.client.chat.completions.create.call_args[1]
        json_schema = call_kwargs['response_format']['json_schema']

        assert json_schema['schema'].get("type") == "object"
        assert "properties" in json_schema['schema']
        assert "name" not in json_schema['schema']

    def test_facets_preserves_strict_setting(self, service):
        """Strict setting should be preserved from FACET_EXTRACTION_SCHEMA_FOR_WANTS."""
        service.extract_facet_data("test text")

        call_kwargs = service.client.chat.completions.create.call_args[1]
        assert call_kwargs['response_format']['json_schema']['strict'] is True
