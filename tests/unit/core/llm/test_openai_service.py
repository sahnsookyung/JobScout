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

import openai

from core.llm.openai_service import (
    OpenAIService,
    _unwrap_schema_spec,
    _is_retryable,
    _parse_reset_duration,
    _wait_from_rate_limit_headers,
    _wait_respecting_retry_after,
)
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


class TestRetryHelpers:
    """Tests for retry logic helpers."""

    def test_is_retryable_with_rate_limit_error(self):
        """RateLimitError should be retryable."""
        from unittest.mock import MagicMock
        exc = MagicMock(spec=openai.RateLimitError)
        assert _is_retryable(exc) is True

    def test_is_retryable_with_timeout_error(self):
        """APITimeoutError should be retryable."""
        from unittest.mock import MagicMock
        exc = MagicMock(spec=openai.APITimeoutError)
        assert _is_retryable(exc) is True

    def test_is_retryable_with_connection_error(self):
        """APIConnectionError should be retryable."""
        from unittest.mock import MagicMock
        exc = MagicMock(spec=openai.APIConnectionError)
        assert _is_retryable(exc) is True

    def test_is_retryable_with_internal_server_error(self):
        """InternalServerError should be retryable."""
        from unittest.mock import MagicMock
        exc = MagicMock(spec=openai.InternalServerError)
        assert _is_retryable(exc) is True

    def test_is_retryable_with_non_retryable_error(self):
        """Non-retryable errors should return False."""
        from unittest.mock import MagicMock
        exc = MagicMock(spec=openai.AuthenticationError)
        assert _is_retryable(exc) is False


class TestParseResetDuration:
    """Tests for _parse_reset_duration helper."""

    def test_parse_milliseconds(self):
        """Should parse ms correctly."""
        assert _parse_reset_duration("500ms") == 0.5

    def test_parse_seconds(self):
        """Should parse seconds correctly."""
        assert _parse_reset_duration("30s") == 30.0

    def test_parse_minutes(self):
        """Should parse minutes correctly."""
        assert _parse_reset_duration("2m") == 120.0

    def test_parse_hours(self):
        """Should parse hours correctly."""
        assert _parse_reset_duration("1h") == 3600.0

    def test_parse_compound(self):
        """Should parse compound duration like 1m30s."""
        assert _parse_reset_duration("1m30s") == 90.0

    def test_parse_decimal(self):
        """Should parse decimal values."""
        assert _parse_reset_duration("1.5s") == 1.5

    def test_parse_empty_string(self):
        """Empty string should return 0.0."""
        assert _parse_reset_duration("") == 0.0


class TestWaitFromRateLimitHeaders:
    """Tests for _wait_from_rate_limit_headers."""

    def test_returns_retry_after_seconds(self):
        """Should use retry-after header when present."""
        from unittest.mock import MagicMock
        exc = MagicMock()
        exc.response.headers = {"retry-after": "30"}
        assert _wait_from_rate_limit_headers(exc) == 30.0

    def test_returns_max_of_multiple_headers(self):
        """Should return maximum when multiple headers present."""
        from unittest.mock import MagicMock
        exc = MagicMock()
        exc.response.headers = {
            "retry-after": "10",
            "x-ratelimit-reset-tokens": "30s"
        }
        assert _wait_from_rate_limit_headers(exc) == 30.0

    def test_returns_zero_when_no_headers(self):
        """Should return 0.0 when no rate limit headers."""
        from unittest.mock import MagicMock
        exc = MagicMock()
        exc.response.headers = {}
        assert _wait_from_rate_limit_headers(exc) == 0.0


class TestWaitRespectingRetryAfter:
    """Tests for _wait_respecting_retry_after"""

    def test_uses_headers_for_rate_limit(self):
        """Should use headers for RateLimitError when available."""
        from unittest.mock import MagicMock, PropertyMock
        exc = MagicMock(spec=openai.RateLimitError)
        type(exc).response = PropertyMock(return_value=MagicMock(headers={"retry-after": "45"}))

        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 1
        retry_state.next_action = None

        wait = _wait_respecting_retry_after(retry_state)
        assert wait == 45.0

    def test_caps_at_120_seconds(self):
        """Should cap wait time at 120 seconds."""
        from unittest.mock import MagicMock, PropertyMock
        exc = MagicMock(spec=openai.RateLimitError)
        type(exc).response = PropertyMock(return_value=MagicMock(headers={"retry-after": "500"}))

        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 1
        retry_state.next_action = None

        wait = _wait_respecting_retry_after(retry_state)
        assert wait == 120.0


class TestExtractResumeData:
    """Tests for extract_resume_data method."""

    @pytest.fixture
    def service(self):
        """Create service with mocked client."""
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()

        mock_message = MagicMock()
        mock_message.content = json.dumps({
            "profile": {"name": "John", "experience": [{"title": "Dev"}]},
            "thought_process": "reasoning"
        })
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        svc.client.chat.completions.create.return_value = mock_response

        return svc

    def test_extract_resume_data_returns_profile(self, service):
        """Should return extracted resume data."""
        result = service.extract_resume_data("My experience includes...")
        assert "profile" in result
        assert result["profile"]["name"] == "John"

    def test_extract_resume_data_uses_resume_schema(self, service):
        """Should use RESUME_SCHEMA for extraction."""
        service.extract_resume_data("test resume")
        call_kwargs = service.client.chat.completions.create.call_args[1]
        assert "response_format" in call_kwargs


class TestExtractRequirementsData:
    """Tests for extract_requirements_data method."""

    @pytest.fixture
    def service(self):
        """Create service with mocked client."""
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()

        mock_message = MagicMock()
        mock_message.content = json.dumps({
            "required": ["Python"],
            "thought_process": "reasoning"
        })
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        svc.client.chat.completions.create.return_value = mock_response

        return svc

    def test_extract_requirements_data_returns_data(self, service):
        """Should return extracted requirements."""
        result = service.extract_requirements_data("Job requires Python")
        assert "required" in result


class TestGenerateEmbedding:
    """Tests for generate_embedding method."""

    @pytest.fixture
    def service(self):
        """Create service with mocked embedding client."""
        svc = OpenAIService(api_key="test")
        svc.embedding_client = MagicMock()

        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1, 0.2, 0.3]
        mock_response = MagicMock()
        mock_response.data = [mock_embedding]
        svc.embedding_client.embeddings.create.return_value = mock_response

        return svc

    def test_generate_embedding_returns_vector(self, service):
        """Should return embedding vector."""
        result = service.generate_embedding("test text")
        assert result == [0.1, 0.2, 0.3]

    def test_generate_embedding_uses_correct_model(self, service):
        """Should use configured embedding model."""
        service.generate_embedding("test")
        call_kwargs = service.embedding_client.embeddings.create.call_args[1]
        assert call_kwargs["model"] == "qwen3-embedding:4b"

    def test_generate_embedding_falls_back_to_main_client(self):
        """Should use main client when no separate embedding client."""
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()

        mock_embedding = MagicMock()
        mock_embedding.embedding = [0.1, 0.2]
        mock_response = MagicMock()
        mock_response.data = [mock_embedding]
        svc.client.embeddings.create.return_value = mock_response

        result = svc.generate_embedding("test")
        assert result == [0.1, 0.2]
