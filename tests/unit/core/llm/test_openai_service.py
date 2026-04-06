"""
Unit tests for OpenAI service schema handling.

Tests verify:
- Schema unwrapping helper works correctly
- extract_structured_data sends proper JSON schema to LLM
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
from core.llm.schema_models import EXTRACTION_SCHEMA


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

    def test_wrapper_missing_strict_defaults_to_false(self):
        """Wrapper without strict key should default to False."""
        wrapped = {"name": "test", "schema": {"type": "object", "properties": {}}}
        name, strict, _ = _unwrap_schema_spec(wrapped)

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

    def test_generate_embedding_rejects_empty_vector(self, service):
        """Should reject empty embedding vectors."""
        service.embedding_client.embeddings.create.return_value.data[0].embedding = []

        with pytest.raises(ValueError, match="empty or non-list vector"):
            service.generate_embedding("test")

    def test_generate_embedding_rejects_non_finite_values(self, service):
        """Should reject non-finite embedding vectors."""
        service.embedding_client.embeddings.create.return_value.data[0].embedding = [0.1, float("nan")]

        with pytest.raises(ValueError, match="non-finite"):
            service.generate_embedding("test")


class TestValidateEmbeddingVector:
    """Tests for _validate_embedding_vector helper."""

    def test_valid_vector_passes_through(self):
        from core.llm.openai_service import _validate_embedding_vector
        result = _validate_embedding_vector([0.1, 0.2, 0.3])
        assert result == [0.1, 0.2, 0.3]

    def test_integer_values_coerced_to_float(self):
        from core.llm.openai_service import _validate_embedding_vector
        result = _validate_embedding_vector([1, 2, 3])
        assert all(isinstance(v, float) for v in result)

    def test_empty_list_raises(self):
        from core.llm.openai_service import _validate_embedding_vector
        with pytest.raises(ValueError, match="empty or non-list"):
            _validate_embedding_vector([])

    def test_non_list_raises(self):
        from core.llm.openai_service import _validate_embedding_vector
        with pytest.raises(ValueError, match="empty or non-list"):
            _validate_embedding_vector("not a list")

    def test_inf_value_raises(self):
        from core.llm.openai_service import _validate_embedding_vector
        with pytest.raises(ValueError, match="non-finite"):
            _validate_embedding_vector([0.1, float("inf")])

    def test_nan_value_raises(self):
        from core.llm.openai_service import _validate_embedding_vector
        with pytest.raises(ValueError, match="non-finite"):
            _validate_embedding_vector([0.1, float("nan")])

    def test_string_element_raises(self):
        from core.llm.openai_service import _validate_embedding_vector
        with pytest.raises(ValueError):
            _validate_embedding_vector([0.1, "bad"])


class TestLogRetry:
    """Tests for _log_retry callback."""

    def test_logs_rate_limit_warning(self, caplog):
        import logging
        from core.llm.openai_service import _log_retry
        retry_state = MagicMock()
        exc = MagicMock(spec=openai.RateLimitError)
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 2
        retry_state.next_action.sleep = 5.0

        with caplog.at_level(logging.WARNING, logger="core.llm.openai_service"):
            _log_retry(retry_state)

        assert any("Rate limit" in r.message for r in caplog.records)

    def test_logs_generic_error_warning(self, caplog):
        import logging
        from core.llm.openai_service import _log_retry
        retry_state = MagicMock()
        exc = MagicMock(spec=openai.APITimeoutError)
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 1
        retry_state.next_action.sleep = 2.0

        with caplog.at_level(logging.WARNING, logger="core.llm.openai_service"):
            _log_retry(retry_state)

        assert any("Transient" in r.message for r in caplog.records)

    def test_handles_none_next_action(self, caplog):
        import logging
        from core.llm.openai_service import _log_retry
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = MagicMock(spec=openai.RateLimitError)
        retry_state.attempt_number = 1
        retry_state.next_action = None

        # Should not raise even when next_action is None
        with caplog.at_level(logging.WARNING, logger="core.llm.openai_service"):
            _log_retry(retry_state)


class TestWaitRespectingRetryAfterFallback:
    """Tests for _wait_respecting_retry_after fallback branch."""

    def test_uses_exponential_backoff_for_non_rate_limit_error(self):
        """Non-RateLimitError should use exponential backoff."""
        exc = MagicMock(spec=openai.APITimeoutError)
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 1
        retry_state.start_time = 0.0

        wait = _wait_respecting_retry_after(retry_state)
        assert wait >= 2.0  # min is 2s

    def test_uses_exponential_backoff_when_rate_limit_headers_are_zero(self):
        """RateLimitError with wait=0 from headers falls back to exponential backoff."""
        from unittest.mock import PropertyMock
        exc = MagicMock(spec=openai.RateLimitError)
        type(exc).response = PropertyMock(return_value=MagicMock(headers={}))
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = exc
        retry_state.attempt_number = 1
        retry_state.start_time = 0.0

        wait = _wait_respecting_retry_after(retry_state)
        assert wait >= 2.0  # falls back to exp backoff min


class TestWaitFromRateLimitHeadersEdgeCases:
    """Edge cases for _wait_from_rate_limit_headers."""

    def test_returns_zero_when_response_raises(self):
        """Should return 0.0 when accessing response headers raises."""
        exc = MagicMock()
        exc.response = None  # accessing .headers will fail
        # AttributeError when accessing None.headers
        result = _wait_from_rate_limit_headers(exc)
        assert result == 0.0

    def test_ignores_non_numeric_retry_after(self):
        """Non-numeric retry-after should be skipped."""
        exc = MagicMock()
        exc.response.headers = {"retry-after": "not-a-number"}
        result = _wait_from_rate_limit_headers(exc)
        assert result == 0.0

    def test_uses_max_of_requests_and_tokens_headers(self):
        """Should return max of request and token reset headers."""
        exc = MagicMock()
        exc.response.headers = {
            "x-ratelimit-reset-requests": "10s",
            "x-ratelimit-reset-tokens": "45s",
        }
        assert _wait_from_rate_limit_headers(exc) == 45.0


class TestOpenAIServiceConstructor:
    """Tests for OpenAIService __init__ with various configurations."""

    def test_default_construction_with_api_key(self):
        svc = OpenAIService(api_key="sk-test")
        assert svc.embedding_client is None
        assert svc.extraction_model == "qwen3:14b"

    def test_extraction_headers_create_httpx_client(self):
        """Constructor should create httpx client when extraction_headers provided."""
        svc = OpenAIService(api_key="test", extraction_headers={"X-Custom": "value"})
        # client._client is the httpx client
        assert svc.client is not None

    def test_separate_embedding_client_created_for_different_url(self):
        """Separate embedding client should be created when embedding_base_url differs."""
        svc = OpenAIService(
            api_key="test",
            embedding_base_url="http://embed-server/v1",
            embedding_api_key="embed-key",
        )
        assert svc.embedding_client is not None

    def test_embedding_headers_create_httpx_client(self):
        """Embedding httpx client should be created when embedding_headers provided."""
        svc = OpenAIService(
            api_key="test",
            embedding_base_url="http://embed/v1",
            embedding_api_key="embed-key",
            embedding_headers={"Authorization": "Bearer embed-token"},
        )
        assert svc.embedding_client is not None

    def test_model_config_overrides_defaults(self):
        svc = OpenAIService(
            api_key="test",
            model_config={
                "extraction_model": "gpt-4o",
                "embedding_model": "text-embedding-3-large",
                "embedding_dimensions": 3072,
                "extraction_temperature": 0.7,
            }
        )
        assert svc.extraction_model == "gpt-4o"
        assert svc.embedding_model == "text-embedding-3-large"
        assert svc.embedding_dimensions == 3072
        assert svc.extraction_temperature == 0.7


class TestGenerateEmbeddingsBatch:
    """Tests for generate_embeddings_batch."""

    @pytest.fixture
    def service(self):
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()
        return svc

    def _make_response(self, vectors):
        embeddings = []
        for v in vectors:
            emb = MagicMock()
            emb.embedding = v
            embeddings.append(emb)
        resp = MagicMock()
        resp.data = embeddings
        return resp

    def test_empty_list_returns_empty(self, service):
        result = service.generate_embeddings_batch([])
        assert result == []
        service.client.embeddings.create.assert_not_called()

    def test_single_batch_returns_all_embeddings(self, service):
        vectors = [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
        service.client.embeddings.create.return_value = self._make_response(vectors)

        result = service.generate_embeddings_batch(["a", "b", "c"])
        assert result == vectors

    def test_uses_embedding_client_when_available(self, service):
        service.embedding_client = MagicMock()
        vectors = [[0.1, 0.2]]
        service.embedding_client.embeddings.create.return_value = self._make_response(vectors)

        result = service.generate_embeddings_batch(["text"])
        assert result == vectors
        service.client.embeddings.create.assert_not_called()

    def test_multiple_batches_when_input_exceeds_batch_size(self, service):
        """Inputs > 32 should be split into multiple API calls."""
        n = 35
        texts = [f"text {i}" for i in range(n)]
        # First batch of 32, second batch of 3
        first_vecs = [[float(i)] for i in range(32)]
        second_vecs = [[float(i + 32)] for i in range(3)]
        service.client.embeddings.create.side_effect = [
            self._make_response(first_vecs),
            self._make_response(second_vecs),
        ]

        result = service.generate_embeddings_batch(texts)
        assert len(result) == n
        assert service.client.embeddings.create.call_count == 2

    def test_preserves_embedding_order(self, service):
        """Embeddings should be returned in the same order as input texts."""
        vectors = [[1.0], [2.0], [3.0]]
        service.client.embeddings.create.return_value = self._make_response(vectors)

        result = service.generate_embeddings_batch(["a", "b", "c"])
        assert result[0] == [1.0]
        assert result[2] == [3.0]


class TestUnloadModel:
    """Tests for unload_model method."""

    def test_sends_request_for_localhost_url(self):
        """Should POST to Ollama API when base_url contains localhost."""
        svc = OpenAIService(api_key="test", base_url="http://localhost:11434/v1")
        with MagicMock():
            import unittest.mock
            with unittest.mock.patch("core.llm.openai_service.requests.post") as mock_post:
                svc.unload_model("llama3")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "api/generate" in call_args[0][0]
        assert call_args[1]["json"]["model"] == "llama3"
        assert call_args[1]["json"]["keep_alive"] == 0

    def test_sends_request_for_127_0_0_1_url(self):
        """Should POST to Ollama API when base_url contains 127.0.0.1."""
        svc = OpenAIService(api_key="test", base_url="http://127.0.0.1:11434/v1")
        with MagicMock():
            import unittest.mock
            with unittest.mock.patch("core.llm.openai_service.requests.post") as mock_post:
                svc.unload_model("mymodel")
        mock_post.assert_called_once()

    def test_sends_request_for_host_docker_internal_url(self):
        """Should POST to Ollama API when base_url contains host.docker.internal."""
        svc = OpenAIService(api_key="test", base_url="http://host.docker.internal:11434/v1")
        import unittest.mock
        with unittest.mock.patch("core.llm.openai_service.requests.post") as mock_post:
            svc.unload_model("mymodel")
        mock_post.assert_called_once()

    def test_no_request_for_remote_url(self):
        """Should not POST when base_url is a remote server."""
        svc = OpenAIService(api_key="sk-test", base_url="https://api.openai.com/v1")
        import unittest.mock
        with unittest.mock.patch("core.llm.openai_service.requests.post") as mock_post:
            svc.unload_model("gpt-4")
        mock_post.assert_not_called()

    def test_exception_during_request_is_swallowed(self):
        """Exceptions from requests.post should be caught and logged."""
        svc = OpenAIService(api_key="test", base_url="http://localhost:11434/v1")
        import unittest.mock
        with unittest.mock.patch(
            "core.llm.openai_service.requests.post",
            side_effect=ConnectionError("refused")
        ):
            # Should not raise
            svc.unload_model("llama3")


class TestExtractStructuredDataEdgeCases:
    """Edge cases for extract_structured_data."""

    @pytest.fixture
    def service(self):
        svc = OpenAIService(api_key="test")
        svc.client = MagicMock()
        return svc

    def _mock_response(self, service, content_str: str):
        mock_message = MagicMock()
        mock_message.content = content_str
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        service.client.chat.completions.create.return_value = mock_response

    def test_custom_system_prompt_and_user_message(self, service):
        """Custom prompts should be passed to the LLM."""
        self._mock_response(service, '{"key": "value"}')
        schema = {"type": "object", "properties": {"key": {"type": "string"}}}
        service.extract_structured_data(
            "text",
            schema,
            system_prompt="Custom system",
            user_message="Custom user message"
        )
        call_kwargs = service.client.chat.completions.create.call_args[1]
        messages = call_kwargs["messages"]
        assert messages[0]["content"] == "Custom system"
        assert messages[1]["content"] == "Custom user message"

    def test_raises_on_json_decode_error(self, service):
        """Should raise json.JSONDecodeError when LLM returns non-JSON."""
        self._mock_response(service, "this is not json at all {{{")
        schema = {"type": "object", "properties": {"key": {"type": "string"}}}
        with pytest.raises(Exception):
            service.extract_structured_data("text", schema)
