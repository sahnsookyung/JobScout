"""
OpenAI Service - LLM implementation using OpenAI API.

Provides structured data extraction and embedding generation.
"""

import os
from typing import Dict, Any, List, Optional, Tuple
import json
import logging
import copy
import math
import re
import requests
import httpx

import openai
from openai import OpenAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity import RetryCallState
from core.llm.interfaces import LLMProvider
from core.llm.system_prompts import (
    DEFAULT_EXTRACTION_SYSTEM_PROMPT,
    RESUME_EXTRACTION_SYSTEM_PROMPT,
    REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT,
)
from core.llm.schema_models import (
    RESUME_SCHEMA,
    EXTRACTION_SCHEMA,
)

logger = logging.getLogger(__name__)

LLM_RETRY_MAX_ATTEMPTS = max(1, int(os.getenv("LLM_RETRY_MAX_ATTEMPTS", "4")))
LLM_RETRY_EXPONENTIAL_MAX_SECONDS = max(
    2,
    float(os.getenv("LLM_RETRY_EXPONENTIAL_MAX_SECONDS", "8")),
)
LLM_RATE_LIMIT_WAIT_CAP_SECONDS = max(
    1,
    float(os.getenv("LLM_RATE_LIMIT_WAIT_CAP_SECONDS", "30")),
)
OPENAI_CLIENT_MAX_RETRIES = max(0, int(os.getenv("OPENAI_CLIENT_MAX_RETRIES", "0")))


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient errors that are worth retrying."""
    return isinstance(
        exc,
        (
            openai.RateLimitError,
            openai.APITimeoutError,
            openai.APIConnectionError,
            openai.InternalServerError,
        ),
    )


def _log_retry(retry_state: RetryCallState) -> None:
    """Log a warning before each retry sleep, including Retry-After info."""
    exc = retry_state.outcome.exception()
    wait = retry_state.next_action.sleep if retry_state.next_action else 0
    if isinstance(exc, openai.RateLimitError):
        logger.warning(
            "Rate limit hit (attempt %s). Waiting %.1fs before retry. Details: %s",
            retry_state.attempt_number,
            wait,
            exc,
        )
    else:
        logger.warning(
            "Transient API error (attempt %s). Waiting %.1fs before retry. Details: %s",
            retry_state.attempt_number,
            wait,
            exc,
        )


def _parse_reset_duration(value: str) -> float:
    """Parse an OpenAI reset-timer header value like '1s', '500ms', '1m30s' into seconds."""
    total = 0.0
    # More specific regex to avoid catastrophic backtracking (S5852)
    for amount, unit in re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", value):  # NOSONAR
        a = float(amount)
        if unit == "ms":
            total += a / 1000
        elif unit == "s":
            total += a
        elif unit == "m":
            total += a * 60
        else:  # h
            total += a * 3600
    return total


def _wait_from_rate_limit_headers(exc: openai.RateLimitError) -> float:
    """Extract the longest declared wait from rate-limit response headers.

    Reads (in priority order, taking the maximum):
      - ``retry-after``                standard HTTP, plain seconds
      - ``x-ratelimit-reset-requests`` OpenAI request-quota reset duration
      - ``x-ratelimit-reset-tokens``   OpenAI token-quota reset duration

    Returns 0.0 if no usable header is present.
    """
    try:
        headers = exc.response.headers
        candidates: list[float] = []

        retry_after = headers.get("retry-after", "")
        if retry_after:
            try:
                candidates.append(float(retry_after))
            except ValueError:
                pass  # ignore non-numeric retry-after header values

        for header in ("x-ratelimit-reset-requests", "x-ratelimit-reset-tokens"):
            parsed = _parse_reset_duration(headers.get(header, ""))
            if parsed > 0:
                candidates.append(parsed)

        return max(candidates) if candidates else 0.0
    except Exception:
        return 0.0


def _wait_respecting_retry_after(retry_state: RetryCallState) -> float:
    """Return how long tenacity should sleep before the next attempt.

    For ``RateLimitError``: honours server-declared timers via response headers.
    For all other retryable errors: falls back to capped exponential backoff.
    """
    exc = retry_state.outcome.exception()
    if isinstance(exc, openai.RateLimitError):
        wait = _wait_from_rate_limit_headers(exc)
        if wait > 0:
            wait = min(wait, LLM_RATE_LIMIT_WAIT_CAP_SECONDS)
            logger.info("Rate limit headers indicate %.1fs wait.", wait)
            return wait

    # Fallback: exponential backoff 2 -> 4 -> 8 ... capped for interactive flows.
    exp = wait_exponential(multiplier=1, min=2, max=LLM_RETRY_EXPONENTIAL_MAX_SECONDS)
    return exp(retry_state)


def _llm_retry(**kwargs):
    """Return a tenacity @retry decorator for LLM API calls."""
    return retry(
        retry=retry_if_exception_type(
            (
                openai.RateLimitError,
                openai.APITimeoutError,
                openai.APIConnectionError,
                openai.InternalServerError,
            )
        ),
        wait=_wait_respecting_retry_after,
        stop=stop_after_attempt(LLM_RETRY_MAX_ATTEMPTS),
        before_sleep=_log_retry,
        reraise=True,
        **kwargs,
    )


def _unwrap_schema_spec(spec: Dict[str, Any]) -> Tuple[str, bool, Dict[str, Any]]:
    """Unwrap a schema spec to extract name, strict flag, and raw JSON schema.

    Args:
        spec: Either a wrapped spec {'name': str, 'strict': bool, 'schema': {...}}
              or a raw JSON schema dict

    Returns:
        Tuple of (name, strict, raw_schema)
    """
    if isinstance(spec, dict) and "schema" in spec and "name" in spec:
        return (
            spec.get("name", "extraction_response"),
            bool(spec.get("strict", False)),
            spec["schema"],
        )
    return "extraction_response", False, spec


def _validate_embedding_vector(vector: Any) -> List[float]:
    """Ensure embedding responses are non-empty finite numeric vectors."""
    if not isinstance(vector, list) or not vector:
        raise ValueError("Embedding API returned an empty or non-list vector")

    validated: List[float] = []
    for value in vector:
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError("Embedding API returned a non-finite vector value")
        validated.append(float(value))

    return validated


def _structured_output_rejected(exc: BaseException) -> bool:
    """Return True when an OpenAI-compatible backend rejects JSON Schema mode."""
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "response_format",
            "json_schema",
            "structured output",
            "structured_outputs",
            "schema",
        )
    )


def _structured_json_object_validation_failed(exc: BaseException) -> bool:
    """Return True when provider-enforced JSON object mode rejects generated output."""
    message = str(exc).lower()
    return "json_validate_failed" in message or "failed to validate json" in message


def _extract_fenced_json_candidate(stripped: str) -> str:
    """Return the content inside a Markdown JSON fence, if one wraps the payload."""
    fence_start = stripped.find("```")
    if fence_start == -1:
        return stripped

    header_start = fence_start + 3
    header_end = stripped.find("\n", header_start)
    if header_end == -1:
        return stripped

    fence_label = stripped[header_start:header_end].strip().lower()
    if fence_label not in ("", "json"):
        return stripped

    fence_end = stripped.find("```", header_end + 1)
    if fence_end == -1:
        return stripped

    return stripped[header_end + 1 : fence_end].strip()


def _parse_structured_json_content(content: Any) -> Dict[str, Any]:
    """Parse a JSON object from an LLM response, tolerating common fenced forms."""
    if not isinstance(content, str):
        raise ValueError("Structured data response content is not text")

    stripped = content.strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        candidate = _extract_fenced_json_candidate(stripped)
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(candidate[start : end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("Structured data response did not contain a JSON object")
    return parsed


class OpenAIService(LLMProvider):
    """
    OpenAI LLM Service.

    Provides structured data extraction using JSON Schema mode
    and embedding generation.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        model_config: Optional[Dict[str, Any]] = None,
        extraction_headers: Optional[Dict[str, str]] = None,
        embedding_api_key: Optional[str] = None,
        embedding_api_secret: Optional[str] = None,
        embedding_base_url: Optional[str] = None,
        embedding_headers: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        structured_output_mode: Optional[str] = None,
        max_output_tokens: Optional[int] = None,
    ):
        # Build extraction client
        client_kwargs = {"max_retries": OPENAI_CLIENT_MAX_RETRIES}
        if timeout_seconds:
            client_kwargs["timeout"] = timeout_seconds
        if api_key:
            client_kwargs["api_key"] = api_key
        if base_url:
            client_kwargs["base_url"] = base_url

        # If custom headers provided, create httpx client with headers
        if extraction_headers:
            http_client = httpx.Client(headers=extraction_headers)
            client_kwargs["http_client"] = http_client

        self.client = OpenAI(**client_kwargs)

        # Build embedding client (separate if different endpoint or headers)
        if embedding_base_url or embedding_api_key or embedding_api_secret or embedding_headers:
            embedding_client_kwargs = {"max_retries": OPENAI_CLIENT_MAX_RETRIES}
            if timeout_seconds:
                embedding_client_kwargs["timeout"] = timeout_seconds
            if embedding_api_key:
                embedding_client_kwargs["api_key"] = embedding_api_key
            if embedding_base_url:
                embedding_client_kwargs["base_url"] = embedding_base_url

            # If custom headers provided for embedding, create httpx client with headers
            if embedding_headers:
                http_client = httpx.Client(headers=embedding_headers)
                embedding_client_kwargs["http_client"] = http_client

            self.embedding_client = OpenAI(**embedding_client_kwargs)
        else:
            self.embedding_client = None

        self.model_config = model_config or {}
        self.extraction_model = self.model_config.get("extraction_model", "qwen3:14b")
        self.embedding_model = self.model_config.get("embedding_model", "qwen3-embedding:4b")
        self.embedding_dimensions = self.model_config.get("embedding_dimensions", 1024)
        self.extraction_temperature = self.model_config.get("extraction_temperature", 0.0)
        self.structured_output_mode = structured_output_mode or "json_schema"
        self.max_output_tokens = max_output_tokens
        self.last_usage: Dict[str, int] | None = None

    def _record_usage(self, response: Any) -> int | None:
        usage = getattr(response, "usage", None)
        total_tokens = getattr(usage, "total_tokens", None) if usage is not None else None
        if total_tokens is None and usage is not None:
            prompt_tokens = getattr(usage, "prompt_tokens", None)
            completion_tokens = getattr(usage, "completion_tokens", None)
            if prompt_tokens is not None or completion_tokens is not None:
                total_tokens = int(prompt_tokens or 0) + int(completion_tokens or 0)
        try:
            parsed = int(total_tokens)
        except (TypeError, ValueError):
            self.last_usage = None
            return None
        if parsed < 0:
            self.last_usage = None
            return None
        self.last_usage = {"total_tokens": parsed}
        return parsed

    @_llm_retry()
    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Extract structured data using JSON Schema mode.

        Args:
            text: Text to extract from
            schema_spec: Either a wrapped spec {'name', 'strict', 'schema'} or raw JSON schema
            system_prompt: Optional custom system prompt. If None, uses default.
            user_message: Optional custom user message. If None, uses default.
        """
        name, strict, raw_schema = _unwrap_schema_spec(schema_spec)
        runtime_schema = copy.deepcopy(raw_schema)

        if runtime_schema.get("type") != "object" or "properties" not in runtime_schema:
            raise ValueError(
                f"Not a valid JSON Schema object. Top-level keys: {list(runtime_schema.keys())}"
            )

        if system_prompt is None:
            system_prompt = DEFAULT_EXTRACTION_SYSTEM_PROMPT

        if user_message is None:
            user_message = (
                f"Extract the data into the requested JSON format.\n\nDescription:\n{text}"
            )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": name,
                "schema": runtime_schema,
                "strict": strict,
            },
        }
        if self.structured_output_mode == "json_object":
            response_format = {"type": "json_object"}
            messages = self._messages_with_schema_guidance(messages, runtime_schema)

        try:
            response = self._create_chat_completion(messages, response_format)
        except openai.BadRequestError as exc:
            if (
                self.structured_output_mode == "json_object"
                and _structured_json_object_validation_failed(exc)
            ):
                logger.info(
                    "JSON Object response_format generated invalid JSON for provider/model %s; retrying without provider response_format.",
                    self.extraction_model,
                )
                response = self._create_chat_completion(messages, None)
            elif self.structured_output_mode != "auto" or not _structured_output_rejected(exc):
                raise
            else:
                logger.info(
                    "JSON Schema response_format rejected by provider for model %s; retrying JSON Object mode.",
                    self.extraction_model,
                )
                json_object_messages = self._messages_with_schema_guidance(messages, runtime_schema)
                try:
                    response = self._create_chat_completion(
                        json_object_messages,
                        {"type": "json_object"},
                    )
                except openai.BadRequestError as json_object_exc:
                    if not _structured_json_object_validation_failed(json_object_exc):
                        raise
                    logger.info(
                        "JSON Object response_format generated invalid JSON for provider/model %s; retrying without provider response_format.",
                        self.extraction_model,
                    )
                    response = self._create_chat_completion(json_object_messages, None)

        try:
            content = response.choices[0].message.content
            self._record_usage(response)
            data = _parse_structured_json_content(content)
        except (json.JSONDecodeError, IndexError, AttributeError, ValueError):
            logger.exception("Failed to parse structured data response")
            raise

        thought_process = data.get("thought_process", "No reasoning provided.")
        logger.debug("=" * 60)
        logger.debug(f"MODEL THINKING ({self.extraction_model}):")
        logger.debug("-" * 60)
        logger.debug(thought_process)
        logger.debug("=" * 60)

        return data

    def _create_chat_completion(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]],
    ) -> Any:
        kwargs: Dict[str, Any] = {}
        if response_format is not None:
            kwargs["response_format"] = response_format
        if self.max_output_tokens is not None:
            kwargs["max_tokens"] = self.max_output_tokens
        return self.client.chat.completions.create(
            model=self.extraction_model,
            messages=messages,
            temperature=self.extraction_temperature,
            **kwargs,
        )

    @staticmethod
    def _messages_with_schema_guidance(
        messages: List[Dict[str, str]],
        runtime_schema: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        guided_messages = [dict(message) for message in messages]
        schema_text = json.dumps(runtime_schema, sort_keys=True)
        guided_messages[-1]["content"] = (
            f"{guided_messages[-1]['content']}\n\n"
            "Return a single valid JSON object matching this JSON Schema. "
            "Do not include markdown fences or prose outside the JSON object.\n"
            f"{schema_text}"
        )
        return guided_messages

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        """Extract structured data from resumes using specialized resume instructions.

        Args:
            text: Resume text to extract from

        Returns:
            Extracted resume data following the RESUME_SCHEMA
        """
        data = self.extract_structured_data(
            text,
            RESUME_SCHEMA,
            system_prompt=RESUME_EXTRACTION_SYSTEM_PROMPT,
            user_message=f"Extract the structured resume data following the schema.\n\nResume:\n{text}",
        )

        logger.debug("=" * 60)
        logger.debug(f"RESUME EXTRACTION ({self.extraction_model}):")
        logger.debug("-" * 60)
        logger.debug(
            f"Extracted profile with {len(data.get('profile', {}).get('experience', []))} experience entries"
        )
        logger.debug("=" * 60)

        return data

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        """Extract structured qualification requirements from job descriptions.

        Args:
            text: Job description text

        Returns:
            Extracted requirements with required/preferred classifications
        """
        data = self.extract_structured_data(
            text,
            EXTRACTION_SCHEMA,
            system_prompt=REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT,
            user_message=(
                f"<JOB_DESCRIPTION>\n{text}\n</JOB_DESCRIPTION>\n\n"
                "Extract qualification requirements and the job offerings profile."
            ),
        )

        logger.debug("=" * 60)
        logger.debug(f"REQUIREMENTS EXTRACTION ({self.extraction_model}):")
        logger.debug("-" * 60)
        logger.debug("extract_structured_data returned type=%s value=%r", type(data), data)
        logger.debug("=" * 60)

        return data

    @_llm_retry()
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text."""
        client = self.embedding_client if self.embedding_client else self.client
        response = client.embeddings.create(
            input=text, model=self.embedding_model, dimensions=self.embedding_dimensions
        )
        self._record_usage(response)
        return _validate_embedding_vector(response.data[0].embedding)

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts in a single API call.

        Sends texts in chunks of up to 100 to stay within API limits.
        Returns embeddings in the same order as the input texts.
        """
        if not texts:
            return []

        _MAX_BATCH = 32
        client = self.embedding_client if self.embedding_client else self.client
        results: List[List[float]] = []
        total_tokens = 0
        usage_available = True

        for i in range(0, len(texts), _MAX_BATCH):
            chunk = texts[i : i + _MAX_BATCH]

            @_llm_retry()
            def _call(chunk=chunk):
                return client.embeddings.create(
                    input=chunk, model=self.embedding_model, dimensions=self.embedding_dimensions
                )

            response = _call()
            results.extend(
                _validate_embedding_vector(item.embedding) for item in response.data
            )
            usage = getattr(response, "usage", None)
            chunk_tokens = getattr(usage, "total_tokens", None) if usage is not None else None
            if chunk_tokens is None:
                usage_available = False
            else:
                total_tokens += max(int(chunk_tokens), 0)

        self.last_usage = {"total_tokens": total_tokens} if usage_available else None

        return results

    def unload_model(self, model_name: str):
        """Unload model from Ollama (no-op for pure OpenAI)."""
        if (
            "localhost" in str(self.client.base_url)
            or "127.0.0.1" in str(self.client.base_url)
            or "host.docker.internal" in str(self.client.base_url)
        ):
            try:
                base = str(self.client.base_url).rstrip("/").replace("/v1", "")
                url = f"{base}/api/generate"
                payload = {"model": model_name, "keep_alive": 0}
                logger.info(f"Unloading model: {model_name} via {url}")
                response = requests.post(url, json=payload, timeout=10)
                response.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to unload model {model_name}: {e}")
