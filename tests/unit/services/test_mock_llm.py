"""Unit tests for services/mock_llm/main.py"""
import json

from fastapi.testclient import TestClient

from tests.mocks.fake_service import FAIL_EXTRACTION_MARKER
from tests.services.mock_llm.main import app

client = TestClient(app)


def _chat_request(user_content: str, schema_name: str = "") -> dict:
    payload: dict = {
        "model": "mock",
        "messages": [{"role": "user", "content": user_content}],
    }
    if schema_name:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": schema_name},
        }
    return payload


def test_health_endpoint():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_chat_completions_fallback_returns_dict_for_valid_json():
    """Unknown schema with a valid JSON dict payload returns it in the response."""
    body = json.dumps({"key": "value"})
    resp = client.post("/v1/chat/completions", json=_chat_request(body, "unknown_schema"))
    assert resp.status_code == 200
    data = resp.json()
    content = json.loads(data["choices"][0]["message"]["content"])
    assert content == {"key": "value"}


def test_chat_completions_fallback_raises_400_on_extraction_failure():
    """FakeLLMService raising an exception in the fallback branch → HTTP 400."""
    # FAIL_EXTRACTION_MARKER triggers ValueError in FakeLLMService._maybe_fail_extraction
    body = f"prefix\n\n{FAIL_EXTRACTION_MARKER}"
    resp = client.post(
        "/v1/chat/completions",
        json=_chat_request(body, "some_schema"),
    )
    assert resp.status_code == 400
