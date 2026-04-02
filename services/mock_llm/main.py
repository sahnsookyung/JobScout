from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Iterable, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from core.llm.fake_service import FakeLLMService

app = FastAPI(title="JobScout Mock OpenAI-Compatible LLM", version="1.0")

_RESUME_PATTERN = re.compile(r"Resume:\n(?P<body>.+)", re.DOTALL)
_JOB_DESCRIPTION_PATTERN = re.compile(
    r"<JOB_DESCRIPTION>\n(?P<body>.+?)\n</JOB_DESCRIPTION>",
    re.DOTALL,
)


class ChatMessage(BaseModel):
    role: str
    content: Any


class ChatCompletionsRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    response_format: Dict[str, Any] | None = None


class EmbeddingsRequest(BaseModel):
    model: str
    input: str | List[str]
    dimensions: int | None = None


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
        return "".join(parts)
    return str(content or "")


def _last_user_message(messages: Iterable[ChatMessage]) -> str:
    for message in reversed(list(messages)):
        if message.role == "user":
            return _message_text(message.content)
    return ""


def _extract_json_suffix(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("{"):
        return stripped
    parts = stripped.rsplit("\n\n", 1)
    return parts[-1].strip()


def _extract_resume_text(message: str) -> str:
    match = _RESUME_PATTERN.search(message)
    return (match.group("body") if match else message).strip()


def _extract_job_description(message: str) -> str:
    match = _JOB_DESCRIPTION_PATTERN.search(message)
    return (match.group("body") if match else message).strip()


def _fake_preference_profile(raw_text: str) -> Dict[str, Any]:
    normalized = " ".join(raw_text.split())
    lowered = normalized.lower()

    def _match_preferences(keywords: Dict[str, str]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for token, label in keywords.items():
            if token in lowered:
                items.append({"label": label, "weight": 0.8, "confidence": 0.9})
        return items

    return {
        "raw_text": normalized,
        "parse_version": "2026-04-01.v1",
        "parser_confidence": 0.82 if normalized else 0.0,
        "work_style": _match_preferences(
            {
                "remote": "Remote-friendly",
                "hybrid": "Hybrid",
                "mentorship": "Mentorship",
            }
        ),
        "team_culture": _match_preferences(
            {
                "collaborative": "Collaborative",
                "high trust": "High trust",
                "kind": "Kind team",
            }
        ),
        "tech_stack": _match_preferences(
            {
                "python": "Python",
                "fastapi": "FastAPI",
                "microservices": "Microservices",
                "aws": "AWS",
                "postgres": "PostgreSQL",
            }
        ),
        "mission_domain": _match_preferences(
            {
                "climate": "Climate",
                "health": "Health",
                "education": "Education",
            }
        ),
        "growth_preferences": _match_preferences(
            {
                "growth": "Growth",
                "learning": "Learning",
                "ownership": "Ownership",
            }
        ),
        "negative_preferences": _match_preferences(
            {
                "onsite": "On-site only",
                "bureaucracy": "Heavy bureaucracy",
            }
        ),
    }


def _schema_name(payload: ChatCompletionsRequest) -> str:
    response_format = payload.response_format or {}
    if response_format.get("type") != "json_schema":
        return ""
    json_schema = response_format.get("json_schema") or {}
    return str(json_schema.get("name", ""))


def _build_schema_response(payload: ChatCompletionsRequest) -> Dict[str, Any]:
    fake_llm = FakeLLMService()
    user_message = _last_user_message(payload.messages)
    schema_name = _schema_name(payload)

    if schema_name == "resume_schema_v1.0":
        return fake_llm.extract_resume_data(_extract_resume_text(user_message))
    if schema_name == "job_extraction_schema":
        return fake_llm.extract_requirements_data(_extract_job_description(user_message))
    if schema_name == "facet_extraction_schema":
        return fake_llm.extract_facet_data(_extract_job_description(user_message))
    if schema_name == "preference_profile_schema":
        raw_text = _extract_json_suffix(user_message)
        return _fake_preference_profile(raw_text)
    if schema_name == "semantic_fit_pairs_v1":
        json_payload = _extract_json_suffix(user_message)
        return fake_llm.extract_structured_data(
            json_payload,
            {"name": "semantic_fit_pairs_v1"},
        )

    try:
        return fake_llm.extract_structured_data(
            _extract_json_suffix(user_message),
            {"name": schema_name} if schema_name else {},
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/chat/completions")
def chat_completions(payload: ChatCompletionsRequest) -> Dict[str, Any]:
    content = json.dumps(_build_schema_response(payload))
    return {
        "id": "chatcmpl-mock-1",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": payload.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                },
                "finish_reason": "stop",
            }
        ],
    }


@app.post("/v1/embeddings")
def embeddings(payload: EmbeddingsRequest) -> Dict[str, Any]:
    inputs = payload.input if isinstance(payload.input, list) else [payload.input]
    fake_llm = FakeLLMService(embedding_dimensions=payload.dimensions or 1024)
    data = [
        {
            "object": "embedding",
            "embedding": fake_llm.generate_embedding(item),
            "index": index,
        }
        for index, item in enumerate(inputs)
    ]
    return {
        "object": "list",
        "model": payload.model,
        "data": data,
    }
