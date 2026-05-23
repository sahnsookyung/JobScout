from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class ResumeVariantCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template_key: Literal["compact"] = "compact"
    tone: Literal["concise", "direct"] = "concise"
    force: bool = False


class ResumeVariantResponse(BaseModel):
    id: str
    match_id: str
    job_post_id: str
    template_key: str
    generation_mode: str
    created_at: str | None = None
    content: dict[str, Any]
    evidence_map: dict[str, Any]
    warnings: list[str]
    download_formats: list[str]
    reused: bool | None = None
    quota_status: dict[str, int] | None = None


class ResumeVariantEnvelope(BaseModel):
    success: bool
    variant: ResumeVariantResponse


class ResumeVariantListResponse(BaseModel):
    success: bool
    count: int
    variants: list[ResumeVariantResponse]
