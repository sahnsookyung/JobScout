from __future__ import annotations

import uuid
from io import BytesIO
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from core.resume_variants.quota import (
    ResumeVariantConcurrencyError,
    ResumeVariantQuotaExceeded,
    ResumeVariantQuotaUnavailable,
)
from core.resume_variants.renderer import ResumeVariantRenderer, safe_filename
from core.resume_variants.service import (
    ResumeVariantConflict,
    ResumeVariantNotFound,
    ResumeVariantRequest,
    ResumeVariantService,
    ResumeVariantValidationError,
    variant_to_response,
)
from web.backend.dependencies import get_current_user, get_db
from web.backend.models.resume_variants import (
    ResumeVariantCreateRequest,
    ResumeVariantEnvelope,
    ResumeVariantListResponse,
)

router = APIRouter(tags=["resume-variants"])

DbSession = Annotated[Session, Depends(get_db)]

_MIME_TYPES = {
    "markdown": "text/markdown; charset=utf-8",
    "html": "text/html; charset=utf-8",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def _uuid_or_400(value: str, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field} format.") from exc


def _request_tenant_id(request: Request) -> uuid.UUID | None:
    state_tenant_id = getattr(request.state, "tenant_id", None)
    if state_tenant_id is not None:
        try:
            return uuid.UUID(str(state_tenant_id))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Trusted tenant context must be a UUID.") from exc
    tenant_header = request.headers.get("X-Tenant-Id", "").strip()
    if not tenant_header:
        return None
    try:
        return uuid.UUID(tenant_header)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="X-Tenant-Id must be a UUID.") from exc


def _service(db: Session) -> ResumeVariantService:
    return ResumeVariantService(db)


@router.post(
    "/api/matches/{match_id}/resume-variants",
    response_model=ResumeVariantEnvelope,
    responses={
        400: {"description": "Invalid UUID or untrusted tenant header"},
        404: {"description": "Match not found"},
        409: {"description": "Match cannot generate a resume variant"},
        413: {"description": "Generated variant exceeds size limit"},
        422: {"description": "Invalid request body"},
        429: {"description": "Resume variant quota exceeded"},
        503: {"description": "Quota backend unavailable"},
    },
)
def create_resume_variant(
    match_id: str,
    body: ResumeVariantCreateRequest,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
) -> ResumeVariantEnvelope:
    result = _run_service_call(
        lambda: _service(db).create_for_match(
            match_id=_uuid_or_400(match_id, "match_id"),
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
            request=ResumeVariantRequest(
                template_key=body.template_key,
                tone=body.tone,
                force=body.force,
            ),
        )
    )
    return ResumeVariantEnvelope(
        success=True,
        variant=variant_to_response(
            result.variant,
            reused=result.reused,
            quota_status=result.quota_status,
        ),
    )


@router.get("/api/resume-variants/{variant_id}", response_model=ResumeVariantEnvelope)
def get_resume_variant(
    variant_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
) -> ResumeVariantEnvelope:
    variant = _run_service_call(
        lambda: _service(db).get_variant(
            variant_id=_uuid_or_400(variant_id, "variant_id"),
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
        )
    )
    return ResumeVariantEnvelope(success=True, variant=variant_to_response(variant))


@router.get("/api/resume-variants", response_model=ResumeVariantListResponse)
def list_resume_variants(
    match_id: Annotated[str, Query(description="Match ID to list variants for")],
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 25,
) -> ResumeVariantListResponse:
    variants = _run_service_call(
        lambda: _service(db).list_for_match(
            match_id=_uuid_or_400(match_id, "match_id"),
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
            limit=limit,
        )
    )
    return ResumeVariantListResponse(
        success=True,
        count=len(variants),
        variants=[variant_to_response(variant) for variant in variants],
    )


@router.get("/api/resume-variants/{variant_id}/download")
def download_resume_variant(
    variant_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    format: Annotated[str, Query(pattern="^(markdown|html|docx)$")] = "markdown",
) -> StreamingResponse:
    variant = _run_service_call(
        lambda: _service(db).get_variant(
            variant_id=_uuid_or_400(variant_id, "variant_id"),
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
        )
    )
    renderer = ResumeVariantRenderer()
    try:
        if format == "markdown":
            payload = renderer.render_markdown(variant.content_json)
            extension = "md"
        elif format == "html":
            payload = renderer.render_html(variant.content_json)
            extension = "html"
        else:
            payload = renderer.render_docx(variant.content_json)
            extension = "docx"
    except ValueError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc

    filename = safe_filename(f"resume-variant-{variant.id}", extension)
    return StreamingResponse(
        BytesIO(payload),
        media_type=_MIME_TYPES[format],
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _run_service_call(fn):
    try:
        return fn()
    except ResumeVariantNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ResumeVariantConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ResumeVariantValidationError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except ResumeVariantConcurrencyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ResumeVariantQuotaExceeded as exc:
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = str(exc.retry_after)
        raise HTTPException(status_code=429, detail=str(exc), headers=headers) from exc
    except ResumeVariantQuotaUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
