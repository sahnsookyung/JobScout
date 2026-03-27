from dataclasses import dataclass
from typing import Any, Optional

from database.models import (
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_FAILED,
    RESUME_PROCESSING_READY,
    RESUME_UPLOAD_FAILED_RETRYABLE,
    RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
    RESUME_UPLOAD_IN_PROGRESS,
    RESUME_UPLOAD_PENDING,
    RESUME_UPLOAD_READY,
    RESUME_FINGERPRINT_VERSION,
    generate_resume_fingerprint,
)
from database.uow import job_uow

PROCESSING_BLOCKING_STATUSES = {
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EMBEDDING,
}


@dataclass(frozen=True)
class ResumeEligibility:
    owner_id: Any
    can_run: bool
    processing_status: str
    message: str
    retryable: bool
    upload_id: Optional[str] = None
    resume_hash: Optional[str] = None
    resume_fingerprint: Optional[str] = None
    processing_task_id: Optional[str] = None


@dataclass(frozen=True)
class ResumePreflight:
    owner_id: Any
    status: str
    message: str
    retryable: bool
    can_skip_upload: bool
    resume_hash: str
    upload_id: Optional[str] = None
    processing_task_id: Optional[str] = None
    resume_fingerprint: Optional[str] = None


def resolve_owner_id(user: Optional[Any]) -> Any:
    owner_id = getattr(user, "id", None) if user is not None else None
    if owner_id is None:
        raise ValueError("Authenticated user is required")
    return owner_id


def serialize_owner_id(owner_id: Any) -> str:
    return str(owner_id)


def build_resume_fingerprint(owner_id: Any, resume_hash: str) -> str:
    return generate_resume_fingerprint(owner_id, resume_hash, RESUME_FINGERPRINT_VERSION)


def evaluate_resume_preflight(owner_id: Any, resume_hash: str) -> ResumePreflight:
    resume_fingerprint = build_resume_fingerprint(owner_id, resume_hash)
    with job_uow() as repo:
        latest_same_upload = repo.get_latest_resume_upload_for_hash(owner_id, resume_hash)

        if repo.is_resume_ready(resume_fingerprint):
            return ResumePreflight(
                owner_id=owner_id,
                status="ready_already_known",
                message="Resume already processed and ready for matching.",
                retryable=False,
                can_skip_upload=True,
                resume_hash=resume_hash,
                upload_id=str(latest_same_upload.id) if latest_same_upload else None,
                processing_task_id=latest_same_upload.processing_task_id if latest_same_upload else None,
                resume_fingerprint=resume_fingerprint,
            )

        if latest_same_upload is not None:
            task_id = latest_same_upload.processing_task_id
            if latest_same_upload.status in {RESUME_UPLOAD_PENDING, RESUME_UPLOAD_IN_PROGRESS}:
                return ResumePreflight(
                    owner_id=owner_id,
                    status="processing_existing",
                    message="Resume is already being processed.",
                    retryable=True,
                    can_skip_upload=True,
                    resume_hash=resume_hash,
                    upload_id=str(latest_same_upload.id),
                    processing_task_id=task_id,
                    resume_fingerprint=resume_fingerprint,
                )

            if latest_same_upload.status == RESUME_UPLOAD_FAILED_RETRYABLE:
                return ResumePreflight(
                    owner_id=owner_id,
                    status=RESUME_UPLOAD_FAILED_RETRYABLE,
                    message=latest_same_upload.user_safe_message or latest_same_upload.last_error or "Resume processing failed but can be retried.",
                    retryable=True,
                    can_skip_upload=True,
                    resume_hash=resume_hash,
                    upload_id=str(latest_same_upload.id),
                    processing_task_id=task_id,
                    resume_fingerprint=resume_fingerprint,
                )

            if latest_same_upload.status == RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED:
                return ResumePreflight(
                    owner_id=owner_id,
                    status=RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
                    message=latest_same_upload.user_safe_message or latest_same_upload.last_error or "Resume processing failed and requires re-upload.",
                    retryable=False,
                    can_skip_upload=False,
                    resume_hash=resume_hash,
                    upload_id=str(latest_same_upload.id),
                    processing_task_id=task_id,
                    resume_fingerprint=resume_fingerprint,
                )

        state = repo.get_resume_processing_state(resume_fingerprint)
        if state and state.processing_status in PROCESSING_BLOCKING_STATUSES:
            return ResumePreflight(
                owner_id=owner_id,
                status="processing_existing",
                message=state.user_safe_message or f"Resume is still processing ({state.processing_status}).",
                retryable=True,
                can_skip_upload=True,
                resume_hash=resume_hash,
                resume_fingerprint=resume_fingerprint,
            )

        if state and state.processing_status == RESUME_PROCESSING_FAILED:
            return ResumePreflight(
                owner_id=owner_id,
                status=(
                    RESUME_UPLOAD_FAILED_RETRYABLE
                    if state.retryable
                    else RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED
                ),
                message=state.user_safe_message or state.last_error or "Previous processing attempt failed.",
                retryable=bool(state.retryable),
                can_skip_upload=bool(state.retryable),
                resume_hash=resume_hash,
                resume_fingerprint=resume_fingerprint,
            )

    return ResumePreflight(
        owner_id=owner_id,
        status="upload_required",
        message="Resume upload is required.",
        retryable=True,
        can_skip_upload=False,
        resume_hash=resume_hash,
        resume_fingerprint=resume_fingerprint,
    )


def evaluate_resume_eligibility(owner_id: Any) -> ResumeEligibility:
    with job_uow() as repo:
        latest_upload = repo.get_latest_resume_upload(owner_id)
        if latest_upload is None:
            return ResumeEligibility(
                owner_id=owner_id,
                can_run=False,
                processing_status="missing",
                message="No resume has been uploaded yet.",
                retryable=True,
            )

        upload_id = str(latest_upload.id)
        task_id = latest_upload.processing_task_id if isinstance(latest_upload.processing_task_id, str) else None

        if latest_upload.status == RESUME_UPLOAD_READY and repo.is_resume_ready(latest_upload.resume_fingerprint):
            return ResumeEligibility(
                owner_id=owner_id,
                can_run=True,
                processing_status=RESUME_PROCESSING_READY,
                message="Resume is ready for matching.",
                retryable=False,
                upload_id=upload_id,
                resume_hash=latest_upload.resume_hash,
                resume_fingerprint=latest_upload.resume_fingerprint,
                processing_task_id=task_id,
            )

        if latest_upload.status in {RESUME_UPLOAD_PENDING, RESUME_UPLOAD_IN_PROGRESS}:
            state = repo.get_resume_processing_state(latest_upload.resume_fingerprint)
            status = state.processing_status if state else "processing"
            return ResumeEligibility(
                owner_id=owner_id,
                can_run=False,
                processing_status=status,
                message=(
                    state.user_safe_message
                    if state and state.user_safe_message
                    else f"Latest uploaded resume is still processing ({status})."
                ),
                retryable=True,
                upload_id=upload_id,
                resume_hash=latest_upload.resume_hash,
                resume_fingerprint=latest_upload.resume_fingerprint,
                processing_task_id=task_id,
            )

        if latest_upload.status in {
            RESUME_UPLOAD_FAILED_RETRYABLE,
            RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
        }:
            return ResumeEligibility(
                owner_id=owner_id,
                can_run=False,
                processing_status=latest_upload.status,
                message=latest_upload.user_safe_message or latest_upload.last_error or "Latest uploaded resume failed.",
                retryable=bool(latest_upload.retryable),
                upload_id=upload_id,
                resume_hash=latest_upload.resume_hash,
                resume_fingerprint=latest_upload.resume_fingerprint,
                processing_task_id=task_id,
            )

        return ResumeEligibility(
            owner_id=owner_id,
            can_run=False,
            processing_status=latest_upload.status,
            message="Latest uploaded resume is not ready for matching.",
            retryable=True,
            upload_id=upload_id,
            resume_hash=latest_upload.resume_hash,
            resume_fingerprint=latest_upload.resume_fingerprint,
            processing_task_id=task_id,
        )
