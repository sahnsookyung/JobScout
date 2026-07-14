from typing import List, Optional, Any, Tuple, Dict
from sqlalchemy.orm import Session

from database.models import (
    JobMatch,
    JobPost,
    RESUME_FINGERPRINT_VERSION,
    SYSTEM_OWNER_ID,
)

from database.repositories.job_post import JobPostRepository
from database.repositories.resume import ResumeRepository
from database.repositories.match import MatchRepository
from database.repositories.match_selection import MatchSelectionRepository
from database.repositories.embedding import EmbeddingRepository
from database.repositories.candidate_preferences import CandidatePreferencesRepository
from database.repositories.notification_settings import NotificationSettingsRepository
from database.repositories.pipeline_run import PipelineRunRepository
from database.repositories.user_feature_capability import UserFeatureCapabilityRepository

class JobRepository:
    def __init__(self, db: Session):
        self.db = db
        self._job_post_repo: Optional[JobPostRepository] = None
        self._resume_repo: Optional[ResumeRepository] = None
        self._match_repo: Optional[MatchRepository] = None
        self._embedding_repo: Optional[EmbeddingRepository] = None
        self._candidate_preferences_repo: Optional[CandidatePreferencesRepository] = None
        self._notification_settings_repo: Optional[NotificationSettingsRepository] = None
        self._pipeline_run_repo: Optional[PipelineRunRepository] = None
        self._user_feature_capability_repo: Optional[UserFeatureCapabilityRepository] = None
        self._match_selection_repo: Optional[MatchSelectionRepository] = None

    @property
    def job_post(self) -> JobPostRepository:
        if self._job_post_repo is None:
            self._job_post_repo = JobPostRepository(self.db)
        return self._job_post_repo

    @property
    def resume(self) -> ResumeRepository:
        if self._resume_repo is None:
            self._resume_repo = ResumeRepository(self.db)
        return self._resume_repo

    @property
    def match(self) -> MatchRepository:
        if self._match_repo is None:
            self._match_repo = MatchRepository(self.db)
        return self._match_repo

    @property
    def embedding(self) -> EmbeddingRepository:
        if self._embedding_repo is None:
            self._embedding_repo = EmbeddingRepository(self.db)
        return self._embedding_repo

    @property
    def candidate_preferences(self) -> CandidatePreferencesRepository:
        if self._candidate_preferences_repo is None:
            self._candidate_preferences_repo = CandidatePreferencesRepository(self.db)
        return self._candidate_preferences_repo

    @property
    def match_selection(self) -> MatchSelectionRepository:
        if self._match_selection_repo is None:
            self._match_selection_repo = MatchSelectionRepository(self.db)
        return self._match_selection_repo

    @property
    def notification_settings(self) -> NotificationSettingsRepository:
        if self._notification_settings_repo is None:
            self._notification_settings_repo = NotificationSettingsRepository(self.db)
        return self._notification_settings_repo

    @property
    def pipeline_run(self) -> PipelineRunRepository:
        if self._pipeline_run_repo is None:
            self._pipeline_run_repo = PipelineRunRepository(self.db)
        return self._pipeline_run_repo

    @property
    def user_feature_capability(self) -> UserFeatureCapabilityRepository:
        if self._user_feature_capability_repo is None:
            self._user_feature_capability_repo = UserFeatureCapabilityRepository(self.db)
        return self._user_feature_capability_repo

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()

    def get_by_fingerprint(self, fingerprint: str, tenant_id: Optional[Any] = None) -> Optional[JobPost]:
        return self.job_post.get_by_fingerprint(fingerprint, tenant_id=tenant_id)

    def get_by_source(self, site_name: str, job_url: str, tenant_id: Optional[Any] = None) -> Optional[JobPost]:
        return self.job_post.get_by_source(site_name, job_url, tenant_id=tenant_id)

    def get_by_id(self, job_post_id: Any) -> JobPost:
        return self.job_post.get_by_id(job_post_id)

    def create_job_post(
        self,
        job_data: dict,
        fingerprint: str,
        location_text: str,
        tenant_id: Optional[Any] = None,
    ) -> JobPost:
        return self.job_post.create_job_post(
            job_data,
            fingerprint,
            location_text,
            tenant_id=tenant_id,
        )

    def get_or_create_source(
        self,
        job_post_id: Any,
        site_name: str,
        job_data: dict,
        tenant_id: Optional[Any] = None,
    ) -> None:
        return self.job_post.get_or_create_source(job_post_id, site_name, job_data, tenant_id=tenant_id)

    def _calculate_content_hash(self, job_data: dict) -> str:
        return self.job_post._calculate_content_hash(job_data)

    def save_job_content(self, job_post_id: Any, job_data: dict) -> None:
        return self.job_post.save_job_content(job_post_id, job_data)

    def update_timestamp(self, job_post: JobPost) -> None:
        return self.job_post.update_timestamp(job_post)

    def deactivate_missing_sources(
        self,
        site_name: str,
        seen_job_urls: List[str],
        tenant_id: Optional[Any] = None,
    ) -> int:
        return self.job_post.deactivate_missing_sources(site_name, seen_job_urls, tenant_id=tenant_id)

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_unextracted_jobs(limit)

    def mark_as_extracted(self, job_post: JobPost) -> None:
        return self.job_post.mark_as_extracted(job_post)

    def mark_extraction_in_progress(self, job_post_id: Any) -> None:
        return self.job_post.mark_extraction_in_progress(job_post_id)

    def mark_extraction_retryable_failed(self, job_post_id: Any, error: str) -> None:
        return self.job_post.mark_extraction_retryable_failed(job_post_id, error)

    def mark_extraction_failed(self, job_post_id: str, error: str) -> None:
        return self.job_post.mark_extraction_failed(job_post_id, error)

    def _extract_years_from_requirement(self, text: str) -> tuple:
        return self.job_post._extract_years_from_requirement(text)

    def save_requirements(self, job_post: JobPost, requirements: List[dict]) -> None:
        return self.job_post.save_requirements(job_post, requirements)

    def save_benefits(self, job_post: JobPost, benefits: List[dict]) -> None:
        return self.job_post.save_benefits(job_post, benefits)

    def save_job_offerings_profile(
        self,
        job_post: JobPost,
        profile: Dict[str, Any],
        *,
        source_description_hash: Optional[str],
        extraction_provider: Optional[str] = None,
        extraction_model: Optional[str] = None,
    ) -> Any:
        return self.job_post.save_job_offerings_profile(
            job_post,
            profile,
            source_description_hash=source_description_hash,
            extraction_provider=extraction_provider,
            extraction_model=extraction_model,
        )

    def get_job_offerings_profiles_by_job_ids(self, job_post_ids: List[Any]) -> Dict[str, Any]:
        return self.job_post.get_job_offerings_profiles_by_job_ids(job_post_ids)

    def update_job_metadata(self, job_post: JobPost, metadata: dict) -> None:
        return self.job_post.update_job_metadata(job_post, metadata)

    def update_content_metadata(self, job_post_id: Any, metadata: dict) -> None:
        return self.job_post.update_content_metadata(job_post_id, metadata)

    def get_unembedded_jobs(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_unembedded_jobs(limit)

    def get_unembedded_requirements(self, limit: int = 1000) -> list:
        return self.job_post.get_unembedded_requirements(limit)

    def get_requirement_by_id(self, req_id: Any) -> Any:
        return self.job_post.get_requirement_by_id(req_id)

    def save_job_embedding(self, job_post: JobPost, embedding: List[float]) -> None:
        return self.job_post.save_job_embedding(job_post, embedding)

    def mark_embedding_in_progress(self, job_post_id: Any) -> None:
        return self.job_post.mark_embedding_in_progress(job_post_id)

    def mark_embedding_retryable_failed(self, job_post_id: Any, error: str) -> None:
        return self.job_post.mark_embedding_retryable_failed(job_post_id, error)

    def bulk_mark_embedding_in_progress(self, job_post_ids: List[Any]) -> None:
        return self.job_post.bulk_mark_embedding_in_progress(job_post_ids)

    def save_requirement_embedding(self, req_id: Any, embedding: List[float]) -> None:
        return self.job_post.save_requirement_embedding(req_id, embedding)

    def mark_embedding_failed(self, job_post_id: Any, error: str) -> None:
        return self.job_post.mark_embedding_failed(job_post_id, error)

    def get_embedded_jobs_for_matching(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_embedded_jobs_for_matching(limit)

    def get_top_jobs_by_summary_embedding(
        self,
        resume_embedding: List[float],
        limit: int,
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None,
        exclude_reusable_resume_fingerprint: Optional[str] = None,
    ) -> List[Tuple[JobPost, float]]:
        return self.job_post.get_top_jobs_by_summary_embedding(
            resume_embedding,
            limit,
            tenant_id,
            require_remote,
            exclude_reusable_resume_fingerprint,
        )

    def get_top_jobs_by_lexical_query(
        self,
        lexical_query: str,
        *,
        resume_embedding: List[float],
        limit: Optional[int] = None,
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None,
        exclude_reusable_resume_fingerprint: Optional[str] = None,
    ) -> List[Tuple[JobPost, float, float]]:
        return self.job_post.get_top_jobs_by_lexical_query(
            lexical_query,
            resume_embedding=resume_embedding,
            limit=limit,
            tenant_id=tenant_id,
            require_remote=require_remote,
            exclude_reusable_resume_fingerprint=exclude_reusable_resume_fingerprint,
        )

    def quarantine_null_description_jobs(self, older_than_days: int = 7) -> int:
        return self.job_post.quarantine_null_description_jobs(older_than_days)

    def claim_missing_description_recovery_jobs(
        self,
        *,
        limit: int = 50,
        run_id: str,
        tenant_id: Optional[Any] = None,
    ) -> List[JobPost]:
        return self.job_post.claim_missing_description_recovery_jobs(
            limit=limit,
            run_id=run_id,
            tenant_id=tenant_id,
        )

    def queue_description_recovery_for_job(self, job: JobPost, *, run_id: str) -> None:
        return self.job_post.queue_description_recovery_for_job(job, run_id=run_id)

    def mark_description_recovery_refreshing(self, job: JobPost, *, run_id: str) -> None:
        return self.job_post.mark_description_recovery_refreshing(job, run_id=run_id)

    def mark_description_recovery_status(
        self,
        job: JobPost,
        *,
        status: str,
        reason: str,
        run_id: Optional[str] = None,
        error: Optional[str] = None,
        retryable: bool = False,
    ) -> None:
        return self.job_post.mark_description_recovery_status(
            job,
            status=status,
            reason=reason,
            run_id=run_id,
            error=error,
            retryable=retryable,
        )

    def mark_description_recovered(
        self,
        job: JobPost,
        *,
        run_id: str,
        reason: str = "description_found",
    ) -> None:
        return self.job_post.mark_description_recovered(job, run_id=run_id, reason=reason)

    def mark_description_recovery_posting_not_found(
        self,
        job: JobPost,
        *,
        source: Any,
        run_id: str,
        reason: str = "authoritative_sync_absent",
    ) -> None:
        return self.job_post.mark_description_recovery_posting_not_found(
            job,
            source=source,
            run_id=run_id,
            reason=reason,
        )

    def get_resume_summary_embedding(self, resume_fingerprint: str) -> Optional[List[float]]:
        return self.resume.get_resume_summary_embedding(resume_fingerprint)

    def get_resume_processing_state(self, resume_fingerprint: str) -> Any:
        return self.resume.get_resume_processing_state(resume_fingerprint)

    def get_latest_resume_processing_state(self) -> Any:
        return self.resume.get_latest_resume_processing_state()

    def create_resume_upload(
        self,
        params: Any,
    ) -> Any:
        return self.resume.create_resume_upload(params)

    def get_resume_upload(self, upload_id: Any, owner_id: Optional[Any] = None) -> Any:
        return self.resume.get_resume_upload(upload_id, owner_id)

    def get_latest_resume_upload(self, owner_id: Any) -> Any:
        return self.resume.get_latest_resume_upload(owner_id)

    def get_latest_resume_upload_for_hash(
        self,
        owner_id: Any,
        resume_hash: str,
    ) -> Any:
        return self.resume.get_latest_resume_upload_for_hash(
            owner_id,
            resume_hash,
        )

    def get_resume_upload_by_task_id(
        self,
        owner_id: Any,
        task_id: str,
    ) -> Any:
        return self.resume.get_resume_upload_by_task_id(owner_id, task_id)

    def update_resume_upload(
        self,
        upload_id: Any,
        *,
        status: Optional[str] = None,
        last_error: Optional[str] = None,
        processing_task_id: Optional[str] = None,
        failure_stage: Optional[str] = None,
        failure_class: Optional[str] = None,
        retryable: Optional[bool] = None,
        user_safe_message: Optional[str] = None,
        failure_debug_context: Optional[dict] = None,
    ) -> Any:
        return self.resume.update_resume_upload(
            upload_id,
            status=status,
            last_error=last_error,
            processing_task_id=processing_task_id,
            failure_stage=failure_stage,
            failure_class=failure_class,
            retryable=retryable,
            user_safe_message=user_safe_message,
            failure_debug_context=failure_debug_context,
        )

    def set_resume_processing_state(
        self,
        resume_fingerprint: str,
        status: str,
        *,
        owner_id: Any = None,
        error: Optional[str] = None,
        extraction_completed_at: Optional[Any] = None,
        embedding_completed_at: Optional[Any] = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
        failure_stage: Optional[str] = None,
        failure_class: Optional[str] = None,
        retryable: Optional[bool] = None,
        user_safe_message: Optional[str] = None,
    ) -> Any:
        owner_id = owner_id or SYSTEM_OWNER_ID
        return self.resume.set_resume_processing_state(
            owner_id=owner_id,
            resume_fingerprint=resume_fingerprint,
            status=status,
            error=error,
            extraction_completed_at=extraction_completed_at,
            embedding_completed_at=embedding_completed_at,
            fingerprint_version=fingerprint_version,
            failure_stage=failure_stage,
            failure_class=failure_class,
            retryable=retryable,
            user_safe_message=user_safe_message,
        )

    def is_resume_ready(self, resume_fingerprint: str) -> bool:
        return self.resume.is_resume_ready(resume_fingerprint)

    def get_latest_ready_resume_fingerprint(self) -> Optional[str]:
        return self.resume.get_latest_ready_resume_fingerprint()

    def resume_needs_embedding(self, resume_fingerprint: str) -> bool:
        return self.resume.resume_needs_embedding(resume_fingerprint)

    def save_structured_resume(
        self,
        resume_fingerprint: str,
        extracted_data: dict,
        *,
        owner_id: Any = None,
        total_experience_years: Optional[float] = None,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: Optional[list] = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> Any:
        owner_id = owner_id or SYSTEM_OWNER_ID
        return self.resume.save_structured_resume(
            owner_id=owner_id,
            resume_fingerprint=resume_fingerprint,
            extracted_data=extracted_data,
            total_experience_years=total_experience_years,
            extraction_confidence=extraction_confidence,
            extraction_warnings=extraction_warnings,
            fingerprint_version=fingerprint_version,
        )

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[dict],
        *,
        owner_id: Any = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> list:
        owner_id = owner_id or SYSTEM_OWNER_ID
        return self.resume.save_resume_section_embeddings(
            resume_fingerprint=resume_fingerprint,
            sections=sections,
            owner_id=owner_id,
            fingerprint_version=fingerprint_version,
        )

    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> list:
        return self.resume.get_resume_section_embeddings(resume_fingerprint, section_type)

    def get_structured_resume_by_fingerprint(self, resume_fingerprint: str) -> Any:
        return self.resume.get_structured_resume_by_fingerprint(resume_fingerprint)

    def get_capability(self, owner_id: Any, feature_key: str) -> Any:
        return self.user_feature_capability.get_capability(owner_id, feature_key)

    def upsert_capability(
        self,
        owner_id: Any,
        feature_key: str,
        *,
        enabled: bool = True,
        value_json: Optional[dict] = None,
        source: Optional[str] = None,
    ) -> Any:
        return self.user_feature_capability.upsert_capability(
            owner_id,
            feature_key,
            enabled=enabled,
            value_json=value_json,
            source=source,
        )

    def find_similar_resume_sections(
        self,
        query_embedding: List[float],
        section_type: Optional[str] = None,
        top_k: int = 10
    ) -> list:
        return self.embedding.find_similar_resume_sections(query_embedding, section_type, top_k)

    def get_existing_match(
        self,
        job_post_id: Any,
        resume_fingerprint: str,
        load_job_post: bool = False,
        owner_id: Any = SYSTEM_OWNER_ID,
    ) -> Optional[JobMatch]:
        return self.match.get_existing_match(
            job_post_id,
            resume_fingerprint,
            load_job_post,
            owner_id,
        )

    def get_matches_for_resume(
        self,
        resume_fingerprint: str,
        min_score: Optional[float] = None,
        status: str = 'active'
    ) -> List[JobMatch]:
        return self.match.get_matches_for_resume(resume_fingerprint, min_score, status)

    def get_reusable_matches_for_resume(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Optional[Any] = None,
    ) -> List[JobMatch]:
        return self.match.get_reusable_matches_for_resume(
            resume_fingerprint,
            tenant_id=tenant_id,
        )

    def count_reusable_matches_for_resume(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Optional[Any] = None,
    ) -> int:
        return self.match.count_reusable_matches_for_resume(
            resume_fingerprint,
            tenant_id=tenant_id,
        )

    def count_pending_matching_jobs(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Optional[Any] = None,
        candidate_preferences: Optional[dict[str, Any]] = None,
    ) -> int:
        return self.match.count_pending_matching_jobs(
            resume_fingerprint,
            tenant_id=tenant_id,
            candidate_preferences=candidate_preferences,
        )

    def activate_matches_by_ids(self, match_ids: List[Any]) -> int:
        return self.match.activate_matches_by_ids(match_ids)

    def invalidate_matches_for_job(
        self,
        job_post_id: Any,
        reason: str = "Job content changed"
    ) -> int:
        return self.match.invalidate_matches_for_job(job_post_id, reason)

    def invalidate_matches_for_resume(
        self,
        resume_fingerprint: str,
        reason: str = "Resume changed"
    ) -> int:
        return self.match.invalidate_matches_for_resume(resume_fingerprint, reason)

    def invalidate_matches_for_resume_except(
        self,
        resume_fingerprint: str,
        active_job_ids: List[Any],
        reason: str = "Resume changed",
    ) -> int:
        return self.match.invalidate_matches_for_resume_except(
            resume_fingerprint,
            active_job_ids,
            reason,
        )

    def get_stale_matches(self, limit: int = 100) -> List[JobMatch]:
        return self.match.get_stale_matches(limit)

    def batch_invalidate_matches_for_jobs(
        self,
        job_ids: List[Any],
        reason: str = "Job content changed"
    ) -> int:
        return self.match.batch_invalidate_matches_for_jobs(job_ids, reason)

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[dict],
        *,
        owner_id: Any = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> list:
        owner_id = owner_id or SYSTEM_OWNER_ID
        return self.resume.save_evidence_unit_embeddings(
            resume_fingerprint=resume_fingerprint,
            evidence_units=evidence_units,
            owner_id=owner_id,
            fingerprint_version=fingerprint_version,
        )

    def find_best_evidence_for_requirement(
        self,
        requirement_embedding: List[float],
        resume_fingerprint: str,
        top_k: int = 5
    ) -> list:
        return self.resume.find_best_evidence_for_requirement(
            requirement_embedding, resume_fingerprint, top_k
        )
