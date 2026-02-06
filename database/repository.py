from typing import List, Optional, Any, Tuple
from sqlalchemy.orm import Session

from database.models import JobPost, JobMatch

from database.repositories.job_post import JobPostRepository
from database.repositories.resume import ResumeRepository
from database.repositories.match import MatchRepository
from database.repositories.embedding import EmbeddingRepository


class JobRepository:
    def __init__(self, db: Session):
        self.db = db
        self._job_post_repo: Optional[JobPostRepository] = None
        self._resume_repo: Optional[ResumeRepository] = None
        self._match_repo: Optional[MatchRepository] = None
        self._embedding_repo: Optional[EmbeddingRepository] = None

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

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()

    def get_by_fingerprint(self, fingerprint: str) -> Optional[JobPost]:
        return self.job_post.get_by_fingerprint(fingerprint)

    def get_by_id(self, job_post_id: Any) -> JobPost:
        return self.job_post.get_by_id(job_post_id)

    def create_job_post(self, job_data: dict, fingerprint: str, location_text: str) -> JobPost:
        return self.job_post.create_job_post(job_data, fingerprint, location_text)

    def get_or_create_source(self, job_post_id: Any, site_name: str, job_data: dict) -> None:
        return self.job_post.get_or_create_source(job_post_id, site_name, job_data)

    def _calculate_content_hash(self, job_data: dict) -> str:
        return self.job_post._calculate_content_hash(job_data)

    def save_job_content(self, job_post_id: Any, job_data: dict) -> None:
        return self.job_post.save_job_content(job_post_id, job_data)

    def update_timestamp(self, job_post: JobPost) -> None:
        return self.job_post.update_timestamp(job_post)

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_unextracted_jobs(limit)

    def mark_as_extracted(self, job_post: JobPost) -> None:
        return self.job_post.mark_as_extracted(job_post)

    def _extract_years_from_requirement(self, text: str) -> tuple:
        return self.job_post._extract_years_from_requirement(text)

    def save_requirements(self, job_post: JobPost, requirements: List[dict]) -> None:
        return self.job_post.save_requirements(job_post, requirements)

    def save_benefits(self, job_post: JobPost, benefits: List[dict]) -> None:
        return self.job_post.save_benefits(job_post, benefits)

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

    def save_requirement_embedding(self, req_id: Any, embedding: List[float]) -> None:
        return self.job_post.save_requirement_embedding(req_id, embedding)

    def get_embedded_jobs_for_matching(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_embedded_jobs_for_matching(limit)

    def get_top_jobs_by_summary_embedding(
        self,
        resume_embedding: List[float],
        limit: int,
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None
    ) -> List[Tuple[JobPost, float]]:
        return self.job_post.get_top_jobs_by_summary_embedding(
            resume_embedding, limit, tenant_id, require_remote
        )

    def get_jobs_for_matching(
        self,
        limit: Optional[int] = None,
        is_embedded: bool = True
    ) -> List[JobPost]:
        return self.job_post.get_jobs_for_matching(limit, is_embedded)

    def get_jobs_needing_facet_extraction(self, limit: int = 100) -> List[JobPost]:
        return self.job_post.get_jobs_needing_facet_extraction(limit)

    def save_job_facet_embedding(
        self,
        job_post_id: Any,
        facet_key: str,
        facet_text: str,
        embedding: List[float],
        content_hash: str
    ) -> Any:
        return self.job_post.save_job_facet_embedding(
            job_post_id, facet_key, facet_text, embedding, content_hash
        )

    def get_job_facet_embeddings(self, job_post_id: Any) -> dict:
        return self.job_post.get_job_facet_embeddings(job_post_id)

    def mark_job_facets_extracted(self, job_post_id: Any, content_hash: str = None) -> None:
        return self.job_post.mark_job_facets_extracted(job_post_id, content_hash)

    def delete_all_facet_embeddings_for_job(self, job_post_id: Any) -> None:
        return self.job_post.delete_all_facet_embeddings_for_job(job_post_id)

    def get_and_claim_jobs_for_facet_extraction(
        self,
        limit: int = 100,
        worker_id: str = "default",
        claim_timeout_minutes: int = 30,
        max_retries: int = 5
    ) -> List[JobPost]:
        return self.job_post.get_and_claim_jobs_for_facet_extraction(
            limit, worker_id, claim_timeout_minutes, max_retries
        )

    def mark_job_facets_failed(self, job_post_id: Any, error: str = None) -> None:
        return self.job_post.mark_job_facets_failed(job_post_id, error)

    def get_resume_summary_embedding(self, resume_fingerprint: str) -> Optional[List[float]]:
        return self.resume.get_resume_summary_embedding(resume_fingerprint)

    def save_structured_resume(
        self,
        resume_fingerprint: str,
        extracted_data: dict,
        total_experience_years: Optional[float] = None,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: Optional[list] = None
    ) -> Any:
        return self.resume.save_structured_resume(
            resume_fingerprint=resume_fingerprint,
            extracted_data=extracted_data,
            total_experience_years=total_experience_years,
            extraction_confidence=extraction_confidence,
            extraction_warnings=extraction_warnings
        )

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[dict]
    ) -> list:
        return self.resume.save_resume_section_embeddings(resume_fingerprint, sections)

    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> list:
        return self.resume.get_resume_section_embeddings(resume_fingerprint, section_type)

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
        resume_fingerprint: str
    ) -> Optional[JobMatch]:
        return self.match.get_existing_match(job_post_id, resume_fingerprint)

    def get_matches_for_resume(
        self,
        resume_fingerprint: str,
        min_score: Optional[float] = None,
        status: str = 'active'
    ) -> List[JobMatch]:
        return self.match.get_matches_for_resume(resume_fingerprint, min_score, status)

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

    def get_stale_matches(self, limit: int = 100) -> List[JobMatch]:
        return self.match.get_stale_matches(limit)

    def batch_invalidate_matches_for_jobs(
        self,
        job_ids: List[Any],
        reason: str = "Job content changed"
    ) -> int:
        return self.match.batch_invalidate_matches_for_jobs(job_ids, reason)

    def save_user_wants(
        self,
        user_id: str,
        resume_fingerprint: Optional[str],
        wants_text: str,
        embedding: List[float],
        facet_key: Optional[str] = None
    ) -> Any:
        return self.resume.save_user_wants(
            user_id, resume_fingerprint, wants_text, embedding, facet_key
        )

    def get_user_wants_embeddings(
        self,
        user_id: str,
        resume_fingerprint: Optional[str] = None
    ) -> List[List[float]]:
        return self.resume.get_user_wants_embeddings(user_id, resume_fingerprint)

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[dict]
    ) -> list:
        return self.resume.save_evidence_unit_embeddings(resume_fingerprint, evidence_units)

    def find_best_evidence_for_requirement(
        self,
        requirement_embedding: List[float],
        resume_fingerprint: str,
        top_k: int = 5
    ) -> list:
        return self.resume.find_best_evidence_for_requirement(
            requirement_embedding, resume_fingerprint, top_k
        )
