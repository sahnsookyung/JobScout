import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, delete

from database.models import (
    StructuredResume,
    ResumeSectionEmbedding,
    ResumeEvidenceUnitEmbedding,
    ResumeProcessingState,
    ResumeUpload,
    RESUME_PROCESSING_READY,
    RESUME_FINGERPRINT_VERSION,
    DEFAULT_LEGACY_OWNER_ID,
    RESUME_UPLOAD_PENDING,
)
from database.repositories.base import BaseRepository
from core.utils import cosine_similarity_from_distance

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResumeUploadCreateParams:
    owner_id: Any
    resume_hash: str
    resume_fingerprint: str
    original_filename: Optional[str] = None
    status: str = RESUME_UPLOAD_PENDING
    last_error: Optional[str] = None
    processing_task_id: Optional[str] = None
    retry_of_upload_id: Optional[Any] = None
    fingerprint_version: int = RESUME_FINGERPRINT_VERSION
    failure_stage: Optional[str] = None
    failure_class: Optional[str] = None
    retryable: Optional[bool] = None
    user_safe_message: Optional[str] = None
    failure_debug_context: Optional[Dict[str, Any]] = None


class ResumeRepository(BaseRepository):
    def get_resume_processing_state(
        self,
        resume_fingerprint: str
    ) -> Optional[ResumeProcessingState]:
        stmt = select(ResumeProcessingState).where(
            ResumeProcessingState.resume_fingerprint == resume_fingerprint
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_latest_resume_processing_state(self) -> Optional[ResumeProcessingState]:
        stmt = select(ResumeProcessingState).order_by(
            ResumeProcessingState.updated_at.desc()
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def create_resume_upload(self, params: ResumeUploadCreateParams) -> ResumeUpload:
        upload = ResumeUpload(
            owner_id=params.owner_id,
            resume_hash=params.resume_hash,
            fingerprint_version=params.fingerprint_version,
            resume_fingerprint=params.resume_fingerprint,
            original_filename=params.original_filename,
            status=params.status,
            last_error=params.last_error,
            processing_task_id=params.processing_task_id,
            retry_of_upload_id=params.retry_of_upload_id,
            failure_stage=params.failure_stage,
            failure_class=params.failure_class,
            retryable=params.retryable,
            user_safe_message=params.user_safe_message,
            failure_debug_context=params.failure_debug_context,
        )
        self.db.add(upload)
        self.db.flush()
        return upload

    def get_resume_upload(
        self,
        upload_id: Any,
        owner_id: Optional[Any] = None,
    ) -> Optional[ResumeUpload]:
        stmt = select(ResumeUpload).where(ResumeUpload.id == upload_id)
        if owner_id is not None:
            stmt = stmt.where(ResumeUpload.owner_id == owner_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_latest_resume_upload(self, owner_id: Any) -> Optional[ResumeUpload]:
        stmt = select(ResumeUpload).where(
            ResumeUpload.owner_id == owner_id
        ).order_by(
            ResumeUpload.created_at.desc(),
            ResumeUpload.id.desc(),
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_ready_resume_uploads(self, owner_id: Any) -> List[ResumeUpload]:
        stmt = select(ResumeUpload).where(
            ResumeUpload.owner_id == owner_id,
            ResumeUpload.status == RESUME_UPLOAD_READY,
        ).order_by(
            ResumeUpload.created_at.desc(),
            ResumeUpload.id.desc(),
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_latest_ready_resume_upload(self, owner_id: Any) -> Optional[ResumeUpload]:
        stmt = select(ResumeUpload).where(
            ResumeUpload.owner_id == owner_id,
            ResumeUpload.status == RESUME_UPLOAD_READY,
        ).order_by(
            ResumeUpload.created_at.desc(),
            ResumeUpload.id.desc(),
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_latest_resume_upload_for_hash(
        self,
        owner_id: Any,
        resume_hash: str,
    ) -> Optional[ResumeUpload]:
        stmt = select(ResumeUpload).where(
            ResumeUpload.owner_id == owner_id,
            ResumeUpload.resume_hash == resume_hash,
        ).order_by(
            ResumeUpload.created_at.desc(),
            ResumeUpload.id.desc(),
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_resume_upload_by_task_id(
        self,
        owner_id: Any,
        task_id: str,
    ) -> Optional[ResumeUpload]:
        stmt = select(ResumeUpload).where(
            ResumeUpload.owner_id == owner_id,
            ResumeUpload.processing_task_id == task_id,
        ).order_by(
            ResumeUpload.created_at.desc(),
            ResumeUpload.id.desc(),
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

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
        failure_debug_context: Optional[Dict[str, Any]] = None,
    ) -> ResumeUpload:
        upload = self.get_resume_upload(upload_id)
        if upload is None:
            raise ValueError(f"Resume upload not found: {upload_id}")

        if status is not None:
            upload.status = status
        upload.last_error = last_error
        if processing_task_id is not None:
            upload.processing_task_id = processing_task_id
        upload.failure_stage = failure_stage
        upload.failure_class = failure_class
        upload.retryable = retryable
        upload.user_safe_message = user_safe_message
        upload.failure_debug_context = failure_debug_context

        self.db.flush()
        return upload

    def set_resume_processing_state(
        self,
        resume_fingerprint: str,
        status: str,
        *,
        owner_id: Any = DEFAULT_LEGACY_OWNER_ID,
        error: Optional[str] = None,
        extraction_completed_at: Optional[Any] = None,
        embedding_completed_at: Optional[Any] = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
        failure_stage: Optional[str] = None,
        failure_class: Optional[str] = None,
        retryable: Optional[bool] = None,
        user_safe_message: Optional[str] = None,
    ) -> ResumeProcessingState:
        owner_id = owner_id or DEFAULT_LEGACY_OWNER_ID
        state = self.get_resume_processing_state(resume_fingerprint)
        if state is None:
            state = ResumeProcessingState(
                owner_id=owner_id,
                fingerprint_version=fingerprint_version,
                resume_fingerprint=resume_fingerprint,
                processing_status=status,
            )
            self.db.add(state)

        state.owner_id = owner_id
        state.fingerprint_version = fingerprint_version
        state.processing_status = status
        state.last_error = error
        state.failure_stage = failure_stage
        state.failure_class = failure_class
        state.retryable = retryable
        state.user_safe_message = user_safe_message
        if extraction_completed_at is not None:
            state.extraction_completed_at = extraction_completed_at
        if embedding_completed_at is not None:
            state.embedding_completed_at = embedding_completed_at

        self.db.flush()
        return state

    def is_resume_ready(self, resume_fingerprint: str) -> bool:
        state = self.get_resume_processing_state(resume_fingerprint)
        if not state or state.processing_status != RESUME_PROCESSING_READY:
            return False

        structured = self.get_structured_resume_by_fingerprint(resume_fingerprint)
        if structured is None:
            return False

        summary_embedding = self.get_resume_summary_embedding(resume_fingerprint)
        if summary_embedding is None:
            return False

        evidence_stmt = select(ResumeEvidenceUnitEmbedding.id).where(
            ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
        ).limit(1)
        evidence_exists = self.db.execute(evidence_stmt).scalar_one_or_none() is not None

        section_stmt = select(ResumeSectionEmbedding.id).where(
            ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
        ).limit(1)
        section_exists = self.db.execute(section_stmt).scalar_one_or_none() is not None

        return evidence_exists and section_exists

    def get_latest_ready_resume_fingerprint(self) -> Optional[str]:
        stmt = select(ResumeProcessingState).where(
            ResumeProcessingState.processing_status == RESUME_PROCESSING_READY
        ).order_by(
            ResumeProcessingState.embedding_completed_at.desc().nullslast(),
            ResumeProcessingState.updated_at.desc(),
        )

        for state in self.db.execute(stmt).scalars():
            if self.is_resume_ready(state.resume_fingerprint):
                return state.resume_fingerprint
        return None

    def resume_needs_embedding(self, resume_fingerprint: str) -> bool:
        state = self.get_resume_processing_state(resume_fingerprint)
        return state is not None and state.processing_status == "extracted"

    def save_structured_resume(
        self,
        resume_fingerprint: str,
        extracted_data: Dict[str, Any],
        *,
        owner_id: Any = DEFAULT_LEGACY_OWNER_ID,
        total_experience_years: Optional[float] = None,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: Optional[List[str]] = None,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> StructuredResume:
        owner_id = owner_id or DEFAULT_LEGACY_OWNER_ID
        stmt = select(StructuredResume).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        )
        existing = self.db.execute(stmt).scalar_one_or_none()

        if existing:
            existing.owner_id = owner_id
            existing.fingerprint_version = fingerprint_version
            existing.extracted_data = extracted_data
            existing.total_experience_years = total_experience_years
            existing.extraction_confidence = extraction_confidence
            existing.extraction_warnings = extraction_warnings or []
            resume_record = existing
        else:
            resume_record = StructuredResume(
                owner_id=owner_id,
                fingerprint_version=fingerprint_version,
                resume_fingerprint=resume_fingerprint,
                extracted_data=extracted_data,
                total_experience_years=total_experience_years,
                extraction_confidence=extraction_confidence,
                extraction_warnings=extraction_warnings or []
            )
            self.db.add(resume_record)

        self.db.flush()
        return resume_record

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict[str, Any]],
        *,
        owner_id: Any = DEFAULT_LEGACY_OWNER_ID,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> List[ResumeSectionEmbedding]:
        owner_id = owner_id or DEFAULT_LEGACY_OWNER_ID
        self.db.execute(
            delete(ResumeSectionEmbedding).where(
                ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
            )
        )

        records = []
        for section in sections:
            record = ResumeSectionEmbedding(
                owner_id=owner_id,
                fingerprint_version=fingerprint_version,
                resume_fingerprint=resume_fingerprint,
                section_type=section['section_type'],
                section_index=section['section_index'],
                source_text=section['source_text'],
                source_data=section['source_data'],
                embedding=section['embedding']
            )
            self.db.add(record)
            records.append(record)

        self.db.flush()
        return records

    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> List[ResumeSectionEmbedding]:
        """Get resume section embeddings for a fingerprint.

        Args:
            resume_fingerprint: The resume fingerprint
            section_type: Optional filter for specific section type

        Returns:
            List of ResumeSectionEmbedding objects
        """
        stmt = select(ResumeSectionEmbedding).where(
            ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
        )

        if section_type:
            stmt = stmt.where(ResumeSectionEmbedding.section_type == section_type)

        stmt = stmt.order_by(
            ResumeSectionEmbedding.section_type,
            ResumeSectionEmbedding.section_index
        )

        return self.db.execute(stmt).scalars().all()

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[Dict[str, Any]],
        *,
        owner_id: Any = DEFAULT_LEGACY_OWNER_ID,
        fingerprint_version: int = RESUME_FINGERPRINT_VERSION,
    ) -> List[ResumeEvidenceUnitEmbedding]:
        owner_id = owner_id or DEFAULT_LEGACY_OWNER_ID
        self.db.execute(
            delete(ResumeEvidenceUnitEmbedding).where(
                ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
            )
        )

        records = []
        for unit in evidence_units:
            record = ResumeEvidenceUnitEmbedding(
                owner_id=owner_id,
                fingerprint_version=fingerprint_version,
                resume_fingerprint=resume_fingerprint,
                evidence_unit_id=unit['evidence_unit_id'],
                source_text=unit['source_text'],
                source_section=unit.get('source_section'),
                tags=unit.get('tags', {}),
                embedding=unit['embedding'],
                years_value=unit.get('years_value'),
                years_context=unit.get('years_context'),
                is_total_years_claim=unit.get('is_total_years_claim', False),
            )
            self.db.add(record)
            records.append(record)

        self.db.flush()
        return records

    def get_resume_summary_embedding(
        self,
        resume_fingerprint: str
    ) -> Optional[List[float]]:
        """Get the summary section embedding for a resume.
        
        Args:
            resume_fingerprint: The resume fingerprint
            
        Returns:
            List of floats (embedding) or None if not found
        """
        sections = self.get_resume_section_embeddings(resume_fingerprint, section_type='summary')
        if sections and len(sections) > 0 and sections[0].embedding is not None:
            return list(sections[0].embedding)
        return None

    def get_structured_resume_by_fingerprint(
        self,
        resume_fingerprint: str
    ) -> Optional[StructuredResume]:
        """Get structured resume by fingerprint.

        Args:
            resume_fingerprint: Resume fingerprint to look up

        Returns:
            StructuredResume if found, None otherwise
        """
        stmt = select(StructuredResume).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def resume_hash_exists(self, resume_fingerprint: str) -> bool:
        """Check if a resume with the given hash already exists in the database.

        This is used for deduplication - if the hash exists, we can skip
        re-processing the same file.

        Args:
            resume_fingerprint: The file hash to check

        Returns:
            True if a resume with this hash exists, False otherwise
        """
        stmt = select(StructuredResume.resume_fingerprint).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        ).limit(1)
        result = self.db.execute(stmt).scalar_one_or_none()
        return result is not None

    def get_latest_stored_resume_fingerprint(self) -> Optional[str]:
        """Get fingerprint of the most recently stored resume.

        Queries the StructuredResume table ordered by created_at timestamp
        to find the most recently processed resume.

        Returns:
            Resume fingerprint string, or None if no resumes exist in database.
        """
        stmt = select(StructuredResume.resume_fingerprint).order_by(
            StructuredResume.created_at.desc()
        ).limit(1)

        result = self.db.execute(stmt).scalar_one_or_none()
        return result

    def find_best_evidence_for_requirement(
        self,
        requirement_embedding: List[float],
        resume_fingerprint: str,
        top_k: int = 5
    ) -> List[Tuple[ResumeEvidenceUnitEmbedding, float]]:
        distance_expr = ResumeEvidenceUnitEmbedding.embedding.cosine_distance(
            requirement_embedding
        ).label("distance")

        stmt = select(ResumeEvidenceUnitEmbedding, distance_expr).where(
            ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
        ).order_by(distance_expr).limit(top_k)

        rows = self.db.execute(stmt).all()
        return [(row[0], cosine_similarity_from_distance(row._mapping['distance'])) for row in rows]
