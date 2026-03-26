import logging
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, delete

from database.models import (
    StructuredResume,
    ResumeSectionEmbedding,
    ResumeEvidenceUnitEmbedding,
    ResumeProcessingState,
    RESUME_PROCESSING_READY,
    UserWants,
)
from database.repositories.base import BaseRepository
from core.utils import cosine_similarity_from_distance

logger = logging.getLogger(__name__)


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

    def set_resume_processing_state(
        self,
        resume_fingerprint: str,
        status: str,
        error: Optional[str] = None,
        extraction_completed_at: Optional[Any] = None,
        embedding_completed_at: Optional[Any] = None,
    ) -> ResumeProcessingState:
        state = self.get_resume_processing_state(resume_fingerprint)
        if state is None:
            state = ResumeProcessingState(
                resume_fingerprint=resume_fingerprint,
                processing_status=status,
            )
            self.db.add(state)

        state.processing_status = status
        state.last_error = error
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
        total_experience_years: Optional[float] = None,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: Optional[List[str]] = None
    ) -> StructuredResume:
        stmt = select(StructuredResume).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        )
        existing = self.db.execute(stmt).scalar_one_or_none()

        if existing:
            existing.extracted_data = extracted_data
            existing.total_experience_years = total_experience_years
            existing.extraction_confidence = extraction_confidence
            existing.extraction_warnings = extraction_warnings or []
            resume_record = existing
        else:
            resume_record = StructuredResume(
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
        sections: List[Dict[str, Any]]
    ) -> List[ResumeSectionEmbedding]:
        self.db.execute(
            delete(ResumeSectionEmbedding).where(
                ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
            )
        )

        records = []
        for section in sections:
            record = ResumeSectionEmbedding(
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
        evidence_units: List[Dict[str, Any]]
    ) -> List[ResumeEvidenceUnitEmbedding]:
        self.db.execute(
            delete(ResumeEvidenceUnitEmbedding).where(
                ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
            )
        )

        records = []
        for unit in evidence_units:
            record = ResumeEvidenceUnitEmbedding(
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
