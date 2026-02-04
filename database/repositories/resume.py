import logging
from typing import List, Optional, Dict, Any
from sqlalchemy import select, delete

from database.models import StructuredResume, ResumeSectionEmbedding, UserWants
from database.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class ResumeRepository(BaseRepository):
    def save_structured_resume(
        self,
        resume_fingerprint: str,
        extracted_data: Dict[str, Any],
        calculated_total_years: Optional[float],
        claimed_total_years: Optional[float],
        experience_validated: bool,
        validation_message: str,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: Optional[List[str]] = None
    ) -> StructuredResume:
        stmt = select(StructuredResume).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        )
        existing = self.db.execute(stmt).scalar_one_or_none()

        if existing:
            existing.extracted_data = extracted_data
            existing.calculated_total_years = calculated_total_years
            existing.claimed_total_years = claimed_total_years
            existing.experience_validated = experience_validated
            existing.validation_message = validation_message
            existing.extraction_confidence = extraction_confidence
            existing.extraction_warnings = extraction_warnings or []
            resume_record = existing
        else:
            resume_record = StructuredResume(
                resume_fingerprint=resume_fingerprint,
                extracted_data=extracted_data,
                calculated_total_years=calculated_total_years,
                claimed_total_years=claimed_total_years,
                experience_validated=experience_validated,
                validation_message=validation_message,
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

    def save_user_wants(
        self,
        user_id: str,
        resume_fingerprint: Optional[str],
        wants_text: str,
        embedding: List[float],
        facet_key: Optional[str] = None
    ) -> UserWants:
        user_want = UserWants(
            user_id=user_id,
            resume_fingerprint=resume_fingerprint,
            wants_text=wants_text,
            embedding=embedding,
            facet_key=facet_key
        )
        self.db.add(user_want)
        return user_want

    def get_user_wants_embeddings(
        self,
        user_id: str,
        resume_fingerprint: Optional[str] = None
    ) -> List[List[float]]:
        stmt = select(UserWants.embedding).where(UserWants.user_id == user_id)
        if resume_fingerprint:
            stmt = stmt.where(UserWants.resume_fingerprint == resume_fingerprint)
        results = self.db.execute(stmt).scalars().all()
        return list(results)
