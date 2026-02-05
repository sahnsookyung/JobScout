import logging
from typing import List, Optional, Dict, Any
from sqlalchemy import select, delete

from database.models import StructuredResume, ResumeSectionEmbedding, ResumeEvidenceUnitEmbedding, UserWants
from database.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class ResumeRepository(BaseRepository):
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
                embedding=unit['embedding']
            )
            self.db.add(record)
            records.append(record)

        self.db.flush()
        return records

    def get_evidence_unit_embeddings(
        self,
        resume_fingerprint: str
    ) -> List[ResumeEvidenceUnitEmbedding]:
        stmt = select(ResumeEvidenceUnitEmbedding).where(
            ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
        )
        return self.db.execute(stmt).scalars().all()

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

    def find_best_evidence_for_requirement(
        self,
        requirement_embedding: List[float],
        resume_fingerprint: str,
        top_k: int = 5
    ) -> List[tuple[ResumeEvidenceUnitEmbedding, float]]:
        distance_expr = ResumeEvidenceUnitEmbedding.embedding.cosine_distance(
            requirement_embedding
        ).label("distance")

        stmt = select(ResumeEvidenceUnitEmbedding, distance_expr).where(
            ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint
        ).order_by(distance_expr).limit(top_k)

        rows = self.db.execute(stmt).all()
        return [(row.ResumeEvidenceUnitEmbedding, float(row.distance)) for row in rows]

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
