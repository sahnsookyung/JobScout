#!/usr/bin/env python3
"""
Resume Profiler - Extract and profile resume data.

Handles:
1. Structured resume extraction using AI
2. Resume evidence unit extraction
3. Section embedding generation
"""
import logging
import json
import threading
from typing import List, Dict, Any, Optional, Iterator

from database.models import generate_resume_fingerprint
from core.llm.interfaces import LLMProvider
from etl.resume.models import ResumeEvidenceUnit
from etl.resume.embedding_store import (
    ResumeSectionEmbeddingStore,
    ResumeEvidenceUnitEmbeddingStore,
)
from core.llm.schema_models import ResumeSchema, Profile

logger = logging.getLogger(__name__)


class ResumeProfiler:
    """
    Service for extracting and profiling resume data.
    Separates resume analysis from job matching logic.
    """

    def __init__(
        self,
        ai_service: LLMProvider,
        store: Optional[ResumeSectionEmbeddingStore | ResumeEvidenceUnitEmbeddingStore] = None
    ):
        self.ai = ai_service
        self.store = store

    def extract_structured_resume(self, resume_data: Dict[str, Any]) -> Optional[ResumeSchema]:
        """Extract comprehensive structured resume data using AI."""
        resume_text = resume_data.get('raw_text') or json.dumps(resume_data, indent=2)
        
        try:
            result = self.ai.extract_resume_data(resume_text)
            if not result or 'profile' not in result:
                logger.warning("Failed to extract structured resume data")
                return None

            resume = ResumeSchema.model_validate(result)
            logger.info(
                f"Extracted resume with {len(resume.profile.experience)} experience entries, "
                f"claimed {resume.claimed_total_years or 'unknown'} years experience"
            )
            return resume

        except Exception as e:
            logger.error(f"Error extracting structured resume: {e}")
            return None

    def extract_resume_evidence(self, profile: Profile) -> List[ResumeEvidenceUnit]:
        """Extract Resume Evidence Units from structured profile."""
        
        def _generate_unit_data() -> Iterator[Dict[str, Any]]:
            # Experience
            for idx, exp in enumerate(profile.experience):
                if exp.description:
                    yield dict(text=exp.description, source="Experience", tags={'company': exp.company or '', 'title': exp.title or '', 'index': idx, 'type': 'description', 'is_current': exp.is_current}, y_val=exp.years_value, y_ctx='experience_at_company' if exp.company else 'experience')
                
                for h in (exp.highlights or []):
                    yield dict(text=h, source="Experience", tags={'company': exp.company or '', 'title': exp.title or '', 'index': idx, 'type': 'highlight', 'is_current': exp.is_current})
                
                for tech in exp.tech_keywords:
                    has_tech = tech in (exp.description or '').lower()
                    yield dict(text=f"Experience with {tech}", source="Experience", tags={'company': exp.company or '', 'title': exp.title or '', 'technology': tech, 'type': 'tech_keyword'}, y_val=exp.years_value if has_tech else None, y_ctx=f'{tech}_experience' if has_tech else None)

            # Projects
            if profile.projects and profile.projects.items:
                for idx, proj in enumerate(profile.projects.items):
                    if proj.description:
                        yield dict(text=proj.description, source="Projects", tags={'project': proj.name or '', 'index': idx, 'type': 'description'})
                    for h in (proj.highlights or []):
                        yield dict(text=h, source="Projects", tags={'project': proj.name or '', 'index': idx, 'type': 'highlight'})

            # Education
            if profile.education:
                for idx, edu in enumerate(profile.education):
                    if edu.description:
                        yield dict(text=edu.description, source="Education", tags={'institution': edu.institution or '', 'degree': edu.degree or '', 'index': idx, 'type': 'description'})
                    for h in (edu.highlights or []):
                        yield dict(text=h, source="Education", tags={'institution': edu.institution or '', 'degree': edu.degree or '', 'index': idx, 'type': 'highlight'})

            # Skills
            for skill in profile.skills.all:
                if skill.name:
                    text = skill.to_embedding_text()
                    text = text.strip() if text and text.strip() else skill.name
                    yield dict(text=text, source="Skills", tags={'skill': skill.name, 'kind': skill.kind or '', 'proficiency': skill.proficiency or '', 'years_experience': skill.years_experience, 'type': 'skill'}, y_val=skill.years_experience, y_ctx=f'{skill.name}_skill')

        # Build objects using enumerate for the ID
        evidence_units = [
            ResumeEvidenceUnit(
                id=f"reu_{i}",
                text=data['text'],
                source_section=data['source'],
                tags=data['tags'],
                years_value=data.get('y_val'),
                years_context=data.get('y_ctx'),
                is_total_years_claim=False
            )
            for i, data in enumerate(_generate_unit_data())
        ]

        logger.info(f"Extracted {len(evidence_units)} evidence units from resume")
        return evidence_units

    def embed_evidence_units(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """Generate embeddings for evidence units in-place."""
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)

    def save_evidence_unit_embeddings(self, resume_fingerprint: str, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """Persist evidence unit embeddings to DB."""
        if not evidence_units or not self.store:
            return

        payload = [
            {
                'evidence_unit_id': u.id,
                'source_text': u.text,
                'source_section': u.source_section,
                'tags': u.tags,
                'embedding': u.embedding,
                'years_value': u.years_value,
                'years_context': u.years_context,
                'is_total_years_claim': u.is_total_years_claim,
            }
            for u in evidence_units if u.embedding is not None
        ]

        if payload:
            self.store.save_evidence_unit_embeddings(resume_fingerprint, payload)
            logger.info(f"Saved {len(payload)} evidence unit embeddings for fingerprint {resume_fingerprint}")

    def save_resume_section_embeddings(self, resume_fingerprint: str, resume: ResumeSchema) -> List[Dict[str, Any]]:
        """Generate and optionally persist embeddings for individual resume sections."""
        sections = []
        profile = resume.profile

        for idx, exp in enumerate(profile.experience):
            if text := exp.to_embedding_text():
                sections.append({'section_type': 'experience', 'section_index': idx, 'source_text': text, 'source_data': exp.model_dump()})

        if text := profile.skills.to_embedding_text():
            sections.append({'section_type': 'skills', 'section_index': 0, 'source_text': text, 'source_data': profile.skills.model_dump()})

        if profile.summary and profile.summary.text:
            sections.append({'section_type': 'summary', 'section_index': 0, 'source_text': profile.summary.text, 'source_data': profile.summary.model_dump()})

        payload = [{**sec, 'embedding': self.ai.generate_embedding(sec['source_text'])} for sec in sections]

        if payload and self.store:
            self.store.save_resume_section_embeddings(resume_fingerprint, payload)
            logger.info(f"Saved {len(payload)} resume section embeddings for fingerprint {resume_fingerprint}")

        return payload

    def _check_interrupted(self, stop_event: Optional[threading.Event]) -> None:
        """Helper to check for early termination."""
        if stop_event and stop_event.is_set():
            logger.info("Resume profiling interrupted (stop event set)")
            raise InterruptedError("Interrupted by system")

    def profile_resume(
        self,
        resume_data: Dict[str, Any],
        stop_event: Optional[threading.Event] = None,
        pre_extracted_resume: Optional[ResumeSchema] = None,
        resume_fingerprint: Optional[str] = None,
    ) -> tuple[Optional[ResumeSchema], List[ResumeEvidenceUnit], List[Dict[str, Any]]]:
        """Complete resume profiling pipeline."""
        if pre_extracted_resume:
            if not resume_fingerprint:
                raise ValueError("resume_fingerprint is required when using pre_extracted_resume")
            resume = pre_extracted_resume
            logger.info("Using pre-extracted resume from storage (skipping LLM extraction)")
        else:
            resume_fingerprint = generate_resume_fingerprint(resume_data)
            self._check_interrupted(stop_event)
            resume = self.extract_structured_resume(resume_data)

        evidence_units, persistence_payload = [], []
        
        if resume:
            self._check_interrupted(stop_event)
            evidence_units = self.extract_resume_evidence(resume.profile)

            self._check_interrupted(stop_event)
            self.embed_evidence_units(evidence_units)
            self.save_evidence_unit_embeddings(resume_fingerprint, evidence_units)

            persistence_payload = self.save_resume_section_embeddings(resume_fingerprint, resume)

        return resume, evidence_units, persistence_payload