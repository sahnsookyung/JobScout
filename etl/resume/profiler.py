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

from core.llm.interfaces import LLMProvider
from etl.resume.models import ResumeEvidenceUnit
from etl.resume.embedding_store import (
    ResumeSectionEmbeddingStore,
    ResumeEvidenceUnitEmbeddingStore,
)
from core.llm.schema_models import ResumeSchema, Profile

logger = logging.getLogger(__name__)
DEFAULT_LEGACY_OWNER_ID = "00000000-0000-0000-0000-000000000001"


def _normalize_owner_id(owner_id: Any) -> Any:
    return owner_id or DEFAULT_LEGACY_OWNER_ID


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
        evidence_units = []
        unit_id = 0

        # Experience section
        for unit in self._extract_experience_evidence(profile.experience):
            unit.id = f"reu_{unit_id}"
            evidence_units.append(unit)
            unit_id += 1

        # Projects section
        for unit in self._extract_project_evidence(profile.projects):
            unit.id = f"reu_{unit_id}"
            evidence_units.append(unit)
            unit_id += 1

        # Education section
        for unit in self._extract_education_evidence(profile.education):
            unit.id = f"reu_{unit_id}"
            evidence_units.append(unit)
            unit_id += 1

        # Skills section
        for unit in self._extract_skill_evidence(profile.skills):
            unit.id = f"reu_{unit_id}"
            evidence_units.append(unit)
            unit_id += 1

        logger.info("Extracted %d evidence units from resume", len(evidence_units))
        return evidence_units

    def _create_experience_description_unit(self, exp, idx: int) -> ResumeEvidenceUnit:
        """Create evidence unit from experience description."""
        return ResumeEvidenceUnit(
            id="",
            text=exp.description,
            source_section="Experience",
            tags={
                'company': exp.company or '',
                'title': exp.title or '',
                'index': idx,
                'type': 'description',
                'is_current': exp.is_current
            },
            years_value=exp.years_value,
            years_context='experience_at_company' if exp.company else 'experience',
            is_total_years_claim=False
        )

    def _create_experience_highlight_unit(self, exp, idx: int, highlight: str) -> ResumeEvidenceUnit:
        """Create evidence unit from experience highlight."""
        return ResumeEvidenceUnit(
            id="",
            text=highlight,
            source_section="Experience",
            tags={
                'company': exp.company or '',
                'title': exp.title or '',
                'index': idx,
                'type': 'highlight',
                'is_current': exp.is_current
            },
            years_value=None,
            years_context=None,
            is_total_years_claim=False
        )

    def _create_experience_tech_unit(self, exp, idx: int, tech: str) -> ResumeEvidenceUnit:
        """Create evidence unit from experience tech keyword."""
        del idx  # Unused parameter - kept for API consistency
        has_tech = tech in (exp.description or '').lower()
        return ResumeEvidenceUnit(
            id="",
            text=f"Experience with {tech}",
            source_section="Experience",
            tags={
                'company': exp.company or '',
                'title': exp.title or '',
                'technology': tech,
                'type': 'tech_keyword'
            },
            years_value=exp.years_value if has_tech else None,
            years_context=f'{tech}_experience' if has_tech else None,
            is_total_years_claim=False
        )

    def _extract_experience_evidence(self, experience: list) -> Iterator[ResumeEvidenceUnit]:
        """Extract evidence units from experience section."""
        for idx, exp in enumerate(experience):
            if exp.description:
                yield self._create_experience_description_unit(exp, idx)

            for h in (exp.highlights or []):
                yield self._create_experience_highlight_unit(exp, idx, h)

            for tech in (exp.tech_keywords or []):
                yield self._create_experience_tech_unit(exp, idx, tech)

    def _extract_project_evidence(self, projects) -> Iterator[ResumeEvidenceUnit]:
        """Extract evidence units from projects section."""
        if not projects or not projects.items:
            return

        for idx, proj in enumerate(projects.items):
            if proj.description:
                yield ResumeEvidenceUnit(
                    id="",
                    text=proj.description,
                    source_section="Projects",
                    tags={
                        'project': proj.name or '',
                        'index': idx,
                        'type': 'description'
                    },
                    years_value=None,
                    years_context=None,
                    is_total_years_claim=False
                )

            for h in (proj.highlights or []):
                yield ResumeEvidenceUnit(
                    id="",
                    text=h,
                    source_section="Projects",
                    tags={
                        'project': proj.name or '',
                        'index': idx,
                        'type': 'highlight'
                    },
                    years_value=None,
                    years_context=None,
                    is_total_years_claim=False
                )

    def _extract_education_evidence(self, education: list) -> Iterator[ResumeEvidenceUnit]:
        """Extract evidence units from education section."""
        if not education:
            return

        for idx, edu in enumerate(education):
            if edu.description:
                yield ResumeEvidenceUnit(
                    id="",
                    text=edu.description,
                    source_section="Education",
                    tags={
                        'institution': edu.institution or '',
                        'degree': edu.degree or '',
                        'index': idx,
                        'type': 'description'
                    },
                    years_value=None,
                    years_context=None,
                    is_total_years_claim=False
                )

            for h in (edu.highlights or []):
                yield ResumeEvidenceUnit(
                    id="",
                    text=h,
                    source_section="Education",
                    tags={
                        'institution': edu.institution or '',
                        'degree': edu.degree or '',
                        'index': idx,
                        'type': 'highlight'
                    },
                    years_value=None,
                    years_context=None,
                    is_total_years_claim=False
                )

    def _extract_skill_evidence(self, skills) -> Iterator[ResumeEvidenceUnit]:
        """Extract evidence units from skills section."""
        if not skills:
            return

        for skill in skills.all:
            if skill.name:
                text = skill.to_embedding_text()
                text = text.strip() if text and text.strip() else skill.name
                yield ResumeEvidenceUnit(
                    id="",
                    text=text,
                    source_section="Skills",
                    tags={
                        'skill': skill.name,
                        'kind': skill.kind or '',
                        'proficiency': skill.proficiency or '',
                        'years_experience': skill.years_experience,
                        'type': 'skill'
                    },
                    years_value=skill.years_experience,
                    years_context=f'{skill.name}_skill',
                    is_total_years_claim=False
                )

    def embed_evidence_units(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """Generate embeddings for evidence units in-place."""
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[ResumeEvidenceUnit],
        *,
        owner_id: Any = None,
    ) -> None:
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
            self.store.save_evidence_unit_embeddings(
                resume_fingerprint,
                payload,
                owner_id=_normalize_owner_id(owner_id),
            )
            logger.info(f"Saved {len(payload)} evidence unit embeddings for fingerprint {resume_fingerprint}")

    def _build_experience_section_payloads(self, profile: Profile) -> List[Dict[str, Any]]:
        """Create section payloads for experience entries with embedding text."""
        sections = []
        for idx, exp in enumerate(profile.experience):
            text = exp.to_embedding_text()
            if text:
                sections.append(
                    {
                        'section_type': 'experience',
                        'section_index': idx,
                        'source_text': text,
                        'source_data': exp.model_dump(),
                    }
                )
        return sections

    def _build_summary_text(self, profile: Profile) -> Optional[str]:
        """Derive summary text from the structured summary or fallback fields."""
        if profile.summary and profile.summary.text:
            return profile.summary.text

        summary_parts = []
        for exp in profile.experience[:2]:
            text = exp.to_embedding_text()
            if text:
                summary_parts.append(text)

        skills_text = profile.skills.to_embedding_text()
        if skills_text:
            summary_parts.append(skills_text)

        return " | ".join(summary_parts) if summary_parts else None

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        resume: ResumeSchema,
        *,
        owner_id: Any = None,
    ) -> List[Dict[str, Any]]:
        """Generate and optionally persist embeddings for individual resume sections."""
        profile = resume.profile
        sections = self._build_experience_section_payloads(profile)

        skills_text = profile.skills.to_embedding_text()
        if skills_text:
            sections.append(
                {
                    'section_type': 'skills',
                    'section_index': 0,
                    'source_text': skills_text,
                    'source_data': profile.skills.model_dump(),
                }
            )

        summary_text = self._build_summary_text(profile)
        if summary_text:
            sections.append({
                'section_type': 'summary',
                'section_index': 0,
                'source_text': summary_text,
                'source_data': profile.summary.model_dump() if profile.summary else {},
            })

        payload = [{**sec, 'embedding': self.ai.generate_embedding(sec['source_text'])} for sec in sections]

        if payload and self.store:
            self.store.save_resume_section_embeddings(
                resume_fingerprint,
                payload,
                owner_id=_normalize_owner_id(owner_id),
            )
            logger.info(f"Saved {len(payload)} resume section embeddings for fingerprint {resume_fingerprint}")

        return payload

    def _check_interrupted(self, stop_event: Optional[threading.Event]) -> None:
        """Helper to check for early termination."""
        if stop_event and stop_event.is_set():
            logger.info("Resume profiling interrupted (stop event set)")
            raise InterruptedError("Interrupted by system")

    def extract_only(self, resume_data: Dict[str, Any]) -> Optional[ResumeSchema]:
        """Extract structured resume data only (no embeddings).
        
        Args:
            resume_data: Raw resume data dict (from parser)
            
        Returns:
            ResumeSchema if extraction successful, None otherwise
        """
        return self.extract_structured_resume(resume_data)

    def embed_only(
        self,
        resume_fingerprint: str,
        resume: ResumeSchema,
        stop_event: Optional[threading.Event] = None,
        *,
        owner_id: Any = None,
    ) -> List[ResumeEvidenceUnit]:
        """Generate embeddings for already-extracted resume.

        Args:
            resume_fingerprint: Resume fingerprint for storage
            resume: Already-extracted ResumeSchema
            stop_event: Optional event to signal interruption

        Returns:
            List of ResumeEvidenceUnit with embeddings generated
        """
        if not resume_fingerprint:
            raise ValueError("resume_fingerprint is required")
        self._check_interrupted(stop_event)
        evidence_units = self.extract_resume_evidence(resume.profile)

        self._check_interrupted(stop_event)
        self.embed_evidence_units(evidence_units)
        self.save_evidence_unit_embeddings(
            resume_fingerprint,
            evidence_units,
            owner_id=owner_id,
        )

        # Skip interruption check between saves to minimize partial persistence risk
        # Note: These saves are not atomic - partial data may occur if crash happens between calls
        self.save_resume_section_embeddings(
            resume_fingerprint,
            resume,
            owner_id=owner_id,
        )

        return evidence_units

    def profile_resume(
        self,
        resume_data: Dict[str, Any],
        resume_fingerprint: str,
        stop_event: Optional[threading.Event] = None,
        pre_extracted_resume: Optional[ResumeSchema] = None,
        *,
        owner_id: Any = None,
    ) -> tuple[Optional[ResumeSchema], List[ResumeEvidenceUnit], List[Dict[str, Any]]]:
        """Complete resume profiling pipeline."""
        if pre_extracted_resume:
            resume = pre_extracted_resume
            logger.info("Using pre-extracted resume from storage (skipping LLM extraction)")
        else:
            if not resume_fingerprint:
                raise ValueError("resume_fingerprint is required when processing resume")
            self._check_interrupted(stop_event)
            resume = self.extract_structured_resume(resume_data)

        evidence_units, persistence_payload = [], []
        
        if resume:
            self._check_interrupted(stop_event)
            evidence_units = self.extract_resume_evidence(resume.profile)

            self._check_interrupted(stop_event)
            self.embed_evidence_units(evidence_units)
            self.save_evidence_unit_embeddings(
                resume_fingerprint,
                evidence_units,
                owner_id=owner_id,
            )

            persistence_payload = self.save_resume_section_embeddings(
                resume_fingerprint,
                resume,
                owner_id=owner_id,
            )

        return resume, evidence_units, persistence_payload
