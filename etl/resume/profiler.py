#!/usr/bin/env python3
"""
Resume Profiler - Extract and profile resume data.

Handles:
1. Structured resume extraction using AI
2. Resume evidence unit extraction
3. Section embedding generation

This separates resume profiling concerns from job matching logic.
"""
from typing import List, Dict, Any, Optional
import logging
import json

from database.models import generate_resume_fingerprint
import threading
from core.llm.interfaces import LLMProvider
from etl.resume.models import ResumeEvidenceUnit
from etl.resume.embedding_store import (
    ResumeSectionEmbeddingStore,
    ResumeEvidenceUnitEmbeddingStore,
)
from core.llm.schema_models import (
    ResumeSchema,
    Profile,
)

logger = logging.getLogger(__name__)


class ResumeProfiler:
    """
    Service for extracting and profiling resume data.

    Separates resume analysis from job matching logic.
    Can be used independently for resume parsing and profiling.

    Note: This class no longer directly persists to the database.
    Use the optional store parameter to persist embeddings, or call
    the persistence layer separately.
    """

    def __init__(
        self,
        ai_service: LLMProvider,
        store: Optional[ResumeSectionEmbeddingStore | ResumeEvidenceUnitEmbeddingStore] = None
    ):
        """
        Initialize resume profiler.

        Args:
            ai_service: AI service for extraction and embeddings
            store: Optional store for persistence (if None, no DB writes)
        """
        self.ai = ai_service
        self.store = store

    def extract_structured_resume(
        self,
        resume_data: Dict[str, Any]
    ) -> Optional[ResumeSchema]:
        """
        Extract comprehensive structured resume data using AI.

        Uses RESUME_SCHEMA to extract work history with dates,
        skills, education, and captures claimed years of experience.

        Args:
            resume_data: Raw resume JSON data

        Returns:
            ResumeSchema Pydantic model with structured data, or None if extraction fails
        """
        try:
            resume_text = json.dumps(resume_data, indent=2)

            extraction_result = self.ai.extract_resume_data(resume_text)

            if not extraction_result or 'profile' not in extraction_result:
                logger.warning("Failed to extract structured resume data")
                return None

            # Validate and parse with Pydantic
            resume = ResumeSchema.model_validate(extraction_result)
            
            logger.info(
                f"Extracted resume with {len(resume.profile.experience)} experience entries, "
                f"claimed {resume.claimed_total_years or 'unknown'} years experience"
            )

            return resume

        except Exception as e:
            logger.error(f"Error extracting structured resume: {e}")
            return None

    def extract_resume_evidence(
        self,
        profile: Profile
    ) -> List[ResumeEvidenceUnit]:
        """
        Extract Resume Evidence Units from structured profile.

        Creates evidence units from:
        - Experience descriptions
        - Individual skills with metadata

        Args:
            profile: Structured resume profile from extraction

        Returns:
            List of ResumeEvidenceUnit objects
        """
        evidence_units = []
        unit_id = 0

        # Extract from experience descriptions
        for idx, exp in enumerate(profile.experience):
            if exp.description:
                evidence_units.append(ResumeEvidenceUnit(
                    id=f"reu_{unit_id}",
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
                    is_total_years_claim=False,
                ))
                unit_id += 1

            if exp.highlights:
                for highlight in exp.highlights:
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{unit_id}",
                        text=highlight,
                        source_section="Experience",
                        tags={
                            'company': exp.company or '',
                            'title': exp.title or '',
                            'index': idx,
                            'type': 'highlight',
                            'is_current': exp.is_current
                        },
                        years_value=None,  # Do not double count years from highlights
                        years_context=None,
                        is_total_years_claim=False,
                    ))
                    unit_id += 1

            # Also extract from tech keywords as individual evidence
            for tech in exp.tech_keywords:
                description_lower = (exp.description or '').lower()
                evidence_units.append(ResumeEvidenceUnit(
                    id=f"reu_{unit_id}",
                    text=f"Experience with {tech}",
                    source_section="Experience",
                    tags={
                        'company': exp.company or '',
                        'title': exp.title or '',
                        'technology': tech,
                        'type': 'tech_keyword'
                    },
                    years_value=exp.years_value if tech in description_lower else None,
                    years_context=f'{tech}_experience' if tech in description_lower else None,
                    is_total_years_claim=False,
                ))
                unit_id += 1

        # Extract from projects (if available)
        if profile.projects and profile.projects.items:
            for idx, project in enumerate(profile.projects.items):
                # Description
                if project.description:
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{unit_id}",
                        text=project.description,
                        source_section="Projects",
                        tags={
                            'project': project.name or '',
                            'index': idx,
                            'type': 'description'
                        },
                        years_value=None,
                        years_context=None,
                        is_total_years_claim=False,
                    ))
                    unit_id += 1

                # Highlights
                if project.highlights:
                    for highlight in project.highlights:
                        evidence_units.append(ResumeEvidenceUnit(
                            id=f"reu_{unit_id}",
                            text=highlight,
                            source_section="Projects",
                            tags={
                                'project': project.name or '',
                                'index': idx,
                                'type': 'highlight'
                            },
                            years_value=None,
                            years_context=None,
                            is_total_years_claim=False,
                        ))
                        unit_id += 1

        # Extract from education (if available)
        if profile.education:
            for idx, edu in enumerate(profile.education):
                # Description
                if edu.description:
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{unit_id}",
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
                        is_total_years_claim=False,
                    ))
                    unit_id += 1

                # Highlights
                if edu.highlights:
                    for highlight in edu.highlights:
                        evidence_units.append(ResumeEvidenceUnit(
                            id=f"reu_{unit_id}",
                            text=highlight,
                            source_section="Education",
                            tags={
                                'institution': edu.institution or '',
                                'degree': edu.degree or '',
                                'index': idx,
                                'type': 'highlight'
                            },
                            years_value=None,
                            years_context=None,
                            is_total_years_claim=False,
                        ))
                        unit_id += 1

        # Extract from skills
        for skill in profile.skills.all:
            if skill.name:
                # Compute embedding text and clean whitespace
                text = skill.to_embedding_text()
                if not text or not text.strip():
                    text = skill.name
                else:
                    text = text.strip()

                evidence_units.append(ResumeEvidenceUnit(
                    id=f"reu_{unit_id}",
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
                    is_total_years_claim=False,
                ))
                unit_id += 1

        logger.info(f"Extracted {len(evidence_units)} evidence units from resume")
        return evidence_units

    def embed_evidence_units(
        self,
        evidence_units: List[ResumeEvidenceUnit]
    ) -> None:
        """Generate embeddings for evidence units in-place."""
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[ResumeEvidenceUnit]
    ) -> None:
        """
        Persist evidence unit embeddings to DB.

        Args:
            resume_fingerprint: Unique identifier for the resume
            evidence_units: List of evidence units with embeddings
        """
        if not evidence_units:
            return

        units_with_embeddings = []
        for unit in evidence_units:
            if unit.embedding is not None:
                units_with_embeddings.append({
                    'evidence_unit_id': unit.id,
                    'source_text': unit.text,
                    'source_section': unit.source_section,
                    'tags': unit.tags,
                    'embedding': unit.embedding,
                    'years_value': unit.years_value,
                    'years_context': unit.years_context,
                    'is_total_years_claim': unit.is_total_years_claim,
                })

        if units_with_embeddings and self.store:
            self.store.save_evidence_unit_embeddings(
                resume_fingerprint=resume_fingerprint,
                evidence_units=units_with_embeddings
            )
            logger.info(
                f"Saved {len(units_with_embeddings)} evidence unit embeddings "
                f"for fingerprint {resume_fingerprint}"
            )

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        resume: ResumeSchema
    ) -> List[Dict[str, Any]]:
        """
        Generate embeddings for individual resume sections.

        Creates embeddings for experience, skills, and summary
        to enable granular matching against job requirements.

        If store is configured, persists the embeddings.

        Args:
            resume_fingerprint: Unique identifier for the resume
            resume: Parsed ResumeSchema with structured data

        Returns:
            List of section dictionaries with embedding data (persistence payload)
        """
        sections_to_embed = []
        profile = resume.profile

        # Experience sections
        for idx, exp in enumerate(profile.experience):
            source_text = exp.to_embedding_text()
            if source_text:
                sections_to_embed.append({
                    'section_type': 'experience',
                    'section_index': idx,
                    'source_text': source_text,
                    'source_data': exp.model_dump()
                })

        # Skills section
        skills_text = profile.skills.to_embedding_text()
        if skills_text:
            sections_to_embed.append({
                'section_type': 'skills',
                'section_index': 0,
                'source_text': skills_text,
                'source_data': profile.skills.model_dump()
            })

        # Summary section
        summary = profile.summary
        if summary and summary.text:
            sections_to_embed.append({
                'section_type': 'summary',
                'section_index': 0,
                'source_text': summary.text,
                'source_data': summary.model_dump()
            })

        # Generate embeddings
        sections_with_embeddings = []
        for section in sections_to_embed:
            embedding = self.ai.generate_embedding(section['source_text'])
            sections_with_embeddings.append({
                **section,
                'embedding': embedding
            })

        # Persist if store available
        if self.store and sections_with_embeddings:
            self.store.save_resume_section_embeddings(
                resume_fingerprint=resume_fingerprint,
                sections=sections_with_embeddings
            )
            logger.info(
                f"Saved {len(sections_with_embeddings)} resume section embeddings "
                f"for fingerprint {resume_fingerprint}"
            )

        return sections_with_embeddings

    def profile_resume(
        self,
        resume_data: Dict[str, Any],
        stop_event: Optional[threading.Event] = None
    ) -> tuple[Optional[ResumeSchema], List[ResumeEvidenceUnit], List[Dict[str, Any]]]:
        """
        Complete resume profiling pipeline.

        Extracts structured profile, evidence units, and embeddings.

        Note: Persistence to database only occurs if a store was provided
        to the constructor. Otherwise, the persistence payload is returned
        for the caller to handle.

        Returns:
            Tuple of (ResumeSchema or None, List[ResumeEvidenceUnit], persistence_payload)
            where persistence_payload is a list of section dicts with embeddings (empty if no profile)
        """
        resume_fingerprint = generate_resume_fingerprint(resume_data)

        if stop_event and stop_event.is_set():
            logger.info("Resume profiling interrupted (stop event set)")
            raise InterruptedError("Interrupted by system")

        # Extract structured resume
        resume = self.extract_structured_resume(resume_data)

        # Extract evidence units from structured profile
        evidence_units = []
        if resume:
            if stop_event and stop_event.is_set():
                logger.info("Resume profiling interrupted (stop event set)")
                raise InterruptedError("Interrupted by system")
            evidence_units = self.extract_resume_evidence(resume.profile)

        # Generate embeddings for evidence units
        if stop_event and stop_event.is_set():
            logger.info("Resume profiling interrupted (stop event set)")
            raise InterruptedError("Interrupted by system")
        self.embed_evidence_units(evidence_units)

        # Persist evidence unit embeddings
        self.save_evidence_unit_embeddings(resume_fingerprint, evidence_units)

        # Generate and persist section embeddings
        persistence_payload = []
        if resume:
            persistence_payload = self.save_resume_section_embeddings(resume_fingerprint, resume)

        return resume, evidence_units, persistence_payload
