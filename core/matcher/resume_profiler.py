#!/usr/bin/env python3
"""
Resume Profiler - Extract and profile resume data.
 
Handles:
1. Structured resume extraction using AI
2. Resume evidence unit extraction
3. Years of experience extraction and validation
4. Section embedding generation
 
This separates resume profiling concerns from job matching logic.
"""
from typing import List, Dict, Any, Optional
import logging
import json

from database.models import generate_resume_fingerprint
from core.llm.interfaces import LLMProvider
from core.config_loader import MatcherConfig
from core.matcher.models import (
    ResumeEvidenceUnit,
    StructuredResumeProfile,
)
from core.matcher.years_extractor import YearsExtractor
from core.matcher.embedding_store import ResumeSectionEmbeddingStore
from etl.schemas import RESUME_SCHEMA

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
        config: Optional[MatcherConfig] = None,
        store: Optional[ResumeSectionEmbeddingStore] = None
    ):
        """
        Initialize resume profiler.
        
        Args:
            ai_service: AI service for extraction and embeddings
            config: Optional matcher configuration
            store: Optional store for persistence (if None, no DB writes)
        """
        self.ai = ai_service
        self.config = config or MatcherConfig()
        self.store = store
        self.years_extractor = YearsExtractor(ai_service)
    
    def extract_structured_resume(
        self,
        resume_data: Dict[str, Any]
    ) -> Optional[StructuredResumeProfile]:
        """
        Extract comprehensive structured resume data using AI.
        
        Uses RESUME_FULL_SCHEMA to extract work history with dates,
        skills, education, and calculates total years of experience.
        
        Args:
            resume_data: Raw resume JSON data
            
        Returns:
            StructuredResumeProfile with calculated experience, or None if extraction fails
        """
        try:
            # Convert resume data to text for extraction
            resume_text = json.dumps(resume_data, indent=2)
            
            # Use AI to extract structured data
            extraction_result = self.ai.extract_structured_data(
                resume_text,
                RESUME_SCHEMA
            )
            
            if not extraction_result or 'profile' not in extraction_result:
                logger.warning("Failed to extract structured resume data")
                return None
            
            profile_data = extraction_result['profile']
            
            # Build the structured profile
            profile = StructuredResumeProfile(
                raw_data=extraction_result,
                experience_entries=profile_data.get('experience', []),
                claimed_total_years=profile_data.get('summary', {}).get('claimed_total_experience_years')
            )
            
            # Calculate years from date ranges
            profile.calculated_total_years = profile.calculate_experience_from_dates()
            
            # Validate the claim
            is_valid, validation_msg = profile.validate_experience_claim()
            if not is_valid:
                logger.warning(f"Resume experience claim validation failed: {validation_msg}")
            else:
                logger.info(f"Resume experience: {validation_msg}")
            
            return profile
            
        except Exception as e:
            logger.error(f"Error extracting structured resume: {e}")
            return None
    
    def extract_resume_evidence(
        self,
        resume_data: Dict[str, Any]
    ) -> List[ResumeEvidenceUnit]:
        """
        Extract Resume Evidence Units from resume JSON.
        
        Parses resume sections and creates evidence units from
        descriptions and highlights.
        """
        evidence_units = []
        unit_id = 0
        
        for section in resume_data.get('sections', []):
            section_title = section.get('title', '')
            
            for item in section.get('items', []):
                # Extract from description
                if item.get('description'):
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{unit_id}",
                        text=item['description'],
                        source_section=section_title,
                        tags={
                            'company': item.get('company', ''),
                            'role': item.get('role', ''),
                            'period': item.get('period', ''),
                            'type': 'description'
                        }
                    ))
                    unit_id += 1
                
                # Extract from highlights
                for highlight in item.get('highlights', []):
                    if highlight and not highlight.startswith('<'):
                        evidence_units.append(ResumeEvidenceUnit(
                            id=f"reu_{unit_id}",
                            text=highlight,
                            source_section=section_title,
                            tags={
                                'company': item.get('company', ''),
                                'role': item.get('role', ''),
                                'type': 'highlight'
                            }
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
    
    def extract_years_from_evidence(
        self,
        evidence_units: List[ResumeEvidenceUnit]
    ) -> None:
        """
        Extract years values from evidence units using regex + AI.
        
        Modifies evidence units in-place with years_value, years_context,
        and is_total_years_claim fields.
        """
        for unit in evidence_units:
            try:
                years_value, years_context, is_total = self.years_extractor.extract_from_text(
                    unit.text
                )
                
                if years_value is not None:
                    unit.years_value = years_value
                    unit.years_context = years_context
                    unit.is_total_years_claim = is_total
                    
                    if is_total:
                        logger.debug(
                            f"Extracted years from evidence {unit.id}: "
                            f"{unit.years_value} years of {unit.years_context} "
                            f"(total={unit.is_total_years_claim})"
                        )
                        
            except Exception as e:
                logger.warning(f"Failed to extract years from evidence {unit.id}: {e}")
                # Continue with other evidence units
    
    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        profile: StructuredResumeProfile
    ) -> List[Dict[str, Any]]:
        """
        Generate embeddings for individual resume sections.
        
        Creates embeddings for experience, projects, skills, and summary
        to enable granular matching against job requirements.
        
        If store is configured, persists the embeddings.
        
        Returns:
            List of section dictionaries with embedding data (persistence payload)
        """
        sections_to_embed = []
        profile_data = profile.raw_data.get('profile', {})
        
        # Embed experience entries
        for idx, experience in enumerate(profile_data.get('experience', [])):
            if not experience:
                continue
            source_text = f"{experience.get('company', '')} - {experience.get('title', '')}"
            if experience.get('description'):
                source_text += f": {experience['description']}"
            if experience.get('highlights'):
                source_text += f" | {' | '.join(experience['highlights'][:3])}"
            
            sections_to_embed.append({
                'section_type': 'experience',
                'section_index': idx,
                'source_text': source_text,
                'source_data': experience
            })
        
        # Embed projects
        for idx, project in enumerate(profile_data.get('projects', [])):
            if not project:
                continue
            source_text = project.get('name', 'Project')
            if project.get('description'):
                source_text += f": {project['description']}"
            if project.get('highlights'):
                source_text += f" | {' | '.join(project['highlights'][:3])}"
            
            sections_to_embed.append({
                'section_type': 'project',
                'section_index': idx,
                'source_text': source_text,
                'source_data': project
            })
        
        # Embed skills
        for idx, skill_group in enumerate(profile_data.get('skills', {}).get('groups', [])):
            if not skill_group:
                continue
            skills_list = skill_group.get('skills', [])
            skill_name = skill_group.get('name', 'Skills')
            source_text = f"{skill_name}: {', '.join(skills_list[:5])}"
            
            sections_to_embed.append({
                'section_type': 'skill',
                'section_index': idx,
                'source_text': source_text,
                'source_data': skill_group
            })
        
        # Embed summary
        summary = profile_data.get('summary', {})
        if summary:
            source_text = summary.get('headline', '')
            if summary.get('objective'):
                source_text += f" | {summary['objective']}"
            
            sections_to_embed.append({
                'section_type': 'summary',
                'section_index': 0,
                'source_text': source_text,
                'source_data': summary
            })
        
        # Generate embeddings
        sections_with_embeddings = []
        for section in sections_to_embed:
            embedding = self.ai.generate_embedding(section['source_text'])
            sections_with_embeddings.append({
                **section,
                'embedding': embedding
            })
        
        # Optionally persist if store is configured
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
        resume_data: Dict[str, Any]
    ) -> tuple[Optional[StructuredResumeProfile], List[ResumeEvidenceUnit], List[Dict[str, Any]]]:
        """
        Complete resume profiling pipeline.
        
        Extracts structured profile, evidence units, years, and embeddings.
        
        Note: Persistence to database only occurs if a store was provided
        to the constructor. Otherwise, the persistence payload is returned
        for the caller to handle.
        
        Returns:
            Tuple of (StructuredResumeProfile or None, List[ResumeEvidenceUnit], persistence_payload)
            where persistence_payload is a list of section dicts with embeddings (empty if no profile)
        """
        # Extract structured profile
        profile = self.extract_structured_resume(resume_data)
        
        # Extract evidence units
        evidence_units = self.extract_resume_evidence(resume_data)
        
        # Extract years from evidence
        self.extract_years_from_evidence(evidence_units)
        
        # Generate embeddings for evidence
        self.embed_evidence_units(evidence_units)
        
        # Generate section embeddings and optionally persist
        persistence_payload = []
        if profile:
            resume_fingerprint = generate_resume_fingerprint(resume_data)
            persistence_payload = self.save_resume_section_embeddings(resume_fingerprint, profile)
        
        return profile, evidence_units, persistence_payload
