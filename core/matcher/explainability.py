#!/usr/bin/env python3
"""
Explainability Module - Resume section to job requirement cross-reference.

Provides detailed breakdown of which resume sections matched which job requirements,
enabling explainable match scores and actionable feedback.

Uses ResumeSectionEmbedding for coarse-grained section-level matching:
- experience: Work experience sections
- projects: Project descriptions
- skills: Skills listings
- summary: Professional summary
- education: Education history
"""

from typing import List, Dict, Any, Optional, Tuple
import logging

from database.models import JobRequirementUnit
from database.repository import JobRepository
from core.utils import cosine_similarity_from_distance

logger = logging.getLogger(__name__)


def calculate_requirement_similarity_with_resume_sections(
    job_requirement: JobRequirementUnit,
    resume_fingerprint: str,
    repo: JobRepository,
    section_types: Optional[List[str]] = None,
    top_k: int = 10
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate similarity between job requirement and resume sections.
    
    Finds the best-matching resume section for each job requirement by searching
    all resume section embeddings (experience, projects, skills, summary).
    
    Args:
        job_requirement: Job requirement with embedding
        resume_fingerprint: Fingerprint of the resume
        repo: JobRepository instance for database access
        section_types: Filter by section type (e.g., ['experience', 'project', 'skill'])
        top_k: Number of top matches to return per requirement
    
    Returns:
        Tuple of (similarity_score, match_details_dict) where match_details includes:
            - similarity: Cosine similarity (0.0-1.0)
            - best_section: Best matching resume section type
            - best_section_index: Index within section type
            - best_section_text: Text of best section
            - all_matches: List of all top matches with their sections
    """
    req_embedding = None
    
    try:
        if hasattr(job_requirement, 'requirement_row') and job_requirement.requirement_row:
            req_embedding = job_requirement.requirement_row.embedding_row.unit.embedding
        elif hasattr(job_requirement, 'embedding_row') and job_requirement.embedding_row:
            req_embedding = job_requirement.embedding_row.unit.embedding
    except AttributeError:
        logger.warning(f"Requirement {getattr(job_requirement, 'id', 'unknown')} has malformed embedding structure")
        return 0.0, {'skipped': True, 'reason': 'Malformed embedding structure'}

    if not req_embedding:
        return 0.0, {'skipped': True, 'reason': 'No embedding'}

    sections = repo.resume.get_resume_section_embeddings(resume_fingerprint, section_type=None)
    all_sections = [s for s in sections if s and s.embedding]

    if section_types:
        all_sections = [s for s in all_sections if s.section_type in section_types]

    if not all_sections:
        return 0.0, {'skipped': True, 'reason': 'No resume sections found'}

    best_section = None
    best_distance = float('inf')
    similarity_scores = []

    for section in all_sections:
        if section.embedding:
            try:
                distance = section.embedding.cosine_distance(req_embedding)
                similarity = cosine_similarity_from_distance(distance)
                similarity_scores.append({
                    'section_type': section.section_type,
                    'section_index': section.section_index,
                    'similarity': similarity,
                    'source_text': section.source_text[:200] if section.source_text else ''
                })
                if distance < best_distance:
                    best_distance = distance
                    best_section = section
            except Exception as e:
                logger.warning(f"Error computing similarity for section: {e}")
                continue

    if not similarity_scores:
        return 0.0, {'skipped': True, 'reason': 'Could not compute similarities'}

    similarity_scores.sort(key=lambda x: x['similarity'], reverse=True)
    best_similarity = similarity_scores[0]['similarity'] if similarity_scores else 0.0

    match_details = {
        'requirement_id': getattr(job_requirement, 'id', 'unknown'),
        'requirement_text': getattr(job_requirement, 'text', '')[:200],
        'similarity': best_similarity,
        'best_section': best_section.section_type if best_section else None,
        'best_section_index': best_section.section_index if best_section else None,
        'best_section_text': best_section.source_text[:200] if best_section and best_section.source_text else None,
        'all_matches': similarity_scores[:top_k]
    }

    return best_similarity, match_details


def explain_match(
    job_requirements: List[JobRequirementUnit],
    resume_fingerprint: str,
    repo: JobRepository
) -> Dict[str, Any]:
    """
    Generate a complete explanation of resume-to-job match.
    
    Args:
        job_requirements: List of job requirements
        resume_fingerprint: Fingerprint of the resume
        repo: JobRepository instance
    
    Returns:
        Dictionary with:
        - per_requirement: List of requirement explanations
        - section_summary: Aggregated scores by section type
        - strengths: List of strongest section matches
        - gaps: List of sections with low match rates
    """
    if not job_requirements:
        return {
            'per_requirement': [],
            'section_summary': {},
            'strengths': [],
            'gaps': [],
            'message': 'No job requirements provided'
        }
    
    requirement_explanations = []
    section_scores: Dict[str, List[float]] = {}

    for req in job_requirements:
        score, details = calculate_requirement_similarity_with_resume_sections(
            job_requirement=req,
            resume_fingerprint=resume_fingerprint,
            repo=repo,
            top_k=3
        )
        
        requirement_explanations.append({
            'requirement_id': getattr(req, 'id', 'unknown'),
            'requirement_text': getattr(req, 'text', '')[:100],
            'similarity': score,
            'details': details
        })
        
        if not details.get('skipped'):
            for match in details.get('all_matches', []):
                section_type = match['section_type']
                if section_type not in section_scores:
                    section_scores[section_type] = []
                section_scores[section_type].append(match['similarity'])

    section_summary = {}
    for section_type, scores in section_scores.items():
        section_summary[section_type] = {
            'avg_similarity': sum(scores) / len(scores) if scores else 0.0,
            'max_similarity': max(scores) if scores else 0.0,
            'requirements_covered': len(scores)
        }

    valid_scores = [(s['similarity'], s['details'].get('best_section')) 
                   for s in requirement_explanations 
                   if s['similarity'] > 0 and s['details'].get('best_section')]
    valid_scores.sort(reverse=True, key=lambda x: x[0])
    strengths = [{'section': sec, 'score': score} for score, sec in valid_scores[:3]]

    sections_with_low_scores = [(sec, data['avg_similarity']) 
                               for sec, data in section_summary.items() 
                               if data['avg_similarity'] < 0.5]
    gaps = [{'section': sec, 'avg_score': score} for sec, score in sections_with_low_scores]

    return {
        'per_requirement': requirement_explanations,
        'section_summary': section_summary,
        'strengths': strengths,
        'gaps': gaps
    }
