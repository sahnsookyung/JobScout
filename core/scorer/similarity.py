#!/usr/bin/env python3
"""
Similarity Calculations - Multi-embedding similarity matching.

Finds the best-matching resume section for each job requirement by searching
all resume section embeddings (experience, projects, skills, summary).
"""

from typing import List, Dict, Any, Optional, Tuple
import logging

from database.models import ResumeSectionEmbedding
from database.repository import JobRepository
from sqlalchemy import select

logger = logging.getLogger(__name__)


def calculate_requirement_similarity_with_resume_sections(
    job_requirement: Any,
    resume_fingerprint: str,
    repo: JobRepository,
    section_types: Optional[List[str]] = None,
    top_k: int = 5
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate similarity between job requirement and resume sections using multi-embedding matching.

    Finds the best-matching resume section for each job requirement by searching
    all resume section embeddings (experience, projects, skills, summary).
    Uses pgvector's <=> operator (cosine distance) for efficient similarity search.

    Args:
        job_requirement: Job requirement with embedding
        resume_fingerprint: Fingerprint of the resume
        repo: JobRepository instance for database access
        section_types: Filter by section type (e.g., ['experience', 'project', 'skill'])
        top_k: Number of top matches to return per requirement

    Returns:
        Tuple of (similarity_score, match_details_dict) where match_details includes:
            - similarity: Cosine similarity (0.0-1.0)
            - best_section: Best matching resume section
            - best_section_type: Type of best section
            - best_section_text: Text of best section
            - all_matches: List of all top matches with their sections

    Formula:
        - cosine_similarity = (req_embedding · best_section_embedding) / (||req|| * ||best_section||)
        - Uses pgvector's <=> operator which returns 1 - distance (cosine distance)
    """
    # Get requirement embedding
    try:
        req_embedding = job_requirement.requirement_row.embedding_row.unit.embedding
    except AttributeError:
        logger.warning(f"Requirement {getattr(job_requirement, 'id', 'unknown')} has malformed embedding structure, skipping")
        return 0.0, {'skipped': True, 'reason': 'Malformed embedding structure'}

    if not req_embedding:
        logger.warning(f"Requirement {job_requirement.id} has no embedding, skipping")
        return 0.0, {'skipped': True, 'reason': 'No embedding'}

    # Check if pgvector is available by testing the column method
    try:
        # Test that the Vector column supports cosine_distance
        ResumeSectionEmbedding.embedding.cosine_distance
    except AttributeError:
        logger.warning("pgvector Vector column cosine_distance not available, skipping similarity calculation")
        return 0.0, {'skipped': True, 'reason': 'pgvector not available'}

    # Build query to find similar resume sections
    stmt = select(
        ResumeSectionEmbedding,
        ResumeSectionEmbedding.embedding.cosine_distance(req_embedding).label('distance')
    )

    # Filter by resume fingerprint
    stmt = stmt.where(ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint)

    # Optionally filter by section types
    if section_types:
        stmt = stmt.where(ResumeSectionEmbedding.section_type.in_(section_types))

    # Order by distance and limit
    stmt = stmt.order_by('distance').limit(top_k)

    # Execute query
    results = repo.db.execute(stmt).all()

    if not results:
        logger.debug(f"No similar sections found for requirement {job_requirement.id}")
        return 0.0, {'skipped': True, 'reason': 'No matching sections found'}

    # Convert to similarity score (pgvector distance = 1 - cosine_similarity)
    # Distance ranges from 0 (identical) to 2 (opposite)
    # Cosine similarity = 1 - distance
    best_result = results[0]
    similarity_score = 1.0 - best_result.distance

    # Build match details
    all_matches = [
        {
            'section_type': row.ResumeSectionEmbedding.section_type,
            'section_index': row.ResumeSectionEmbedding.section_index,
            'distance': row.distance,
            'similarity': 1.0 - row.distance,
            'source_text': row.ResumeSectionEmbedding.source_text
        }
        for row in results
    ]

    match_details = {
        'similarity': similarity_score,
        'best_section': best_result.ResumeSectionEmbedding.section_type,
        'best_section_index': best_result.ResumeSectionEmbedding.section_index,
        'best_section_text': best_result.ResumeSectionEmbedding.source_text,
        'best_section_type': best_result.ResumeSectionEmbedding.section_type,
        'all_matches': all_matches
    }

    return similarity_score, match_details
