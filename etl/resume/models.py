#!/usr/bin/env python3
"""
Resume Models - Data structures for resume extraction and profiling.

Note: Structured resume data now uses Pydantic models from etl.schema_models.
This file contains only internal data structures not derived from JSON schemas.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class ResumeEvidenceUnit:
    """
    Resume Evidence Unit - atomic claim from resume.
    
    Represents a single piece of evidence from a resume that can be matched
    against job requirements. Each unit has text content, metadata about its
    source, and optionally an embedding vector.
    """
    id: str  # Unique identifier for this evidence unit
    text: str  # The actual evidence text
    source_section: str  # Which resume section this came from (e.g., "Experience", "Skills")
    tags: Dict[str, Any] = field(default_factory=dict)  # Metadata (company, role, skill, etc.)
    embedding: Optional[List[float]] = None  # Vector embedding for similarity matching
    years_value: Optional[float] = None  # Extracted years of experience (if applicable)
    years_context: Optional[str] = None  # What the years refer to (e.g., "Python", "total experience")
    is_total_years_claim: bool = False  # Whether this represents a total years claim
