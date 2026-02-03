#!/usr/bin/env python3
"""
Resume Embedding Text Builder - Build composite text for resume-level embedding.

Standardizes evidence slice limit configuration.
"""
from typing import List

from core.matcher.models import ResumeEvidenceUnit


class ResumeEmbeddingTextBuilder:
    """Build resume-level embedding text from evidence units."""
    
    def __init__(self, slice_limit: int = 10):
        """
        Initialize builder.
        
        Args:
            slice_limit: Number of evidence units to include in composite text.
                        Default 10 to preserve two-stage matching behavior.
                        Configured via MatcherConfig.resume_evidence_slice_limit.
        """
        self.slice_limit = slice_limit
    
    def build(self, evidence_units: List[ResumeEvidenceUnit]) -> str:
        """
        Build composite resume text for embedding.
        
        Joins top N evidence unit texts into a single string.
        "Top N" means "first N" due to slice behavior [:N], not "top by score".
        
        Args:
            evidence_units: List of resume evidence units (ordered by extraction)
        
        Returns:
            Composite text string for embedding
        """
        return " ".join([e.text for e in evidence_units[:self.slice_limit]])
