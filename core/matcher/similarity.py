#!/usr/bin/env python3
"""
Similarity Calculator - Cosine similarity between vectors.
"""
import math
from typing import List


class SimilarityCalculator:
    """Calculate cosine similarity between vectors."""
    
    @staticmethod
    def calculate(vec1: List[float], vec2: List[float]) -> float:
        """
        Calculate normalized cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
        
        Returns:
            Normalized cosine similarity (0.0 to 1.0), or 0.0 if either vector is zero
        """
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        raw_cosine = dot_product / (norm1 * norm2)
        normalized = (raw_cosine + 1.0) / 2.0
        
        return max(0.0, min(1.0, normalized))
