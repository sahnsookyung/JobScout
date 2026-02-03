#!/usr/bin/env python3
"""
Stage-1 Embedding Builder - Build resume embedding for candidate retrieval.

Supports two modes:
1. Text mode: Concatenate evidence unit texts (legacy)
2. Pooled REU mode: Compute weighted mean of curated REU embeddings (new, default)
"""
from typing import List, Dict, Tuple, Any, Optional
import logging
import math

from core.matcher.models import ResumeEvidenceUnit
from core.config_loader import Stage1EmbeddingConfig
from core.llm.interfaces import LLMProvider

logger = logging.getLogger(__name__)


class Stage1EmbeddingBuilder:
    """Build Stage-1 resume embedding from evidence units."""
    
    def __init__(
        self,
        config: Stage1EmbeddingConfig,
        ai_service: Optional[LLMProvider] = None
    ):
        """
        Initialize builder with configuration.

        Args:
            config: Stage1EmbeddingConfig with mode and weights
            ai_service: Optional AI service for generating embeddings in text mode
        """
        self.config = config
        self.ai_service = ai_service
    
    def build(self, evidence_units: List[ResumeEvidenceUnit]) -> Tuple[List[float], Dict[str, Any]]:
        """
        Build resume embedding for Stage-1 candidate retrieval.
        
        Args:
            evidence_units: List of resume evidence units (should have embeddings populated)
        
        Returns:
            (resume_embedding, build_details)
        
        build_details contains:
        - requested_mode: Mode requested in config
        - actual_mode: Mode actually used
        - fallback_reason: Present only if requested != actual
        - evidence_count: How many REUs were included
        - sections_used: Which sections were included (for pooled mode)
        - section_weights: Weights applied (for pooled mode)
        """
        requested_mode = self.config.mode
        fallback_reason = None
        
        if self.config.mode == "text":
            embedding, details = self._build_from_text_mode(evidence_units)
            if details.get("mode") != "text":
                fallback_reason = details.get("fallback_reason")
            return embedding, self._build_details_with_mode_info(
                requested_mode, details, fallback_reason
            )
        elif self.config.mode == "pooled_reu":
            embedding, details = self._build_from_pooled_mode(evidence_units)
            return embedding, self._build_details_with_mode_info(
                requested_mode, details, fallback_reason
            )
        else:
            logger.warning(f"Unknown Stage-1 embedding mode: {self.config.mode}")
            embedding, details = self._build_from_pooled_mode(evidence_units)
            fallback_reason = "unknown_mode"
            return embedding, self._build_details_with_mode_info(
                requested_mode, details, fallback_reason
            )
    
    def _build_details_with_mode_info(
        self,
        requested_mode: str,
        details: Dict[str, Any],
        fallback_reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """Add mode information to build details."""
        result = {
            "requested_mode": requested_mode,
            "actual_mode": details.get("mode", "pooled_reu"),
            **details
        }
        if fallback_reason is not None:
            result["fallback_reason"] = fallback_reason
        return result
    
    def _build_from_text_mode(self, evidence_units: List[ResumeEvidenceUnit]) -> Tuple[List[float], Dict[str, Any]]:
        """
        Legacy mode: Concatenate first N evidence unit texts.

        This is original behavior for backward compatibility.
        Generates embedding from concatenated text using AI service if available.
        """
        sliced_units = evidence_units[:self.config.text_evidence_slice_limit]
        resume_text = " ".join([e.text for e in sliced_units])

        # Generate embedding from concatenated text
        if self.ai_service:
            try:
                embedding = self.ai_service.generate_embedding(resume_text)
                logger.debug(f"Generated embedding from text mode, dimension: {len(embedding)}")
                details = {
                    "mode": "text",
                    "evidence_count": len(sliced_units),
                    "slice_limit": self.config.text_evidence_slice_limit,
                    "method": "text_embedding",
                    "embedding_dim": len(embedding)
                }
                return embedding, details
            except Exception as e:
                logger.warning(f"Failed to generate embedding in text mode: {e}, falling back to pooled mode")
                fallback_reason = "ai_error"
        else:
            logger.debug("Text mode: AI service not available, falling back to pooled mode")
            fallback_reason = "ai_unavailable"

        # Fall back to pooled mode if AI service not available or generation failed
        embedding, details = self._build_from_pooled_mode(evidence_units)
        details["fallback_reason"] = fallback_reason
        return embedding, details
    
    def _build_from_pooled_mode(self, evidence_units: List[ResumeEvidenceUnit]) -> Tuple[List[float], Dict[str, any]]:
        """
        Pooled REU mode: Compute weighted mean of curated REU embeddings.
        
        This is the new, default mode for Stage-1 embedding.
        Robust to resume ordering and uses embeddings we already need for Stage-2.
        """
        selected_units = self._select_curated_subset(evidence_units)
        
        if not selected_units:
            logger.warning("No curated evidence units found; falling back to all REUs")
            selected_units = evidence_units
        
        if self.config.pooling_method == "weighted_mean":
            resume_embedding = self._weighted_mean_pooling(selected_units)
        elif self.config.pooling_method == "mean":
            resume_embedding = self._mean_pooling(selected_units)
        else:
            logger.warning(f"Unknown pooling method: {self.config.pooling_method}")
            resume_embedding = self._weighted_mean_pooling(selected_units)
        
        resume_embedding = self._l2_normalize(resume_embedding)
        
        sections_used = set(e.source_section for e in selected_units)
        
        # Build weights accounting with both raw and normalized names
        weights_applied = {}
        raw_to_normalized = {}
        for e in selected_units:
            raw_name = e.source_section
            normalized_name = self._normalize_section_name(raw_name)
            weight = self.config.section_weights.get(normalized_name, 1.0)
            weights_applied[normalized_name] = weight
            raw_to_normalized[raw_name] = normalized_name
        
        details = {
            "mode": "pooled_reu",
            "evidence_count": len(selected_units),
            "total_evidence_count": len(evidence_units),
            "sections_used": list(sections_used),
            "pooling_method": self.config.pooling_method,
            "section_weights": weights_applied,
            "raw_to_normalized_mapping": raw_to_normalized,
            "method": "pooled_vector"
        }
        
        logger.info(
            f"Stage-1 embedding: pooled {len(selected_units)} REUs "
            f"from sections {list(sections_used)} using {self.config.pooling_method}"
        )
        
        return resume_embedding, details
    
    def _select_curated_subset(self, evidence_units: List[ResumeEvidenceUnit]) -> List[ResumeEvidenceUnit]:
        """
        Select curated subset of evidence units using section-based metadata.
        
        Deterministic selection based on source_section field only.
        No JD consultation - uses resume-local metadata.
        
        Section inclusion rules:
        - Always include: Summary, Skills, Professional Experience
        - Default exclude/heavily down-weight: Education, Projects
        - Configurable via section_weights
        
        Section name normalization maps:
        - "summary" -> "Summary"
        - "skills" -> "Skills"  
        - "experience" / "professional experience" / "work experience" -> "experience"
        - "projects" -> "projects"
        - "education" -> "education"
        
        Args:
            evidence_units: All resume evidence units
        
        Returns:
            Curated subset of evidence units for pooling
        """
        selected = []
        
        for unit in evidence_units:
            section_name = unit.source_section.lower()
            normalized_name = self._normalize_section_name(section_name)
            weight = self.config.section_weights.get(normalized_name, 0.0)
            
            if weight > 0:
                selected.append(unit)
                logger.debug(f"Selected REU from '{section_name}' (weight: {weight})")
            else:
                logger.debug(f"Excluded REU from '{section_name}' (weight: {weight})")
        
        return selected
    
    def _normalize_section_name(self, section_name: str) -> str:
        """
        Normalize section names for matching against config weights.
        
        Maps common variations to canonical names.
        """
        section_lower = section_name.lower().strip()
        
        mapping = {
            "summary": ["summary", "summary section", "about", "profile"],
            "skills": ["skills", "skills section", "technical skills", "skill groups"],
            "experience": ["experience", "experience section", "professional experience", "work experience", "work history"],
            "projects": ["projects", "project section", "portfolio"],
            "education": ["education", "education section", "academic", "degrees"]
        }
        
        for canonical_name, variations in mapping.items():
            if section_lower in variations:
                return canonical_name
        
        return section_lower
    
    def _determine_embedding_dimension(self, evidence_units: List[ResumeEvidenceUnit]) -> int:
        """
        Determine embedding dimension from evidence units or config.
        
        Args:
            evidence_units: Evidence units to check for embeddings
        
        Returns:
            Embedding dimension
        
        Raises:
            ValueError: If dimension cannot be determined
        """
        for unit in evidence_units:
            if unit.embedding is not None:
                return len(unit.embedding)
        
        if hasattr(self.config, 'embedding_dim'):
            return self.config.embedding_dim
        
        raise ValueError("Cannot determine embedding dimension: no non-None embeddings found and no embedding_dim configured")
    
    def _weighted_mean_pooling(self, evidence_units: List[ResumeEvidenceUnit]) -> List[float]:
        """
        Compute weighted mean pooling over evidence unit embeddings.
        
        Formula: r = (sum w_i * v_i) / sum w_i)
        
        Args:
            evidence_units: Evidence units with embeddings populated
        
        Returns:
            Weighted mean vector
        """
        dimension = self._determine_embedding_dimension(evidence_units)
        
        if not evidence_units:
            return [0.0] * dimension
        
        weights = []
        vectors = []
        
        for unit in evidence_units:
            if unit.embedding is None:
                logger.warning(f"REU {unit.id} has no embedding; skipping in pooling")
                continue
            
            section_name = self._normalize_section_name(unit.source_section)
            weight = self.config.section_weights.get(section_name, 1.0)
            
            weights.append(weight)
            vectors.append(unit.embedding)
        
        if not vectors:
            return [0.0] * dimension
        
        dimension = len(vectors[0])
        result = [0.0] * dimension
        
        for d in range(dimension):
            weighted_sum = 0.0
            weight_sum = 0.0
            for i, (vec, weight) in enumerate(zip(vectors, weights)):
                weighted_sum += vec[d] * weight
                weight_sum += weight
            
            result[d] = weighted_sum / weight_sum if weight_sum > 0 else 0.0
        
        return result
    
    def _mean_pooling(self, evidence_units: List[ResumeEvidenceUnit]) -> List[float]:
        """
        Compute simple mean pooling over evidence unit embeddings.
        
        Formula: r = (1/n) * sum v_i
        
        Args:
            evidence_units: Evidence units with embeddings populated
        
        Returns:
            Mean pooled vector
        """
        dimension = self._determine_embedding_dimension(evidence_units)
        
        if not evidence_units:
            return [0.0] * dimension
        
        vectors = [unit.embedding for unit in evidence_units if unit.embedding is not None]
        
        if not vectors:
            return [0.0] * dimension
        
        dimension = len(vectors[0])
        result = [0.0] * dimension
        n = len(vectors)
        
        for d in range(dimension):
            sum_d = sum(vec[d] for vec in vectors)
            result[d] = sum_d / n
        
        return result
    
    def _l2_normalize(self, vector: List[float]) -> List[float]:
        """
        L2 normalize vector for cosine similarity stability.
        
        Formula: v_normalized = v / ||v||
        
        Args:
            vector: Vector to normalize
        
        Returns:
            L2 normalized vector
        """
        norm = math.sqrt(sum(v * v for v in vector))
        
        if norm == 0:
            return [0.0] * len(vector)
        
        return [v / norm for v in vector]
