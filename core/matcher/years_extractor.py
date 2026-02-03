#!/usr/bin/env python3
"""
Years Extractor - Extract years of experience using regex + AI.

Two forms:
1. Regex-style extractor (extract_from_text)
2. AI-based extractor (extract_from_evidence)

Uses LLM to understand context and differentiate between total experience
and skill-specific experience claims.
"""
import re
from typing import List, Tuple, Optional
import logging

from core.llm.interfaces import LLMProvider
from core.matcher.models import ResumeEvidenceUnit

from etl.schemas import RESUME_SCHEMA

logger = logging.getLogger(__name__)


class YearsExtractor:
    """Extract years of experience using regex + AI semantic extraction."""
    
    def __init__(self, ai_service: LLMProvider):
        """Initialize years extractor with AI service."""
        self.ai = ai_service
    
    def extract_from_text(self, text: str) -> Tuple[Optional[float], Optional[str], bool]:
        """
        Extract years of experience from text using semantic patterns.
        
        Returns:
            (years_value, years_context, is_total_claim)
        
        Lifted from original matcher_service.py lines 220-268.
        """
        if not text:
            return None, None, False
        
        text_lower = text.lower()
        
        years_patterns = [
            r'(\d+(?:\.\d+)?)\+?\s*years?\s+(?:of\s+)?([^,.;]+)',
            r'(\d+(?:\.\d+)?)\+?\s*yrs?\s+(?:of\s+)?([^,.;]+)',
        ]
        
        total_patterns = [
            r'(\d+(?:\.\d+)?)\+?\s*years?\s+(?:of\s+)?(?:total\s+)?(?:professional\s+)?(?:career\s+)?(?:overall\s+)?experience',
            r'total\s+(?:of\s+)?(\d+(?:\.\d+)?)\+?\s*years?',
            r'over\s+(\d+(?:\.\d+)?)\+?\s*years?\s+(?:of\s+)?(?:professional\s+)?experience',
        ]
        
        for pattern in total_patterns:
            match = re.search(pattern, text_lower)
            if match:
                years = float(match.group(1))
                return years, "total", True
        
        for pattern in years_patterns:
            match = re.search(pattern, text_lower)
            if match:
                years = float(match.group(1))
                context = match.group(2).strip()
                context = re.sub(r'\s+', ' ', context)
                context = re.sub(r'^(?:of|in|with|using)\s+', '', context)
                return years, context, False
        
        return None, None, False
    
    def extract_from_evidence(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """
        Extract years of experience from evidence units using AI semantic extraction.
        
        Uses LLM to understand context and differentiate between total experience
        and skill-specific experience claims.
        
        Lifted from original matcher_service.py lines 270-307.
        """
        for unit in evidence_units:
            try:
                if len(unit.text) < 10:
                    continue
                
                extraction_result = self.ai.extract_structured_data(
                    unit.text,
                    RESUME_SCHEMA
                )
                
                if extraction_result and 'years_claims' in extraction_result:
                    claims = extraction_result['years_claims']
                    if claims:
                        claim = claims[0]
                        years_val = claim.get('years_value')
                        if years_val is not None:
                            try:
                                unit.years_value = float(years_val)
                                unit.years_context = claim.get('context')
                                unit.is_total_years_claim = claim.get('is_total_experience', False)

                                logger.debug(
                                    f"Extracted from evidence {unit.id}: "
                                    f"{unit.years_value} years of {unit.years_context} "
                                    f"(total={unit.is_total_years_claim})"
                                )
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid years_value for evidence {unit.id}: {years_val}")
            
            except Exception as e:
                logger.warning(f"Failed to extract years from evidence {unit.id}: {e}")
