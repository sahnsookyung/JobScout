#!/usr/bin/env python3
"""
Years Extractor - Extract years of experience from resume text.

Uses regex patterns + AI fallback to extract years values from text.
"""
from typing import Optional, Tuple
import re
import logging

from core.llm.interfaces import LLMProvider

logger = logging.getLogger(__name__)


class YearsExtractor:
    """
    Extract years of experience values from text using regex + AI.
    """

    YEARS_PATTERN = re.compile(
        r'(?:(\d+(?:\.\d+)?)\s*(?:-|to)\s*(\d+(?:\.\d+)?))?\s*'
        r'(\d+(?:\.\d+)?)\s*(?:years?|yrs?|jaar|años|ans|y\.?r\.?s?)?'
        r'(?:\s+(?:of|in|with|at))?\s*([a-zA-Z_]+)?',
        re.IGNORECASE
    )

    TOTAL_CLAIM_PATTERN = re.compile(
        r'(?:total|overall|cumulative)?\s*experience\s*:?\s*'
        r'(\d+(?:\.\d+)?)\s*(?:-|to)?\s*(\d+(?:\.\d+)?)?\s*years?',
        re.IGNORECASE
    )

    def __init__(self, ai_service: LLMProvider):
        self.ai = ai_service

    def extract_from_text(self, text: str) -> Tuple[Optional[float], Optional[str], bool]:
        """
        Extract years value and context from text.

        Args:
            text: Text to extract from

        Returns:
            Tuple of (years_value, context, is_total_claim)
        """
        years_value = None
        years_context = None
        is_total = False

        text_lower = text.lower()

        for match in self.TOTAL_CLAIM_PATTERN.finditer(text):
            if match.group(1):
                years_value = float(match.group(1))
                years_context = "total_experience"
                is_total = True
                logger.debug(f"Total experience claim: {years_value} years")
                return years_value, years_context, is_total

        for match in self.YEARS_PATTERN.finditer(text):
            if match.group(3):
                years_value = float(match.group(3))
                years_context = match.group(4) if match.group(4) else "general"
                is_total = False
                logger.debug(f"Extracted {years_value} years in context: {years_context}")
                return years_value, years_context, is_total

        try:
            ai_result = self.ai.extract_structured_data(
                text,
                {
                    'type': 'object',
                    'properties': {
                        'years_value': {'type': 'number'},
                        'years_context': {'type': 'string'},
                        'is_total': {'type': 'boolean'}
                    }
                }
            )

            if ai_result and ai_result.get('years_value') is not None:
                years_value = float(ai_result['years_value'])
                years_context = ai_result.get('years_context', 'unknown')
                is_total = ai_result.get('is_total', False)
                logger.debug(f"AI extracted: {years_value} years ({years_context}, total={is_total})")

        except Exception as e:
            logger.warning(f"Failed to extract years via AI: {e}")

        return years_value, years_context, is_total

    def extract_from_evidence(self, evidence_units) -> None:
        """
        Extract years from evidence units in-place.
        """
        for unit in evidence_units:
            try:
                years_value, years_context, is_total = self.extract_from_text(unit.text)

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
