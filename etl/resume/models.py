#!/usr/bin/env python3
"""
Resume Models - Data structures for resume extraction and profiling.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import logging
logger = logging.getLogger(__name__)


@dataclass
class ResumeEvidenceUnit:
    """Resume Evidence Unit - atomic claim from resume."""
    id: str
    text: str
    source_section: str
    tags: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    years_value: Optional[float] = None
    years_context: Optional[str] = None
    is_total_years_claim: bool = False


@dataclass
class StructuredResumeProfile:
    """Structured resume data extracted using comprehensive schema."""
    raw_data: Dict[str, Any]
    calculated_total_years: Optional[float] = None
    claimed_total_years: Optional[float] = None
    experience_entries: List[Dict[str, Any]] = field(default_factory=list)

    def calculate_experience_from_dates(self) -> float:
        """Calculate total years of experience from date ranges."""
        total_months = 0

        for entry in self.experience_entries:
            start_date_str = entry.get('start_date')
            end_date_str = entry.get('end_date')
            is_current = entry.get('is_current', False)

            if not start_date_str:
                continue

            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m').date()

                if is_current or not end_date_str:
                    end_date = date.today()
                else:
                    end_date = datetime.strptime(end_date_str, '%Y-%m').date()

                diff = relativedelta(end_date, start_date)
                months = diff.years * 12 + diff.months
                total_months += max(0, months)

            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse dates for experience entry: {e}")
                continue

        return round(total_months / 12, 1)

    def validate_experience_claim(self) -> Tuple[bool, str]:
        """
        Check if claimed total years matches calculated years.
        Returns: (is_valid, message)
        """
        if self.claimed_total_years is None:
            return True, "No explicit claim made"

        if self.calculated_total_years is None:
            return True, "Could not calculate from dates"

        tolerance = max(self.claimed_total_years * 0.2, 1.0)
        difference = abs(self.claimed_total_years - self.calculated_total_years)

        if difference <= tolerance:
            return True, f"Claim valid: {self.claimed_total_years} vs calculated {self.calculated_total_years}"
        else:
            return False, f"Claim suspicious: claims {self.claimed_total_years} but calculated {self.calculated_total_years}"
