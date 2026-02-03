#!/usr/bin/env python3
"""
Matcher Models - Data structures for matching.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from database.models import JobRequirementUnit


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
                import logging
                logger = logging.getLogger(__name__)
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


@dataclass
class RequirementMatchResult:
    """Result of matching a single requirement."""
    requirement: JobRequirementUnit
    evidence: Optional[ResumeEvidenceUnit]
    similarity: float
    is_covered: bool


@dataclass
class PreferencesAlignmentScore:
    """Score indicating how well a job aligns with user preferences."""
    overall_score: float
    location_match: float
    company_size_match: float
    industry_match: float
    role_match: float
    details: Dict[str, Any]


@dataclass
class JobMatchPreliminary:
    """Preliminary match before scoring (output of MatcherService)."""
    job: 'database.models.JobPost'
    job_similarity: float
    preferences_alignment: Optional[PreferencesAlignmentScore]
    requirement_matches: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]
    resume_fingerprint: str
