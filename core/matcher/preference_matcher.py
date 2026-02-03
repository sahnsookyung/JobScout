#!/usr/bin/env python3
"""
Preference Matcher - Calculate job preferences alignment.

Computes separate subscores (location/company size/industry/role)
and then a weighted average using configured weights.
"""
from typing import Dict, Any, Optional, Tuple
import logging

from database.models import JobPost
from core.matcher.models import PreferencesAlignmentScore
from core.config_loader import PreferenceWeights

logger = logging.getLogger(__name__)


class PreferenceMatcher:
    """Calculate job preferences alignment."""
    
    def __init__(self, weights: PreferenceWeights):
        """
        Initialize with preference weights.
        
        Args:
            weights: PreferenceWeights with location, company_size, industry, role
        """
        self.weights = {
            'location': weights.location,
            'company_size': weights.company_size,
            'industry': weights.industry,
            'role': weights.role
        }
    
    def calculate_alignment(
        self,
        job: JobPost,
        preferences: Optional[Dict[str, Any]]
    ) -> Optional[PreferencesAlignmentScore]:
        """
        Calculate overall preferences alignment score.
        
        Returns None if no preferences provided.
        """
        if not preferences:
            return None
        
        location_score, location_details = self.calculate_location_match(job, preferences)
        company_size_score, size_details = self.calculate_company_size_match(job, preferences)
        industry_score, industry_details = self.calculate_industry_match(job, preferences)
        role_score, role_details = self.calculate_role_match(job, preferences)
        
        overall_score = (
            location_score * self.weights['location'] +
            company_size_score * self.weights['company_size'] +
            industry_score * self.weights['industry'] +
            role_score * self.weights['role']
        )
        
        return PreferencesAlignmentScore(
            overall_score=overall_score,
            location_match=location_score,
            company_size_match=company_size_score,
            industry_match=industry_score,
            role_match=role_score,
            details={
                'location': location_details,
                'company_size': size_details,
                'industry': industry_details,
                'role': role_details,
                'weights': self.weights
            }
        )
    
    def calculate_location_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate location match score based on preferences.
        
        Returns: (score, details)
        """
        job_prefs = preferences.get('job_preferences', {})
        location_prefs = job_prefs.get('location_preferences', {})
        
        preferred_locations = location_prefs.get('preferred_locations', [])
        avoid_locations = location_prefs.get('avoid_locations', [])
        wants_remote = job_prefs.get('wants_remote', True)
        
        details = {
            'job_location': job.location_text,
            'job_is_remote': job.is_remote,
            'user_wants_remote': wants_remote,
            'preferred_locations': preferred_locations,
            'avoid_locations': avoid_locations
        }
        
        if wants_remote:
            if job.is_remote:
                return 1.0, details
            else:
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 0.7, details
                
                for avoid_loc in avoid_locations:
                    if avoid_loc.lower() in job_loc:
                        return 0.0, details
                
                return 0.3, details
        else:
            if job.is_remote:
                return 0.8, details
            else:
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 1.0, details
                return 0.6, details
    
    def calculate_company_size_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate company size match score.
        
        Returns: (score, details)
        """
        company_prefs = preferences.get('company_preferences', {})
        size_prefs = company_prefs.get('company_size', {})
        
        details = {
            'job_company_size': job.company_num_employees,
            'preferred_size': size_prefs
        }
        
        if not job.company_num_employees:
            return 0.5, details
        
        try:
            emp_count = int(job.company_num_employees)
        except (ValueError, TypeError):
            return 0.5, details
        
        employee_range = size_prefs.get('employee_count', {})
        min_size = employee_range.get('minimum', 0)
        max_size = employee_range.get('maximum', float('inf'))
        
        if min_size <= emp_count <= max_size:
            return 1.0, details
        elif emp_count < min_size:
            ratio = emp_count / min_size if min_size > 0 else 0
            return max(0.0, ratio * 0.5), details
        else:
            ratio = max_size / emp_count if emp_count > 0 else 0
            return max(0.0, ratio * 0.5), details
    
    def calculate_industry_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate industry match score.
        
        Returns: (score, details)
        """
        company_prefs = preferences.get('company_preferences', {})
        industry_prefs = company_prefs.get('industry', {})
        
        preferred_industries = industry_prefs.get('preferred', [])
        avoid_industries = industry_prefs.get('avoid', [])
        
        job_industry = (job.company_industry or '').lower()
        
        details = {
            'job_industry': job.company_industry,
            'preferred_industries': preferred_industries,
            'avoid_industries': avoid_industries
        }
        
        for avoid in avoid_industries:
            if avoid.lower() in job_industry:
                return 0.0, details
        
        for preferred in preferred_industries:
            if preferred.lower() in job_industry:
                return 1.0, details
        
        return 0.5, details
    
    def calculate_role_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate role/title match score.
        
        Returns: (score, details)
        """
        career_prefs = preferences.get('career_preferences', {})
        
        preferred_roles = career_prefs.get('role_types', [])
        avoid_roles = career_prefs.get('avoid_roles', [])
        
        job_title = (job.title or '').lower()
        
        details = {
            'job_title': job.title,
            'preferred_roles': preferred_roles,
            'avoid_roles': avoid_roles
        }
        
        for avoid in avoid_roles:
            if avoid.lower() in job_title:
                return 0.0, details
        
        for preferred in preferred_roles:
            if preferred.lower() in job_title:
                return 1.0, details
        
        target_seniority = career_prefs.get('seniority_level', '')
        job_level = (job.job_level or '').lower()
        
        if target_seniority and job_level:
            if target_seniority.lower() in job_level:
                return 0.8, details
        
        return 0.5, details
