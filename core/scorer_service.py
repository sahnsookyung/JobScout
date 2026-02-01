#!/usr/bin/env python3
"""
Scoring Service - Stage 2: Rule-based Scoring with Preferences Support

Takes preliminary matches from MatcherService and calculates final scores:
- Coverage metrics (required vs preferred)
- Preferences alignment integration
- Penalty application (location, seniority, compensation, preferences)
- Final weighted score with preferences boost

Designed to be microservice-ready and can run independently
of the MatcherService.
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
import logging
from sqlalchemy import func
from sqlalchemy.orm import Session

from database.repository import JobRepository
from database.models import (
    JobPost, JobMatch, JobMatchRequirement, 
    generate_resume_fingerprint
)
from core.config_loader import ScorerConfig
from core.matcher_service import (
    JobMatchPreliminary, RequirementMatchResult, 
    PreferencesAlignmentScore
)

logger = logging.getLogger(__name__)


@dataclass
class ScoredJobMatch:
    """Complete scored match result."""
    job: JobPost
    overall_score: float
    base_score: float
    preferences_boost: float  # Bonus from preferences alignment
    penalties: float
    required_coverage: float
    preferred_coverage: float
    job_similarity: float
    preferences_alignment: Optional[PreferencesAlignmentScore]
    penalty_details: List[Dict[str, Any]]
    matched_requirements: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]
    resume_fingerprint: str
    match_type: str
    ranking_mode: str = "discovery"  # discovery or strict
    score_components: Optional[Dict[str, Any]] = field(default=None)  # Detailed breakdown for explainability


class ScoringService:
    """
    Service for Stage 2: Rule-based Scoring with Preferences.
    
    Calculates final scores from preliminary matches:
    - Coverage percentages
    - Preferences alignment boost
    - Weighted base score
    - Penalties for mismatches
    - Final overall score
    
    Supports configurable ranking modes:
    - Discovery mode: optimize for breadth/recall
    - Strict mode: optimize for precision with coverage gates
    
    Designed to be independent - can be run as separate microservice.
    """
    
    # Preferences boost configuration
    PREFERENCES_BOOST_MAX = 15.0  # Maximum bonus from preferences
    
    def __init__(
        self,
        repo: JobRepository,
        config: ScorerConfig
    ):
        self.repo = repo
        self.config = config
    
    def calculate_coverage(
        self,
        matched_requirements: List[RequirementMatchResult],
        missing_requirements: List[RequirementMatchResult]
    ) -> Tuple[float, float]:
        """
        Calculate required and preferred coverage percentages.
        
        Returns: (required_coverage, preferred_coverage)
        """
        all_reqs = matched_requirements + missing_requirements
        
        required_total = len([r for r in all_reqs if r.requirement.req_type == 'required'])
        required_covered = len([m for m in matched_requirements if m.requirement.req_type == 'required'])
        
        preferred_total = len([r for r in all_reqs if r.requirement.req_type == 'preferred'])
        preferred_covered = len([m for m in matched_requirements if m.requirement.req_type == 'preferred'])
        
        required_coverage = required_covered / required_total if required_total > 0 else 0.0
        preferred_coverage = preferred_covered / preferred_total if preferred_total > 0 else 0.0
        
        return required_coverage, preferred_coverage
    
    def calculate_base_score(
        self,
        required_coverage: float,
        preferred_coverage: float
    ) -> float:
        """
        Calculate base score before penalties and preferences boost.
        
        Formula: 100 * (w_req * RequiredCoverage + w_pref * PreferredCoverage)
        """
        return 100 * (
            self.config.weight_required * required_coverage +
            self.config.weight_preferred * preferred_coverage
        )
    
    def calculate_discovery_score(
        self,
        job_similarity: float,
        required_coverage: float,
        preferred_coverage: float,
        preferences_alignment: Optional[PreferencesAlignmentScore],
        soft_penalties: float,
        ranking_config: Any
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate score using discovery mode formula (FR-6.1).
        
        Discovery mode prioritizes exploration while preventing weak fits from dominating.
        
        Formula:
        - preferences_alignment_default_score = 0.5 if preferences absent
        - required_coverage_factor = floor + (1 - floor) * (required_coverage ^ power)
        - blended_score = w_sim*job_similarity + w_prefcov*preferred_coverage + w_prefalign*pref_align
        - overall_score = clamp(0, 100, 100 * required_coverage_factor * blended_score - soft_penalties)
        
        Args:
            job_similarity: Job-level cosine similarity (0.0-1.0)
            required_coverage: Fraction of required requirements covered (0.0-1.0)
            preferred_coverage: Fraction of preferred requirements covered (0.0-1.0)
            preferences_alignment: Preferences alignment score object
            soft_penalties: Total penalty points
            ranking_config: DiscoveryModeConfig instance
        
        Returns:
            Tuple of (overall_score, score_components_dict)
        """
        cfg = ranking_config
        
        # Preferences alignment score with default
        if preferences_alignment:
            pref_align_score = preferences_alignment.overall_score
        else:
            pref_align_score = 0.5  # Neutral default per FR-6.1
        
        # Required coverage factor (non-linear dampening)
        req_factor = (
            cfg.required_coverage_factor_floor + 
            (1.0 - cfg.required_coverage_factor_floor) * 
            (required_coverage ** cfg.required_coverage_factor_power)
        )
        
        # Blended relevance score (weighted combination)
        blended_score = (
            cfg.job_similarity_weight * job_similarity +
            cfg.preferred_coverage_weight * preferred_coverage +
            cfg.preferences_alignment_weight * pref_align_score
        )
        
        # Apply penalties with multiplier
        adjusted_penalties = soft_penalties * cfg.penalties_multiplier
        
        # Final score calculation
        raw_score = 100.0 * req_factor * blended_score - adjusted_penalties
        overall_score = max(0.0, min(100.0, raw_score))
        
        # Return score with component breakdown for explainability
        score_components = {
            'mode': 'discovery',
            'job_similarity': job_similarity,
            'required_coverage': required_coverage,
            'preferred_coverage': preferred_coverage,
            'preferences_alignment_score': pref_align_score,
            'required_coverage_factor': req_factor,
            'blended_score': blended_score,
            'soft_penalties': soft_penalties,
            'penalties_multiplier': cfg.penalties_multiplier,
            'adjusted_penalties': adjusted_penalties,
            'raw_score': raw_score,
            'overall_score': overall_score
        }
        
        return overall_score, score_components
    
    def calculate_strict_score(
        self,
        job_similarity: float,
        required_coverage: float,
        preferred_coverage: float,
        preferences_alignment: Optional[PreferencesAlignmentScore],
        penalties: float,
        ranking_config: Any
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate score using strict mode formula (FR-6.2).
        
        Strict mode strongly prefers high required coverage and enforces hard constraints.
        
        Formula:
        - If required_coverage < required_coverage_minimum:
          - If low_fit_policy == reject: overall_score = 0
          - If low_fit_policy == cap: overall_score = min(calculated_score, low_fit_score_cap)
        - Otherwise:
          - required_coverage_factor = required_coverage ^ power
          - blended_score = w_req*required_coverage + w_sim*job_similarity + w_prefcov*preferred_coverage + w_prefalign*pref_align
          - overall_score = clamp(0, 100, 100 * blended_score - penalties)
        
        Args:
            job_similarity: Job-level cosine similarity (0.0-1.0)
            required_coverage: Fraction of required requirements covered (0.0-1.0)
            preferred_coverage: Fraction of preferred requirements covered (0.0-1.0)
            preferences_alignment: Preferences alignment score object
            penalties: Total penalty points
            ranking_config: StrictModeConfig instance
        
        Returns:
            Tuple of (overall_score, score_components_dict)
        """
        cfg = ranking_config
        
        # Preferences alignment score with default
        if preferences_alignment:
            pref_align_score = preferences_alignment.overall_score
        else:
            pref_align_score = 0.5  # Neutral default
        
        # Check coverage gate
        coverage_gate_triggered = required_coverage < cfg.required_coverage_minimum
        gate_action = None
        
        if coverage_gate_triggered:
            gate_action = cfg.low_fit_policy
            if cfg.low_fit_policy == "reject":
                # Reject low-coverage matches entirely
                score_components = {
                    'mode': 'strict',
                    'job_similarity': job_similarity,
                    'required_coverage': required_coverage,
                    'preferred_coverage': preferred_coverage,
                    'preferences_alignment_score': pref_align_score,
                    'coverage_gate_triggered': True,
                    'gate_action': 'reject',
                    'required_coverage_minimum': cfg.required_coverage_minimum,
                    'overall_score': 0.0
                }
                return 0.0, score_components
        
        # Required coverage factor (exponential emphasis)
        req_factor = required_coverage ** cfg.required_coverage_emphasis_power
        
        # Blended relevance score (weighted combination with required coverage prominently)
        blended_score = (
            cfg.required_coverage_weight * required_coverage +
            cfg.job_similarity_weight * job_similarity +
            cfg.preferred_coverage_weight * preferred_coverage +
            cfg.preferences_alignment_weight * pref_align_score
        )
        
        # Apply penalties with multiplier (strict mode penalizes more)
        adjusted_penalties = penalties * cfg.penalties_multiplier
        
        # Final score calculation
        raw_score = 100.0 * req_factor * blended_score - adjusted_penalties
        overall_score = max(0.0, min(100.0, raw_score))
        
        # Apply low-fit cap if triggered and policy is "cap"
        if coverage_gate_triggered and cfg.low_fit_policy == "cap":
            overall_score = min(overall_score, cfg.low_fit_score_cap)
        
        # Return score with component breakdown for explainability
        score_components = {
            'mode': 'strict',
            'job_similarity': job_similarity,
            'required_coverage': required_coverage,
            'preferred_coverage': preferred_coverage,
            'preferences_alignment_score': pref_align_score,
            'required_coverage_factor': req_factor,
            'blended_score': blended_score,
            'penalties': penalties,
            'penalties_multiplier': cfg.penalties_multiplier,
            'adjusted_penalties': adjusted_penalties,
            'coverage_gate_triggered': coverage_gate_triggered,
            'gate_action': gate_action,
            'required_coverage_minimum': cfg.required_coverage_minimum,
            'raw_score': raw_score,
            'overall_score': overall_score
        }
        
        return overall_score, score_components
    
    def calculate_preferences_boost(
        self,
        preferences_alignment: Optional[PreferencesAlignmentScore]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate bonus score from preferences alignment.
        
        Good preference matches get a boost to overall score.
        
        Returns: (boost_amount, boost_details)
        """
        if not preferences_alignment:
            return 0.0, {'reason': 'No preferences provided', 'boost': 0.0}
        
        # Calculate boost based on overall alignment
        # Scale alignment score (0.0-1.0) to boost range
        alignment = preferences_alignment.overall_score
        
        # Non-linear scaling: higher alignment gets disproportionately more boost
        # Score 0.5 -> small boost
        # Score 0.8 -> good boost
        # Score 1.0 -> max boost
        if alignment >= 0.9:
            boost = self.PREFERENCES_BOOST_MAX
        elif alignment >= 0.75:
            boost = self.PREFERENCES_BOOST_MAX * 0.7
        elif alignment >= 0.6:
            boost = self.PREFERENCES_BOOST_MAX * 0.4
        elif alignment >= 0.5:
            boost = self.PREFERENCES_BOOST_MAX * 0.2
        else:
            boost = 0.0
        
        details = {
            'reason': f"Preferences alignment: {alignment:.2f}",
            'boost': boost,
            'alignment_breakdown': {
                'location': preferences_alignment.location_match,
                'company_size': preferences_alignment.company_size_match,
                'industry': preferences_alignment.industry_match,
                'role': preferences_alignment.role_match
            }
        }
        
        return boost, details
    
    def calculate_penalties(
        self,
        job: JobPost,
        required_coverage: float,
        matched_requirements: List[RequirementMatchResult],
        missing_requirements: List[RequirementMatchResult],
        preferences_alignment: Optional[PreferencesAlignmentScore] = None
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """
        Calculate total penalties with detailed breakdown.
        
        Includes penalties from:
        - Missing required skills
        - Location mismatch
        - Seniority mismatch
        - Compensation mismatch
        - Preferences mismatches (if preferences_alignment is provided)
        
        Returns: (total_penalties, penalty_details)
        """
        penalties = 0.0
        penalty_details = []
        
        # Penalty for missing required requirements
        all_reqs = matched_requirements + missing_requirements
        required_total = len([r for r in all_reqs if r.requirement.req_type == 'required'])
        required_covered = len([m for m in matched_requirements if m.requirement.req_type == 'required'])
        missing_required = required_total - required_covered
        
        if missing_required > 0:
            penalty_amount = missing_required * self.config.penalty_missing_required
            penalties += penalty_amount
            missing_reqs = [m.requirement.text for m in missing_requirements 
                           if m.requirement.req_type == 'required']
            penalty_details.append({
                'type': 'missing_required',
                'amount': penalty_amount,
                'reason': f"{missing_required} required skill(s) not covered",
                'details': missing_reqs[:3]  # Limit to first 3
            })
        
        # Penalty for location mismatch (from config or preferences)
        if preferences_alignment:
            # Use preferences alignment location score
            if preferences_alignment.location_match < 0.5:
                penalties += self.config.penalty_location_mismatch
                penalty_details.append({
                    'type': 'location_mismatch',
                    'amount': self.config.penalty_location_mismatch,
                    'reason': f"Poor location match",
                    'details': preferences_alignment.details.get('location', {})
                })
        elif self.config.wants_remote and not job.is_remote:
            # Fallback to config-based check
            penalties += self.config.penalty_location_mismatch
            penalty_details.append({
                'type': 'location_mismatch',
                'amount': self.config.penalty_location_mismatch,
                'reason': f"Job is not remote (user preference: remote)",
                'details': f"Job location: {job.location_text}, remote={job.is_remote}"
            })
        
        # Penalty for seniority mismatch
        if self.config.target_seniority and job.job_level:
            job_level = (job.job_level or '').lower()
            target = self.config.target_seniority.lower()
            
            seniority_mismatch = False
            if target == 'junior' and ('senior' in job_level or 'lead' in job_level):
                seniority_mismatch = True
            elif target == 'senior' and ('junior' in job_level or 'entry' in job_level):
                seniority_mismatch = True
            
            if seniority_mismatch:
                penalties += self.config.penalty_seniority_mismatch
                penalty_details.append({
                    'type': 'seniority_mismatch',
                    'amount': self.config.penalty_seniority_mismatch,
                    'reason': f"Seniority level mismatch",
                    'details': f"Job level: {job.job_level}, Target: {self.config.target_seniority}"
                })
        
        # Penalty for compensation mismatch
        if self.config.min_salary and job.salary_max:
            try:
                job_salary = float(job.salary_max)
                if job_salary < self.config.min_salary:
                    penalties += self.config.penalty_compensation_mismatch
                    penalty_details.append({
                        'type': 'compensation_mismatch',
                        'amount': self.config.penalty_compensation_mismatch,
                        'reason': f"Salary below minimum requirement",
                        'details': f"Job max: {job.salary_max}, User min: {self.config.min_salary}"
                    })
            except (ValueError, TypeError):
                pass  # Can't parse salary
        
        # Additional penalties from preferences alignment
        if preferences_alignment:
            # Penalty for bad industry match
            if preferences_alignment.industry_match == 0.0:
                penalty_amount = 10.0
                penalties += penalty_amount
                penalty_details.append({
                    'type': 'industry_mismatch',
                    'amount': penalty_amount,
                    'reason': f"Job in avoided industry",
                    'details': preferences_alignment.details.get('industry', {})
                })
            
            # Penalty for bad role match
            if preferences_alignment.role_match == 0.0:
                penalty_amount = 10.0
                penalties += penalty_amount
                penalty_details.append({
                    'type': 'role_mismatch',
                    'amount': penalty_amount,
                    'reason': f"Job title matches avoided role",
                    'details': preferences_alignment.details.get('role', {})
                })
        
        return penalties, penalty_details
    
    def score_preliminary_match(
        self,
        preliminary: JobMatchPreliminary,
        match_type: str = "requirements_only"
    ) -> ScoredJobMatch:
        """
        Calculate final score from preliminary match.
        
        Formula: Overall = BaseScore + PreferencesBoost - Penalties
        """
        job = preliminary.job
        
        # Calculate coverage
        required_coverage, preferred_coverage = self.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )
        
        # Calculate base score
        base_score = self.calculate_base_score(required_coverage, preferred_coverage)
        
        # Calculate preferences boost (bonus for good matches)
        preferences_boost, boost_details = self.calculate_preferences_boost(
            preliminary.preferences_alignment
        )
        
        # Calculate penalties (including from preferences)
        penalties, penalty_details = self.calculate_penalties(
            job,
            required_coverage,
            preliminary.requirement_matches,
            preliminary.missing_requirements,
            preliminary.preferences_alignment
        )
        
        # Final score (never negative)
        overall_score = max(0.0, base_score + preferences_boost - penalties)
        
        # Log if preferences contributed significantly
        if preferences_boost > 0:
            logger.debug(f"Job {job.id}: +{preferences_boost:.1f} boost from preferences")
        
        return ScoredJobMatch(
            job=job,
            overall_score=overall_score,
            base_score=base_score,
            preferences_boost=preferences_boost,
            penalties=penalties,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            job_similarity=preliminary.job_similarity,
            preferences_alignment=preliminary.preferences_alignment,
            penalty_details=penalty_details + [boost_details] if preferences_boost > 0 else penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type
        )
    
    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        match_type: str = "requirements_only"
    ) -> List[ScoredJobMatch]:
        """
        Score multiple preliminary matches.
        
        Returns sorted by overall score (highest first).
        """
        scored_matches = []
        
        for preliminary in preliminary_matches:
            scored = self.score_preliminary_match(preliminary, match_type)
            scored_matches.append(scored)
        
        # Sort by overall score descending
        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)
        
        return scored_matches
    
    def score_preliminary_match_with_mode(
        self,
        preliminary: JobMatchPreliminary,
        ranking_config: Any,
        match_type: str = "requirements_only"
    ) -> ScoredJobMatch:
        """
        Calculate final score from preliminary match using mode-specific formula.
        
        Supports discovery and strict modes with different scoring formulas:
        - Discovery: optimize for breadth/recall (FR-6.1)
        - Strict: optimize for precision with coverage gates (FR-6.2)
        
        Args:
            preliminary: Preliminary match from MatcherService
            ranking_config: RankingConfig (contains mode and mode-specific settings)
            match_type: Type of match being performed
        
        Returns:
            ScoredJobMatch with mode-specific score and component breakdown
        """
        job = preliminary.job
        mode = ranking_config.mode
        
        # Calculate coverage (common to both modes)
        required_coverage, preferred_coverage = self.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )
        
        # Calculate penalties (with mode-specific handling)
        penalties, penalty_details = self.calculate_penalties(
            job,
            required_coverage,
            preliminary.requirement_matches,
            preliminary.missing_requirements,
            preliminary.preferences_alignment
        )
        
        if mode == "discovery":
            # Discovery mode: optimize for breadth/recall (FR-6.1)
            mode_config = ranking_config.discovery
            
            # Missing required policy: disabled by default in discovery
            if mode_config.missing_required_policy == "disabled":
                # Remove missing_required penalties to avoid double-counting
                penalties = sum(p['amount'] for p in penalty_details if p['type'] != 'missing_required')
                penalty_details = [p for p in penalty_details if p['type'] != 'missing_required']
            
            overall_score, score_components = self.calculate_discovery_score(
                job_similarity=preliminary.job_similarity,
                required_coverage=required_coverage,
                preferred_coverage=preferred_coverage,
                preferences_alignment=preliminary.preferences_alignment,
                soft_penalties=penalties,
                ranking_config=mode_config
            )
            
            # Calculate base_score and preferences_boost for backward compatibility
            base_score = score_components.get('blended_score', 0.0) * 100.0
            preferences_boost = 0.0  # Discovery mode integrates preferences into blend
            
        else:  # strict mode
            # Strict mode: optimize for precision with coverage gates (FR-6.2)
            mode_config = ranking_config.strict
            
            overall_score, score_components = self.calculate_strict_score(
                job_similarity=preliminary.job_similarity,
                required_coverage=required_coverage,
                preferred_coverage=preferred_coverage,
                preferences_alignment=preliminary.preferences_alignment,
                penalties=penalties,
                ranking_config=mode_config
            )
            
            # Calculate base_score and preferences_boost for backward compatibility
            base_score = score_components.get('blended_score', 0.0) * 100.0
            preferences_boost = 0.0  # Strict mode integrates preferences into blend
        
        # Log score components for debugging
        logger.debug(f"Job {job.id} ({mode} mode): score={overall_score:.1f}, "
                    f"req_cov={required_coverage:.2f}, sim={preliminary.job_similarity:.2f}")
        
        return ScoredJobMatch(
            job=job,
            overall_score=overall_score,
            base_score=base_score,
            preferences_boost=preferences_boost,
            penalties=penalties,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            job_similarity=preliminary.job_similarity,
            preferences_alignment=preliminary.preferences_alignment,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            ranking_mode=mode,
            score_components=score_components
        )
    
    def score_matches_with_mode(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        ranking_config: Any,
        match_type: str = "requirements_only",
        final_results_n: Optional[int] = None
    ) -> List[ScoredJobMatch]:
        """
        Score multiple preliminary matches using mode-specific formula.
        
        Args:
            preliminary_matches: List of preliminary matches from Stage 2
            ranking_config: RankingConfig with mode and mode-specific settings
            match_type: Type of match being performed
            final_results_n: Optional limit on final results (defaults to mode config)
        
        Returns:
            List of ScoredJobMatch sorted by overall score (highest first),
            limited to final_results_n
        """
        scored_matches = []
        
        for preliminary in preliminary_matches:
            scored = self.score_preliminary_match_with_mode(
                preliminary, ranking_config, match_type
            )
            scored_matches.append(scored)
        
        # Sort by overall score descending
        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)
        
        # Apply final results limit based on mode
        if final_results_n is None:
            if ranking_config.mode == "discovery":
                final_results_n = ranking_config.discovery.final_results_n
            else:
                final_results_n = ranking_config.strict.final_results_n
        
        limited_matches = scored_matches[:final_results_n]
        
        logger.info(f"Scored {len(scored_matches)} matches in {ranking_config.mode} mode, "
                   f"returning top {len(limited_matches)}")
        
        return limited_matches
    
    def save_match_to_db(
        self,
        scored_match: ScoredJobMatch,
        preferences_file_hash: Optional[str] = None
    ) -> JobMatch:
        """
        Save scored match to database.
        
        Creates JobMatch record with associated JobMatchRequirement records.
        """
        job = scored_match.job
        
        # Check if match already exists
        from sqlalchemy import select
        existing_stmt = select(JobMatch).where(
            JobMatch.job_post_id == job.id,
            JobMatch.resume_fingerprint == scored_match.resume_fingerprint
        )
        existing = self.repo.db.execute(existing_stmt).scalar_one_or_none()
        
        if existing:
            # Update existing match with new scores
            match_record = existing
            match_record.status = 'active'
            match_record.job_similarity = scored_match.job_similarity
            match_record.overall_score = scored_match.overall_score
            match_record.base_score = scored_match.base_score
            match_record.penalties = scored_match.penalties
            match_record.penalty_details = {
                'details': scored_match.penalty_details,
                'total': scored_match.penalties,
                'preferences_boost': scored_match.preferences_boost
            }
            match_record.required_coverage = scored_match.required_coverage
            match_record.preferred_coverage = scored_match.preferred_coverage
            match_record.total_requirements = len(scored_match.matched_requirements) + len(scored_match.missing_requirements)
            match_record.matched_requirements_count = len(scored_match.matched_requirements)
            match_record.match_type = scored_match.match_type
            match_record.preferences_file_hash = preferences_file_hash
            match_record.calculated_at = func.now()
            # Preserve notified status on update
        else:
            # Create new match
            match_record = JobMatch(
                job_post_id=job.id,
                resume_fingerprint=scored_match.resume_fingerprint,
                job_similarity=scored_match.job_similarity,
                overall_score=scored_match.overall_score,
                base_score=scored_match.base_score,
                penalties=scored_match.penalties,
                penalty_details={
                    'details': scored_match.penalty_details,
                    'total': scored_match.penalties,
                    'preferences_boost': scored_match.preferences_boost
                },
                required_coverage=scored_match.required_coverage,
                preferred_coverage=scored_match.preferred_coverage,
                total_requirements=len(scored_match.matched_requirements) + len(scored_match.missing_requirements),
                matched_requirements_count=len(scored_match.matched_requirements),
                match_type=scored_match.match_type,
                preferences_file_hash=preferences_file_hash,
                notified=False  # New matches are not notified yet
            )
            self.repo.db.add(match_record)
        
        self.repo.db.flush()  # Get ID
        
        # Delete old requirement matches if updating
        if existing:
            from sqlalchemy import delete
            self.repo.db.execute(
                delete(JobMatchRequirement).where(
                    JobMatchRequirement.job_match_id == match_record.id
                )
            )
        
        # Create requirement match records
        for req_match in scored_match.matched_requirements:
            jmr = JobMatchRequirement(
                job_match_id=match_record.id,
                job_requirement_unit_id=req_match.requirement.id,
                evidence_text=req_match.evidence.text if req_match.evidence else "",
                evidence_section=req_match.evidence.source_section if req_match.evidence else None,
                evidence_tags=req_match.evidence.tags if req_match.evidence else {},
                similarity_score=req_match.similarity,
                is_covered=req_match.is_covered,
                req_type=req_match.requirement.req_type
            )
            self.repo.db.add(jmr)
        
        # Also record missing requirements
        for req_match in scored_match.missing_requirements:
            jmr = JobMatchRequirement(
                job_match_id=match_record.id,
                job_requirement_unit_id=req_match.requirement.id,
                evidence_text="",
                evidence_section=None,
                evidence_tags={},
                similarity_score=req_match.similarity,
                is_covered=False,
                req_type=req_match.requirement.req_type
            )
            self.repo.db.add(jmr)
        
        self.repo.db.commit()
        
        logger.info(f"Saved match for job {job.id}: score={scored_match.overall_score:.1f} "
                   f"(base={scored_match.base_score:.1f}, boost={scored_match.preferences_boost:.1f}, "
                   f"penalties={scored_match.penalties:.1f})")
        
        return match_record
