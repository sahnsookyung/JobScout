#!/usr/bin/env python3
"""
Match service - business logic for job match operations.
"""

import json
import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import desc

from database.models import JobMatch, JobPost, JobMatchRequirement
from ..models.responses import (
    MatchSummary,
    MatchDetail,
    MatchDetailResponse,
    JobDetails,
    RequirementDetail
)
from ..utils import safe_float, safe_int, safe_str, safe_datetime_iso
from ..exceptions import MatchNotFoundException, JobNotFoundException

logger = logging.getLogger(__name__)


class MatchService:
    """Service for managing job matches."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_matches(
        self,
        status: str = "active",
        min_fit: Optional[float] = None,
        top_k: Optional[int] = None,
        remote_only: bool = False,
        show_hidden: bool = False
    ) -> List[MatchSummary]:
        """
        Get filtered job matches.
        
        Args:
            status: Match status filter ("active", "stale", or "all").
            min_fit: Minimum fit score filter.
            top_k: Maximum number of results to return.
            remote_only: Filter to remote jobs only.
            show_hidden: Include hidden matches in results.
        
        Returns:
            List of match summaries.
        """
        query = self.db.query(JobMatch)
        
        # Filter by status
        if status != "all":
            query = query.filter(JobMatch.status == status)
        
        # Filter by fit score
        if min_fit is not None:
            query = query.filter(
                (JobMatch.fit_score >= min_fit) | (JobMatch.fit_score.is_(None))
            )
        
        # Filter hidden matches
        if not show_hidden:
            query = query.filter(JobMatch.is_hidden.is_(False))
        
        # Eager load job posts to avoid N+1 queries
        query = query.options(joinedload(JobMatch.job_post))
        
        # Order by overall score
        query = query.order_by(desc(JobMatch.overall_score))
        
        # Filter remote jobs before limit (via SQL JOIN)
        if remote_only:
            query = query.join(JobPost).filter(JobPost.is_remote.is_(True))
        
        # Apply limit
        if top_k:
            query = query.limit(top_k)
        
        matches = query.all()
        
        return [self._to_match_summary(m) for m in matches]
    
    def get_match_detail(self, match_id: str) -> MatchDetailResponse:
        """
        Get detailed information about a specific match.

        Args:
            match_id: The match ID.

        Returns:
            Detailed match information.

        Raises:
            MatchNotFoundException: If match is not found.
        """
        try:
            match = self.db.query(JobMatch).get(match_id)
            if not match:
                raise MatchNotFoundException(f"Match {match_id} not found")

            job = self.db.query(JobPost).get(match.job_post_id)

            req_matches = self.db.query(JobMatchRequirement).options(
                joinedload(JobMatchRequirement.requirement)
            ).filter(
                JobMatchRequirement.job_match_id == match_id
            ).all()

            requirements = [self._to_requirement_detail(req) for req in req_matches]

            penalty_details = self._parse_penalty_details(match.penalty_details)

            return MatchDetailResponse(
                success=True,
                match=self._to_match_detail(match, penalty_details),
                job=self._to_job_details(job),
                requirements=requirements
            )
        except MatchNotFoundException:
            raise
        except Exception as e:
            logger.error(f"Error fetching match details for {match_id}: {e}", exc_info=True)
            raise MatchNotFoundException(f"Error fetching match details: {str(e)}") from e
    
    def toggle_hidden(self, match_id: str) -> bool:
        """
        Toggle the hidden status of a match.
        
        Args:
            match_id: The match ID.
        
        Returns:
            New hidden status.
        
        Raises:
            MatchNotFoundException: If match is not found.
        """
        from database.repositories.match import MatchRepository
        
        repo = MatchRepository(self.db)
        match = repo.get_match_by_id(match_id)
        
        if not match:
            raise MatchNotFoundException(f"Match {match_id} not found")
        
        new_status = not (match.is_hidden or False)
        repo.update_hidden_status(match_id, new_status)
        self.db.commit()
        
        return new_status
    
    def get_match_explanation(self, match_id: str) -> Dict[str, Any]:
        """
        Get explainability details for a specific match.
        
        Args:
            match_id: The match ID.
        
        Returns:
            Match explanation data.
        
        Raises:
            MatchNotFoundException: If match is not found.
            JobNotFoundException: If associated job is not found.
        """
        match = self.db.query(JobMatch).get(match_id)
        if not match:
            raise MatchNotFoundException(f"Match {match_id} not found")
        
        resume_fp = match.resume_fingerprint
        if not resume_fp:
            return {
                "success": True,
                "explanation": None,
                "message": "Match has no resume fingerprint"
            }
        
        job = self.db.query(JobPost).get(match.job_post_id)
        if not job:
            raise JobNotFoundException(f"Job {match.job_post_id} not found")
        
        if not hasattr(job, 'requirements') or not job.requirements:
            return {
                "success": True,
                "explanation": None,
                "message": "Job has no requirements"
            }
        
        from core.matcher.explainability import explain_match
        from database.repository import JobRepository
        
        repo = JobRepository(self.db)
        explanation = explain_match(
            job_requirements=job.requirements,
            resume_fingerprint=resume_fp,
            repo=repo
        )
        
        return {
            "success": True,
            "match_id": match_id,
            "explanation": explanation
        }
    
    # Private helper methods
    
    def _to_match_summary(self, match: JobMatch) -> MatchSummary:
        """Convert ORM model to MatchSummary response model."""
        job = match.job_post

        try:
            job_id = str(job.id) if job else None
            title = job.title if job and hasattr(job, 'title') else "Unknown"
            company = job.company if job and hasattr(job, 'company') else "Unknown"
            location = job.location_text if job and hasattr(job, 'location_text') else None
            is_remote = job.is_remote if job and hasattr(job, 'is_remote') else False
        except Exception as e:
            logger.warning(f"Error accessing job_post fields for match {match.id}: {e}")
            job_id = None
            title = "Unknown"
            company = "Unknown"
            location = None
            is_remote = False

        return MatchSummary(
            match_id=str(match.id),
            job_id=job_id,
            title=title,
            company=company,
            location=location,
            is_remote=is_remote,
            fit_score=safe_float(match.fit_score) if match.fit_score is not None else None,
            want_score=safe_float(match.want_score) if match.want_score is not None else None,
            overall_score=safe_float(match.overall_score) if match.overall_score is not None else 0.0,
            base_score=safe_float(match.base_score) if match.base_score is not None else 0.0,
            penalties=safe_float(match.penalties) if match.penalties is not None else 0.0,
            required_coverage=safe_float(match.required_coverage) if match.required_coverage is not None else 0.0,
            preferred_coverage=safe_float(match.preferred_coverage) if match.preferred_coverage is not None else 0.0,
            match_type=safe_str(match.match_type, "unknown"),
            is_hidden=match.is_hidden or False,
            created_at=safe_datetime_iso(match.created_at),
            calculated_at=safe_datetime_iso(match.calculated_at),
        )
    
    def _to_match_detail(self, match: JobMatch, penalty_details: Dict[str, Any]) -> MatchDetail:
        """Convert ORM model to MatchDetail response model."""
        return MatchDetail(
            match_id=str(match.id),
            resume_fingerprint=safe_str(match.resume_fingerprint),
            fit_score=safe_float(match.fit_score) if match.fit_score is not None else None,
            want_score=safe_float(match.want_score) if match.want_score is not None else None,
            overall_score=safe_float(match.overall_score),
            fit_components=match.fit_components,
            want_components=match.want_components,
            fit_weight=safe_float(match.fit_weight) if match.fit_weight is not None else None,
            want_weight=safe_float(match.want_weight) if match.want_weight is not None else None,
            base_score=safe_float(match.base_score),
            penalties=safe_float(match.penalties),
            required_coverage=safe_float(match.required_coverage),
            preferred_coverage=safe_float(match.preferred_coverage),
            total_requirements=safe_int(match.total_requirements),
            matched_requirements_count=safe_int(match.matched_requirements_count),
            match_type=safe_str(match.match_type, "unknown"),
            status=safe_str(match.status, "unknown"),
            created_at=safe_datetime_iso(match.created_at),
            calculated_at=safe_datetime_iso(match.calculated_at),
            penalty_details=penalty_details,
        )
    
    def _to_job_details(self, job: Optional[JobPost]) -> JobDetails:
        """Convert ORM model to JobDetails response model."""
        if not job:
            return JobDetails(
                job_id=None,
                title=None,
                company=None,
                location=None,
                is_remote=None,
                description=None,
                salary_min=None,
                salary_max=None,
                currency=None,
                min_years_experience=None,
                requires_degree=None,
                security_clearance=None,
                job_level=None,
            )
        
        return JobDetails(
            job_id=str(job.id),
            title=job.title,
            company=job.company,
            location=job.location_text,
            is_remote=job.is_remote,
            description=job.description,
            salary_min=safe_float(job.salary_min) if job.salary_min is not None else None,
            salary_max=safe_float(job.salary_max) if job.salary_max is not None else None,
            currency=job.currency,
            min_years_experience=safe_int(job.min_years_experience) if job.min_years_experience is not None else None,
            requires_degree=job.requires_degree,
            security_clearance=job.security_clearance,
            job_level=job.job_level,
        )
    
    def _to_requirement_detail(self, req: JobMatchRequirement) -> RequirementDetail:
        """Convert ORM model to RequirementDetail response model."""
        return RequirementDetail(
            requirement_id=str(req.job_requirement_unit_id),
            requirement_text=req.requirement.text if req.requirement else None,
            evidence_text=req.evidence_text,
            evidence_section=req.evidence_section,
            similarity_score=safe_float(req.similarity_score),
            is_covered=req.is_covered or False,
            req_type=safe_str(req.req_type, "required"),
        )
    
    def _parse_penalty_details(self, penalty_details: Any) -> Dict[str, Any]:
        """Parse penalty details from JSON or dict."""
        if penalty_details is None:
            return {}
        
        if isinstance(penalty_details, dict):
            return penalty_details
        
        if isinstance(penalty_details, str):
            try:
                return json.loads(penalty_details)
            except (json.JSONDecodeError, ValueError):
                return {}
        
        return {}
