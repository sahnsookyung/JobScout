#!/usr/bin/env python3
"""
JobScout Web Dashboard - FastAPI Version

A modern web application to view job matching results with automatic API documentation.

Usage:
    uv run python web/app.py
    
Then open:
    - http://localhost:8080 - Dashboard (default port, configurable in config.yaml)
    - http://localhost:8080/docs - API Documentation (Swagger UI)
    - http://localhost:8080/redoc - Alternative API Documentation
"""

import os
import sys
import json
import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from decimal import Decimal

from database.models import JobMatch, JobPost, JobMatchRequirement
from core.config_loader import ResultPolicy

# Configure logging
logger = logging.getLogger(__name__)

# Load configuration
config_path = project_root / 'config.yaml'
if config_path.exists():
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
else:
    config = {}

# Web server configuration (with environment variable overrides for Docker)
WEB_HOST = os.environ.get('WEB_HOST', config.get('web', {}).get('host', '0.0.0.0'))
WEB_PORT = int(os.environ.get('WEB_PORT', config.get('web', {}).get('port', 8080)))

# Database configuration - use environment variable or config file
DB_URL = os.environ.get('DATABASE_URL', config.get('database', {}).get('url', 'postgresql://user:password@localhost:5432/jobscout'))

# Create engine once at module level (not per-request)
ENGINE = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=ENGINE)


def get_db():
    """FastAPI dependency that yields a database session and ensures cleanup."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create FastAPI app with metadata
app = FastAPI(
    title="JobScout API",
    description="API for viewing job matching results",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Pydantic Models for API responses

class MatchSummary(BaseModel):
    """Summary of a job match."""
    match_id: str
    job_id: Optional[str]
    title: str
    company: str
    location: Optional[str]
    is_remote: Optional[bool]

    # New: Explicit Fit/Want/Overall scores
    fit_score: Optional[float] = Field(None, ge=0, le=100)
    want_score: Optional[float] = Field(None, ge=0, le=100)
    overall_score: float = Field(ge=0, le=100)

    # Legacy fields for backward compatibility
    base_score: float = Field(ge=0, le=100)
    penalties: float = Field(ge=0)
    required_coverage: float = Field(ge=0, le=1)
    preferred_coverage: float = Field(ge=0, le=1)
    match_type: str
    created_at: Optional[str]
    calculated_at: Optional[str]

    class Config:
        json_schema_extra = {
            "example": {
                "match_id": "550e8400-e29b-41d4-a716-446655440000",
                "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "title": "Senior Python Developer",
                "company": "TechCorp",
                "location": "Remote",
                "is_remote": True,
                "fit_score": 82.5,
                "want_score": 78.0,
                "overall_score": 81.0,
                "base_score": 95.0,
                "penalties": 9.5,
                "required_coverage": 0.9,
                "preferred_coverage": 0.8,
                "match_type": "with_preferences",
                "created_at": "2026-02-01T12:00:00",
                "calculated_at": "2026-02-01T12:00:00"
            }
        }


class RequirementDetail(BaseModel):
    """Details of a requirement match."""
    requirement_id: str
    evidence_text: Optional[str]
    evidence_section: Optional[str]
    similarity_score: float = Field(ge=0, le=1)
    is_covered: bool
    req_type: str


class JobDetails(BaseModel):
    """Details of a job posting."""
    job_id: Optional[str]
    title: Optional[str]
    company: Optional[str]
    location: Optional[str]
    is_remote: Optional[bool]
    description: Optional[str]
    salary_min: Optional[float]
    salary_max: Optional[float]
    currency: Optional[str]
    min_years_experience: Optional[int]
    requires_degree: Optional[bool]
    security_clearance: Optional[bool]
    job_level: Optional[str]


class MatchDetail(BaseModel):
    """Detailed match information."""
    match_id: str
    resume_fingerprint: str

    # New: Explicit Fit/Want/Overall scores
    fit_score: Optional[float] = None
    want_score: Optional[float] = None
    overall_score: float

    # Score breakdowns
    fit_components: Optional[Dict[str, Any]] = None
    want_components: Optional[Dict[str, Any]] = None
    fit_weight: Optional[float] = None
    want_weight: Optional[float] = None

    # Legacy fields
    base_score: float
    penalties: float
    required_coverage: float
    preferred_coverage: float
    total_requirements: int
    matched_requirements_count: int
    match_type: str
    status: str
    created_at: Optional[str]
    calculated_at: Optional[str]
    penalty_details: Dict[str, Any]


class MatchDetailResponse(BaseModel):
    """Response containing full match details."""
    success: bool
    match: MatchDetail
    job: JobDetails
    requirements: List[RequirementDetail]


class MatchesResponse(BaseModel):
    """Response containing list of matches."""
    success: bool
    count: int
    matches: List[MatchSummary]


class ScoreDistribution(BaseModel):
    """Distribution of match scores."""
    excellent: int = Field(ge=0, description="Matches with score >= 80")
    good: int = Field(ge=0, description="Matches with score 60-79")
    average: int = Field(ge=0, description="Matches with score 40-59")
    poor: int = Field(ge=0, description="Matches with score < 40")


class StatsResponse(BaseModel):
    """Response containing overall statistics."""
    success: bool
    stats: Dict[str, Any]


class ScoringWeightsResponse(BaseModel):
    """Response containing scoring weights configuration."""
    fit_weight: float
    want_weight: float
    facet_weights: Dict[str, float]


class PolicyResponse(BaseModel):
    """Response containing result policy configuration."""
    min_fit: float
    top_k: int
    min_jd_required_coverage: Optional[float] = None


POLICY_PRESETS = {
    "strict": ResultPolicy(min_fit=70.0, min_jd_required_coverage=0.80, top_k=25),
    "balanced": ResultPolicy(min_fit=55.0, min_jd_required_coverage=0.60, top_k=50),
    "discovery": ResultPolicy(min_fit=40.0, min_jd_required_coverage=None, top_k=100),
}

_default_policy = POLICY_PRESETS["balanced"]


def get_current_policy() -> ResultPolicy:
    """Get the current result policy from database or fall back to default.

    Returns:
        ResultPolicy: The active policy, loaded from DB if available, otherwise default.
    """
    try:
        from database.models import AppSettings
        setting = db_session_scope().__enter__().query(AppSettings).filter(AppSettings.key == 'result_policy').first()
        if setting and setting.value:
            import json
            data = json.loads(setting.value)
            return ResultPolicy(
                min_fit=data.get('min_fit', _default_policy.min_fit),
                top_k=data.get('top_k', _default_policy.top_k),
                min_jd_required_coverage=data.get('min_jd_required_coverage', _default_policy.min_jd_required_coverage)
            )
    except Exception:
        pass
    return _default_policy


def set_current_policy(policy: ResultPolicy) -> None:
    """Save the current result policy to the database.

    Args:
        policy: The ResultPolicy to persist.
    """
    try:
        from database.models import AppSettings
        import json
        session = db_session_scope().__enter__()
        setting = session.query(AppSettings).filter(AppSettings.key == 'result_policy').first()
        value = json.dumps({
            'min_fit': policy.min_fit,
            'top_k': policy.top_k,
            'min_jd_required_coverage': policy.min_jd_required_coverage
        })
        if setting:
            setting.value = value
        else:
            setting = AppSettings(key='result_policy', value=value)
            session.add(setting)
        session.commit()
    except Exception:
        pass


def db_session_scope():
    """Create a database session scope for helper functions."""
    from contextlib import contextmanager
    @contextmanager
    def scope():
        db = SessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    return scope


_current_policy = get_current_policy()


# Helper functions

def decimal_to_float(val):
    """Convert Decimal to float safely."""
    if val is None:
        return 0.0
    return float(val)


# API Endpoints

@app.get("/", response_class=HTMLResponse)
def read_root():
    """
    Serve the main dashboard HTML page.
    """
    html_path = project_root / 'web' / 'templates' / 'index.html'
    if not html_path.exists():
        return HTMLResponse(content="<h1>Dashboard not found</h1><p>Please ensure web/templates/index.html exists</p>", status_code=404)
    
    with open(html_path, 'r') as f:
        return HTMLResponse(content=f.read())


@app.get("/api/matches", response_model=MatchesResponse, tags=["matches"])
def get_matches(
    status: str = Query(default="active", description="Match status: active, stale, or all"),
    min_fit: float = Query(default=None, ge=0, le=100, description="Minimum fit score filter"),
    top_k: int = Query(default=None, ge=1, le=500, description="Maximum results to return"),
    db: Session = Depends(get_db)
):
    """
    Get a list of job matches filtered by result policy.
    
    Uses the current in-memory policy settings by default:
    - min_fit: Minimum fit score to include
    - top_k: Maximum number of results to return
    
    Both can be overridden via query parameters.
    Returns matches sorted by overall score (highest first).
    """
    try:
        # Use policy defaults, allow override via query params
        effective_min_fit = min_fit if min_fit is not None else _current_policy.min_fit
        effective_top_k = top_k if top_k is not None else _current_policy.top_k
        
        # Build query
        query = db.query(JobMatch)
        
        if status != "all":
            query = query.filter(JobMatch.status == status)
        
        matches = query.filter(
            (JobMatch.fit_score >= effective_min_fit) | (JobMatch.fit_score.is_(None))
        ).order_by(desc(JobMatch.overall_score)).limit(effective_top_k).all()
        
        # Batch load all related JobPost records to avoid N+1 queries
        job_ids = [match.job_post_id for match in matches]
        jobs = db.query(JobPost).filter(JobPost.id.in_(job_ids)).all() if job_ids else []
        jobs_by_id = {str(job.id): job for job in jobs}
        
        # Format results
        results = []
        for match in matches:
            job = jobs_by_id.get(str(match.job_post_id))
            if job:
                results.append(MatchSummary(
                    match_id=str(match.id),
                    job_id=str(job.id),
                    title=job.title or "Unknown",
                    company=job.company or "Unknown",
                    location=job.location_text,
                    is_remote=job.is_remote,
                    fit_score=decimal_to_float(match.fit_score) if match.fit_score else None,
                    want_score=decimal_to_float(match.want_score) if match.want_score else None,
                    overall_score=decimal_to_float(match.overall_score),
                    base_score=decimal_to_float(match.base_score),
                    penalties=decimal_to_float(match.penalties),
                    required_coverage=decimal_to_float(match.required_coverage),
                    preferred_coverage=decimal_to_float(match.preferred_coverage),
                    match_type=match.match_type or "unknown",
                    created_at=match.created_at.isoformat() if match.created_at else None,
                    calculated_at=match.calculated_at.isoformat() if match.calculated_at else None,
                ))
        
        return MatchesResponse(success=True, count=len(results), matches=results)
        
    except Exception as e:
        logger.exception("Error fetching matches")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/matches/{match_id}", response_model=MatchDetailResponse, tags=["matches"])
def get_match_details(match_id: str, db: Session = Depends(get_db)):
    """
    Get detailed information about a specific match.
    
    Includes match metadata, job details, and requirement coverage.
    """
    try:
        match = db.query(JobMatch).get(match_id)
        if not match:
            raise HTTPException(status_code=404, detail="Match not found")
        
        job = db.query(JobPost).get(match.job_post_id)
        
        # Get requirement matches
        req_matches = db.query(JobMatchRequirement).filter(
            JobMatchRequirement.job_match_id == match_id
        ).all()
        
        requirements = [
            RequirementDetail(
                requirement_id=str(req.job_requirement_unit_id),
                evidence_text=req.evidence_text,
                evidence_section=req.evidence_section,
                similarity_score=decimal_to_float(req.similarity_score),
                is_covered=req.is_covered,
                req_type=req.req_type
            )
            for req in req_matches
        ]
        
        # Parse penalty details
        penalty_details = match.penalty_details or {}
        if isinstance(penalty_details, str):
            try:
                penalty_details = json.loads(penalty_details)
            except (json.JSONDecodeError, ValueError):
                penalty_details = {}
        
        return MatchDetailResponse(
            success=True,
            match=MatchDetail(
                match_id=str(match.id),
                resume_fingerprint=match.resume_fingerprint or "",
                fit_score=decimal_to_float(match.fit_score) if match.fit_score else None,
                want_score=decimal_to_float(match.want_score) if match.want_score else None,
                overall_score=decimal_to_float(match.overall_score),
                fit_components=match.fit_components if hasattr(match, 'fit_components') else None,
                want_components=match.want_components if hasattr(match, 'want_components') else None,
                fit_weight=decimal_to_float(match.fit_weight) if match.fit_weight else None,
                want_weight=decimal_to_float(match.want_weight) if match.want_weight else None,
                base_score=decimal_to_float(match.base_score),
                penalties=decimal_to_float(match.penalties),
                required_coverage=decimal_to_float(match.required_coverage),
                preferred_coverage=decimal_to_float(match.preferred_coverage),
                total_requirements=match.total_requirements or 0,
                matched_requirements_count=match.matched_requirements_count or 0,
                match_type=match.match_type or "unknown",
                status=match.status or "unknown",
                created_at=match.created_at.isoformat() if match.created_at else None,
                calculated_at=match.calculated_at.isoformat() if match.calculated_at else None,
                penalty_details=penalty_details,
            ),
            job=JobDetails(
                job_id=str(job.id) if job else None,
                title=job.title if job else None,
                company=job.company if job else None,
                location=job.location_text if job else None,
                is_remote=job.is_remote if job else None,
                description=job.description if job else None,
                salary_min=float(job.salary_min) if job and job.salary_min else None,
                salary_max=float(job.salary_max) if job and job.salary_max else None,
                currency=job.currency if job else None,
                min_years_experience=job.min_years_experience if job else None,
                requires_degree=job.requires_degree if job else None,
                security_clearance=job.security_clearance if job else None,
                job_level=job.job_level if job else None,
            ),
            requirements=requirements
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error fetching match details")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/stats", response_model=StatsResponse, tags=["stats"])
def get_stats(db: Session = Depends(get_db)):
    """
    Get overall statistics about matches in the database.
    
    Returns total counts and score distribution.
    """
    try:
        total_matches = db.query(JobMatch).count()
        active_matches = db.query(JobMatch).filter(JobMatch.status == 'active').count()
        
        # Score distribution
        score_dist = {
            'excellent': db.query(JobMatch).filter(JobMatch.overall_score >= 80).count(),
            'good': db.query(JobMatch).filter(
                JobMatch.overall_score >= 60,
                JobMatch.overall_score < 80
            ).count(),
            'average': db.query(JobMatch).filter(
                JobMatch.overall_score >= 40,
                JobMatch.overall_score < 60
            ).count(),
            'poor': db.query(JobMatch).filter(JobMatch.overall_score < 40).count(),
        }
        
        return StatsResponse(
            success=True,
            stats={
                'total_matches': total_matches,
                'active_matches': active_matches,
                'score_distribution': score_dist
            }
        )
        
    except Exception as e:
        logger.exception("Error fetching stats")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/config/scoring-weights", response_model=ScoringWeightsResponse, tags=["config"])
def get_scoring_weights():
    """
    Get current scoring weights configuration.

    Returns Fit/Want weights and facet weights for Want score calculation.
    """
    scorer_config = config.get('matching', {}).get('scorer', {})

    return ScoringWeightsResponse(
        fit_weight=scorer_config.get('fit_weight', 0.70),
        want_weight=scorer_config.get('want_weight', 0.30),
        facet_weights=scorer_config.get('facet_weights', {
            'remote_flexibility': 0.15,
            'compensation': 0.20,
            'learning_growth': 0.15,
            'company_culture': 0.15,
            'work_life_balance': 0.15,
            'tech_stack': 0.10,
            'visa_sponsorship': 0.10
        })
    )


@app.get("/api/v1/policy", response_model=PolicyResponse, tags=["policy"])
def get_policy():
    """
    Get current result policy configuration.

    Returns the in-memory policy settings for filtering and truncating results.
    """
    return PolicyResponse(
        min_fit=_current_policy.min_fit,
        top_k=_current_policy.top_k,
        min_jd_required_coverage=_current_policy.min_jd_required_coverage
    )


@app.put("/api/v1/policy", response_model=PolicyResponse, tags=["policy"])
def update_policy(policy: PolicyResponse):
    """
    Update result policy configuration.

    Updates persisted policy settings. Changes are stored in the database.

    - min_fit: Minimum fit score (0-100) to include in results
    - top_k: Maximum number of results to return (1-500)
    - min_jd_required_coverage: Minimum job description coverage (0-1), or null to disable
    """
    coverage = policy.min_jd_required_coverage
    if coverage is not None:
        coverage = min(1.0, max(0.0, coverage))
    new_policy = ResultPolicy(
        min_fit=min(100, max(0, policy.min_fit)),
        top_k=min(500, max(1, policy.top_k)),
        min_jd_required_coverage=coverage
    )
    set_current_policy(new_policy)
    global _current_policy
    _current_policy = new_policy
    return PolicyResponse(
        min_fit=_current_policy.min_fit,
        top_k=_current_policy.top_k,
        min_jd_required_coverage=_current_policy.min_jd_required_coverage
    )


@app.post("/api/v1/policy/preset/{preset_name}", response_model=PolicyResponse, tags=["policy"])
def apply_preset(preset_name: str):
    """
    Apply a result policy preset.

    Presets:
    - strict: min_fit=70, min_required_coverage=0.80, top_k=25
    - balanced: min_fit=55, min_required_coverage=0.60, top_k=50
    - discovery: min_fit=40, min_required_coverage=null, top_k=100
    """
    global _current_policy
    preset_name = preset_name.lower()
    if preset_name not in POLICY_PRESETS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid preset. Valid options: {', '.join(POLICY_PRESETS.keys())}"
        )
    _current_policy = POLICY_PRESETS[preset_name]
    set_current_policy(_current_policy)
    return PolicyResponse(
        min_fit=_current_policy.min_fit,
        top_k=_current_policy.top_k,
        min_jd_required_coverage=_current_policy.min_jd_required_coverage
    )


# Notification endpoints
from notification import NotificationService, NotificationPriority
from database.repository import JobRepository

def get_notification_service(db: Session = Depends(get_db)):
    """Get notification service with database session."""
    repo = JobRepository(db)
    return NotificationService(repo)

class NotificationRequest(BaseModel):
    """Request to send a notification."""
    type: str = Field(..., description="Notification type: email, slack, webhook, push")
    recipient: str = Field(..., description="Recipient (email, user ID, webhook URL)")
    subject: str = Field(..., description="Notification subject")
    body: str = Field(..., description="Notification body")
    priority: str = Field(default="normal", description="Priority: low, normal, high, urgent")

class NotificationResponse(BaseModel):
    """Response after sending notification."""
    success: bool
    notification_id: str
    message: str

class QueueStatusResponse(BaseModel):
    """Response with queue status."""
    success: bool
    status: str
    queue_length: int
    redis_connected: bool

@app.post("/api/notifications/send", response_model=NotificationResponse, tags=["notifications"])
def send_notification(
    request: NotificationRequest,
    notification_service: NotificationService = Depends(get_notification_service)
):
    """
    Send a notification via the message queue.
    
    Supports: email, slack, webhook, push
    """
    try:
        notif_type = request.type
        priority = NotificationPriority(request.priority)
        
        notification_id = notification_service.queue_notification(
            type=notif_type,
            recipient=request.recipient,
            subject=request.subject,
            body=request.body,
            priority=priority
        )
        
        return NotificationResponse(
            success=True,
            notification_id=notification_id,
            message=f"Notification queued successfully ({request.type})"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid notification type or priority: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/notifications/queue-status", response_model=QueueStatusResponse, tags=["notifications"])
def get_queue_status(
    notification_service: NotificationService = Depends(get_notification_service)
):
    """
    Get the status of the notification queue.
    
    Shows queue length and Redis connection status.
    """
    try:
        status = notification_service.get_queue_status()
        
        return QueueStatusResponse(
            success=True,
            status=status.get('status', 'unknown'),
            queue_length=status.get('queue_length', 0),
            redis_connected=status.get('redis_connected', False)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/notifications/test-match", response_model=NotificationResponse, tags=["notifications"])
def test_match_notification(
    user_id: str = Query(..., description="User ID to notify"),
    job_title: str = Query(default="Senior Python Developer", description="Job title"),
    company: str = Query(default="TechCorp", description="Company name"),
    score: float = Query(default=85.0, ge=0, le=100, description="Match score"),
    notification_service: NotificationService = Depends(get_notification_service)
):
    """
    Test sending a job match notification.
    
    Useful for testing the notification pipeline.
    """
    try:
        notification_id = notification_service.notify_new_match(
            user_id=user_id,
            match_id="test-match-id",
            job_title=job_title,
            company=company,
            score=score,
            location="Remote",
            is_remote=True
        )
        
        return NotificationResponse(
            success=True,
            notification_id=notification_id,
            message="Test match notification queued"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("JobScout Web Dashboard (FastAPI)")
    print("=" * 60)
    print(f"\nDashboard:     http://localhost:{WEB_PORT}")
    print(f"API Docs:      http://localhost:{WEB_PORT}/docs")
    print(f"Alt API Docs:  http://localhost:{WEB_PORT}/redoc")
    print(f"\nPress Ctrl+C to stop the server\n")
    
    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT)
