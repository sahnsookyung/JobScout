#!/usr/bin/env python3
"""
JobScout Web Dashboard - FastAPI Version

A modern web application to view job matching results with automatic API documentation.

Usage:
    uv run python web/app.py
    
Then open:
    - http://localhost:5000 - Dashboard
    - http://localhost:5000/docs - API Documentation (Swagger UI)
    - http://localhost:5000/redoc - Alternative API Documentation
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from decimal import Decimal

from database.models import JobMatch, JobPost, JobMatchRequirement

# Database configuration
DB_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://user:password@localhost:5432/jobscout'
)

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
    overall_score: float = Field(ge=0, le=100)
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
                "overall_score": 85.5,
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
    overall_score: float
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


# Helper functions

def get_db_session():
    """Create database session."""
    engine = create_engine(DB_URL)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


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
    min_score: float = Query(default=0, ge=0, le=100, description="Minimum match score to include"),
    status: str = Query(default="active", description="Match status: active, stale, or all"),
    limit: int = Query(default=50, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get a list of job matches with optional filtering.
    
    Returns matches sorted by overall score (highest first).
    """
    session = get_db_session()
    try:
        # Build query
        query = session.query(JobMatch)
        
        if status != "all":
            query = query.filter(JobMatch.status == status)
        
        matches = query.filter(
            JobMatch.overall_score >= min_score
        ).order_by(desc(JobMatch.overall_score)).limit(limit).all()
        
        # Format results
        results = []
        for match in matches:
            job = session.query(JobPost).get(match.job_post_id)
            if job:
                results.append(MatchSummary(
                    match_id=str(match.id),
                    job_id=str(job.id),
                    title=job.title or "Unknown",
                    company=job.company or "Unknown",
                    location=job.location_text,
                    is_remote=job.is_remote,
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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@app.get("/api/matches/{match_id}", response_model=MatchDetailResponse, tags=["matches"])
def get_match_details(match_id: str):
    """
    Get detailed information about a specific match.
    
    Includes match metadata, job details, and requirement coverage.
    """
    session = get_db_session()
    try:
        match = session.query(JobMatch).get(match_id)
        if not match:
            raise HTTPException(status_code=404, detail="Match not found")
        
        job = session.query(JobPost).get(match.job_post_id)
        
        # Get requirement matches
        req_matches = session.query(JobMatchRequirement).filter(
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
            import json
            try:
                penalty_details = json.loads(penalty_details)
            except:
                penalty_details = {}
        
        return MatchDetailResponse(
            success=True,
            match=MatchDetail(
                match_id=str(match.id),
                resume_fingerprint=match.resume_fingerprint or "",
                overall_score=decimal_to_float(match.overall_score),
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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


@app.get("/api/stats", response_model=StatsResponse, tags=["stats"])
def get_stats():
    """
    Get overall statistics about matches in the database.
    
    Returns total counts and score distribution.
    """
    session = get_db_session()
    try:
        total_matches = session.query(JobMatch).count()
        active_matches = session.query(JobMatch).filter(JobMatch.status == 'active').count()
        
        # Score distribution
        score_dist = {
            'excellent': session.query(JobMatch).filter(JobMatch.overall_score >= 80).count(),
            'good': session.query(JobMatch).filter(
                JobMatch.overall_score >= 60,
                JobMatch.overall_score < 80
            ).count(),
            'average': session.query(JobMatch).filter(
                JobMatch.overall_score >= 40,
                JobMatch.overall_score < 60
            ).count(),
            'poor': session.query(JobMatch).filter(JobMatch.overall_score < 40).count(),
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
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


# Notification endpoints
from notification import NotificationService, NotificationPriority

notification_service = NotificationService()

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
def send_notification(request: NotificationRequest):
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
def get_queue_status():
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
    score: float = Query(default=85.0, ge=0, le=100, description="Match score")
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
    print(f"\nDashboard:     http://localhost:5000")
    print(f"API Docs:      http://localhost:5000/docs")
    print(f"Alt API Docs:  http://localhost:5000/redoc")
    print(f"\nPress Ctrl+C to stop the server\n")
    
    uvicorn.run(app, host="0.0.0.0", port=5000)
