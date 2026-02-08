#!/usr/bin/env python3
"""
Stats endpoints - view match statistics.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..dependencies import get_db
from ..services.policy_service import get_policy_service
from ..models.responses import StatsResponse
from database.models import JobMatch

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """
    Get overall statistics about matches in the database.
    
    Returns total counts and score distribution.
    """
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    min_fit = current_policy.min_fit
    
    # Base query
    base_query = db.query(JobMatch)
    
    # Total matches
    total_matches = base_query.count()
    
    # Hidden count
    hidden_count = base_query.filter(JobMatch.is_hidden.is_(True)).count()
    
    # Below threshold count
    below_threshold_count = base_query.filter(
        (JobMatch.fit_score < min_fit) | (JobMatch.fit_score.is_(None)),
        JobMatch.is_hidden.is_(False)
    ).count()
    
    # Active matches (visible and above threshold)
    active_matches = total_matches - hidden_count - below_threshold_count
    
    # Score distribution
    score_dist = {
        'excellent': base_query.filter(JobMatch.overall_score >= 80).count(),
        'good': base_query.filter(
            JobMatch.overall_score >= 60,
            JobMatch.overall_score < 80
        ).count(),
        'average': base_query.filter(
            JobMatch.overall_score >= 40,
            JobMatch.overall_score < 60
        ).count(),
        'poor': base_query.filter(JobMatch.overall_score < 40).count(),
    }
    
    return StatsResponse(
        success=True,
        stats={
            'total_matches': total_matches,
            'active_matches': active_matches,
            'hidden_count': hidden_count,
            'below_threshold_count': below_threshold_count,
            'min_fit_threshold': min_fit,
            'score_distribution': score_dist
        }
    )
