"""Candidate preference endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..dependencies import get_current_user, get_db
from ..models.requests import CandidatePreferencesUpdateRequest
from ..models.responses import CandidatePreferencesResponse
from ..services.candidate_preferences_service import CandidatePreferencesService

router = APIRouter(tags=["candidate-preferences"])


def get_candidate_preferences_service(
    db: Annotated[Session, Depends(get_db)],
) -> CandidatePreferencesService:
    return CandidatePreferencesService(db)


@router.get("/api/v1/candidate-preferences", response_model=CandidatePreferencesResponse)
def get_candidate_preferences(
    service: Annotated[CandidatePreferencesService, Depends(get_candidate_preferences_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    return CandidatePreferencesResponse(**service.get_preferences(user))


@router.put("/api/v1/candidate-preferences", response_model=CandidatePreferencesResponse)
def update_candidate_preferences(
    request: CandidatePreferencesUpdateRequest,
    service: Annotated[CandidatePreferencesService, Depends(get_candidate_preferences_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    payload = {
        "remote_mode": request.remote_mode,
        "target_locations": request.target_locations,
        "visa_sponsorship_required": request.visa_sponsorship_required,
        "salary_min": request.salary_min,
        "employment_types": request.employment_types,
        "soft_preferences": request.soft_preferences,
        "preference_mode": request.preference_mode,
    }
    return CandidatePreferencesResponse(**service.update_preferences(user, payload))
