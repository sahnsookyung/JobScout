"""Integration coverage for candidate preference settings and mode resolution."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from web.backend.config import get_config as get_web_config
from database.models import CandidatePreferences, User, UserAuthIdentity

pytestmark = [
    pytest.mark.integration,
    pytest.mark.db,
]

OWNER_ID = UUID("00000000-0000-0000-0000-000000000001")
OWNER_SUBJECT = "dev-bypass:dev-user@jobscout.local"


@pytest.fixture
def candidate_preferences_env(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.setenv("AUTH_MODE", "dev-bypass")
    monkeypatch.setenv("JOBSCOUT_ENV", "test")
    monkeypatch.setenv("DEV_BYPASS_USER_ID", str(OWNER_ID))

    from core.llm.fake_service import FakeLLMService
    from services.scorer_matcher.preference_semantics import LLMPreferenceParser
    
    def fake_build_parser(config):
        return LLMPreferenceParser(FakeLLMService())
    
    monkeypatch.setattr(
        "web.backend.services.candidate_preferences_service.build_preference_parser",
        fake_build_parser,
    )

    engine = create_engine(test_db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    get_web_config.cache_clear()

    yield SessionLocal

    engine.dispose()


@pytest.fixture(autouse=True)
def clear_candidate_preferences_tables(candidate_preferences_env):
    session = candidate_preferences_env()
    try:
        session.query(CandidatePreferences).delete()
        session.query(UserAuthIdentity).delete()
        session.query(User).delete()
        session.commit()
    finally:
        session.close()


def _seed_user(SessionLocal):
    session = SessionLocal()
    try:
        user = User(
            id=OWNER_ID,
            email="dev-user@jobscout.local",
            display_name="JobScout Dev User",
            is_active=True,
        )
        session.add(user)
        session.flush()
        session.add(
            UserAuthIdentity(
                user_id=OWNER_ID,
                provider="password",
                provider_subject=OWNER_SUBJECT,
                email="dev-user@jobscout.local",
                email_normalized="dev-user@jobscout.local",
                email_verified=True,
            )
        )
        session.commit()
    finally:
        session.close()


def _build_client(SessionLocal):
    from web.backend.dependencies import get_current_user, get_db
    from web.backend.routers.candidate_preferences import router

    @contextlib.contextmanager
    def local_session():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    def override_get_db():
        with local_session() as session:
            yield session

    app = FastAPI()
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=OWNER_ID)
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def test_candidate_preferences_api_round_trip(candidate_preferences_env):
    _seed_user(candidate_preferences_env)
    client = _build_client(candidate_preferences_env)

    get_response = client.get("/api/v1/candidate-preferences")
    assert get_response.status_code == 200
    initial = get_response.json()
    assert initial["remote_mode"] == "any"
    assert initial["allowed_preference_modes"] == ["semantic_rerank"]
    assert initial["effective_preference_mode"] == "semantic_rerank"

    update_response = client.put(
        "/api/v1/candidate-preferences",
        json={
            "remote_mode": "remote",
            "target_locations": ["Remote"],
            "visa_sponsorship_required": False,
            "salary_min": 120000,
            "employment_types": ["Full-time"],
            "soft_preferences": "Mentorship and modern backend teams",
            "preference_mode": "llm_judge",
        },
    )

    assert update_response.status_code == 200
    payload = update_response.json()
    assert payload["remote_mode"] == "remote"
    assert payload["target_locations"] == ["Remote"]
    assert payload["preference_mode"] == "semantic_rerank"
    assert payload["effective_preference_mode"] == "semantic_rerank"
    assert payload["soft_preference_summary"] == "Mentorship, Modern engineering, Backend systems"

    session = candidate_preferences_env()
    try:
        stored = session.execute(
            select(CandidatePreferences).where(CandidatePreferences.owner_id == OWNER_ID)
        ).scalar_one()
        assert stored.preference_mode == "semantic_rerank"
        assert stored.preference_profile is not None
        assert stored.preference_profile["raw_text"] == "Mentorship and modern backend teams"
        assert stored.soft_preference_summary == "Mentorship, Modern engineering, Backend systems"
    finally:
        session.close()
