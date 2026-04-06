"""Unit tests for database/repositories/candidate_preferences.py"""

from types import SimpleNamespace
from uuid import uuid4

from database.repositories.candidate_preferences import CandidatePreferencesRepository


class TestCandidatePreferencesRepository:
    def test_get_preferences_returns_scalar_result(self):
        owner_id = uuid4()
        expected = object()
        db = SimpleNamespace(
            execute=lambda stmt: SimpleNamespace(scalar_one_or_none=lambda: expected)
        )
        repo = CandidatePreferencesRepository(db)
        assert repo.get_preferences(owner_id) is expected

    def test_get_or_create_returns_existing_record(self):
        owner_id = uuid4()
        existing = object()
        db = SimpleNamespace(add=lambda obj: None, flush=lambda: None)
        repo = CandidatePreferencesRepository(db)
        repo.get_preferences = lambda oid: existing
        assert repo.get_or_create_preferences(owner_id) is existing

    def test_get_or_create_creates_new_record_when_none(self):
        owner_id = uuid4()
        added = []
        db = SimpleNamespace(add=lambda obj: added.append(obj), flush=lambda: None)
        repo = CandidatePreferencesRepository(db)
        repo.get_preferences = lambda oid: None

        result = repo.get_or_create_preferences(owner_id)

        assert result.owner_id == owner_id
        assert result in added
