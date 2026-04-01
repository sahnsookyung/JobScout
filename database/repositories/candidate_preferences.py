from typing import Any, Optional

from sqlalchemy import select

from database.models import CandidatePreferences
from database.repositories.base import BaseRepository


class CandidatePreferencesRepository(BaseRepository):
    """Persistence helpers for per-user candidate preferences."""

    def get_preferences(self, owner_id: Any) -> Optional[CandidatePreferences]:
        stmt = select(CandidatePreferences).where(CandidatePreferences.owner_id == owner_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_or_create_preferences(self, owner_id: Any) -> CandidatePreferences:
        preferences = self.get_preferences(owner_id)
        if preferences is not None:
            return preferences

        preferences = CandidatePreferences(owner_id=owner_id)
        self.db.add(preferences)
        self.db.flush()
        return preferences
