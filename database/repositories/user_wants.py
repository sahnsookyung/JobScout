import logging
from typing import List, Optional, Any
from sqlalchemy import select

from database.models import UserWants
from database.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class UserWantsRepository(BaseRepository):
    def save_user_wants(
        self,
        user_id: Any,
        wants_text: str,
        embedding: List[float],
        facet_key: Optional[str] = None
    ) -> UserWants:
        user_want = UserWants(
            owner_id=user_id,
            wants_text=wants_text,
            embedding=embedding,
            facet_key=facet_key
        )
        self.db.add(user_want)
        return user_want

    def get_user_wants_embeddings(
        self,
        user_id: Any
    ) -> List[List[float]]:
        stmt = select(UserWants.embedding).where(UserWants.owner_id == user_id)
        results = self.db.execute(stmt).scalars().all()
        return list(results)
