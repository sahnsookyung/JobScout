from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select

from database.models import UserFeatureCapability
from database.repositories.base import BaseRepository


class UserFeatureCapabilityRepository(BaseRepository):
    def get_capability(self, owner_id: Any, feature_key: str) -> Optional[UserFeatureCapability]:
        stmt = select(UserFeatureCapability).where(
            UserFeatureCapability.owner_id == owner_id,
            UserFeatureCapability.feature_key == feature_key,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def upsert_capability(
        self,
        owner_id: Any,
        feature_key: str,
        *,
        enabled: bool = True,
        value_json: Optional[dict] = None,
        source: Optional[str] = None,
    ) -> UserFeatureCapability:
        capability = self.get_capability(owner_id, feature_key)
        if capability is None:
            capability = UserFeatureCapability(
                owner_id=owner_id,
                feature_key=feature_key,
            )
            self.db.add(capability)

        capability.enabled = enabled
        capability.value_json = value_json
        capability.source = source
        self.db.flush()
        return capability
