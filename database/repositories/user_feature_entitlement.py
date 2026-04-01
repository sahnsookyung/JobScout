from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select

from database.models import UserFeatureEntitlement
from database.repositories.base import BaseRepository


class UserFeatureEntitlementRepository(BaseRepository):
    def get_entitlement(self, owner_id: Any, feature_key: str) -> Optional[UserFeatureEntitlement]:
        stmt = select(UserFeatureEntitlement).where(
            UserFeatureEntitlement.owner_id == owner_id,
            UserFeatureEntitlement.feature_key == feature_key,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def upsert_entitlement(
        self,
        owner_id: Any,
        feature_key: str,
        *,
        enabled: bool = True,
        value_json: Optional[dict] = None,
        source: Optional[str] = None,
    ) -> UserFeatureEntitlement:
        entitlement = self.get_entitlement(owner_id, feature_key)
        if entitlement is None:
            entitlement = UserFeatureEntitlement(
                owner_id=owner_id,
                feature_key=feature_key,
            )
            self.db.add(entitlement)

        entitlement.enabled = enabled
        entitlement.value_json = value_json
        entitlement.source = source
        self.db.flush()
        return entitlement
