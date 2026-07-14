"""DB-backed runtime store for the active RankingConfig."""

from __future__ import annotations

import json
import logging
from typing import Optional

from core.config_loader import RankingConfig  # canonical definition lives in config_loader

logger = logging.getLogger(__name__)


class RankingPolicyStore:
    """DB-backed store for the active RankingConfig.

    Owner overrides are stored on CandidatePreferences. AppSettings remains a
    read-only fallback for installations that predate owner-scoped policy.
    On any DB error the in-memory YAML default is returned.
    """

    _SETTINGS_KEY = "ranking_config"

    def __init__(self, default_config: Optional[RankingConfig] = None) -> None:
        self._default = default_config or self._load_default_from_yaml()

    def get_current_config(self, owner_id: object | None = None) -> RankingConfig:
        return self._load_from_db(owner_id)

    def update_config(
        self,
        config: RankingConfig,
        *,
        owner_id: object | None = None,
    ) -> RankingConfig:
        self._save_to_db(config, owner_id=owner_id)
        return config

    # ------------------------------------------------------------------ private

    def _load_default_from_yaml(self) -> RankingConfig:
        try:
            from core.config_loader import load_config
            cfg = load_config()
            ranking = getattr(cfg, "ranking", None)
            if ranking is not None:
                return RankingConfig.model_validate(ranking.model_dump())
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not load default ranking config from YAML: %s", exc)
        return RankingConfig()

    def _load_from_db(self, owner_id: object | None = None) -> RankingConfig:
        try:
            from database.models import AppSettings, CandidatePreferences
            from database.database import db_session_scope
            with db_session_scope() as session:
                if owner_id is not None:
                    preferences = session.query(CandidatePreferences).filter(
                        CandidatePreferences.owner_id == owner_id
                    ).first()
                    if preferences and isinstance(preferences.ranking_config, dict):
                        return RankingConfig(**preferences.ranking_config)

                setting = session.query(AppSettings).filter(
                    AppSettings.key == self._SETTINGS_KEY
                ).first()
                if setting and setting.value:
                    data = (
                        json.loads(setting.value)
                        if isinstance(setting.value, str)
                        else setting.value
                    )
                    return RankingConfig(**data)
        except Exception as exc:
            logger.warning(
                "Could not load ranking config from DB, using in-memory default: %s", exc
            )
        return self._default

    def _save_to_db(
        self,
        config: RankingConfig,
        *,
        owner_id: object | None,
    ) -> None:
        if owner_id is None:
            raise ValueError("owner_id is required to update ranking configuration")

        from database.models import CandidatePreferences
        from database.database import db_session_scope
        with db_session_scope() as session:
            preferences = (
                session.query(CandidatePreferences)
                .filter(CandidatePreferences.owner_id == owner_id)
                .first()
            )
            if preferences is None:
                preferences = CandidatePreferences(owner_id=owner_id)
                session.add(preferences)
            preferences.ranking_config = config.model_dump(mode="json")
            preferences.revision = int(preferences.revision or 0) + 1
            session.commit()


_ranking_policy_store: Optional[RankingPolicyStore] = None


def get_ranking_policy_store() -> RankingPolicyStore:
    global _ranking_policy_store
    if _ranking_policy_store is None:
        _ranking_policy_store = RankingPolicyStore()
    return _ranking_policy_store
