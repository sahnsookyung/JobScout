"""DB-backed runtime store for the active RankingConfig."""

from __future__ import annotations

import json
import logging
from typing import Optional

from core.config_loader import RankingConfig  # canonical definition lives in config_loader

logger = logging.getLogger(__name__)


class RankingPolicyStore:
    """DB-backed store for the active RankingConfig.

    Follows the same pattern as ResultPolicyStore in core/policy.py.
    The config is serialised as JSON into app_settings with key='ranking_config'.
    On any DB error the in-memory YAML default is returned (fallback-to-default,
    not a strict fail-closed guarantee — the service continues with startup config).
    """

    _SETTINGS_KEY = "ranking_config"

    def __init__(self, default_config: Optional[RankingConfig] = None) -> None:
        self._default = default_config or self._load_default_from_yaml()

    def get_current_config(self) -> RankingConfig:
        return self._load_from_db()

    def update_config(self, config: RankingConfig) -> RankingConfig:
        self._save_to_db(config)
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

    def _load_from_db(self) -> RankingConfig:
        try:
            from database.models import AppSettings
            from database.database import db_session_scope
            with db_session_scope() as session:
                setting = (
                    session.query(AppSettings)
                    .filter(AppSettings.key == self._SETTINGS_KEY)
                    .first()
                )
                if setting and setting.value:
                    raw = setting.value
                    data = json.loads(raw) if isinstance(raw, str) else raw
                    return RankingConfig(**data)
        except Exception as exc:
            logger.warning(
                "Could not load ranking config from DB, using in-memory default: %s", exc
            )
        return self._default

    def _save_to_db(self, config: RankingConfig) -> None:
        from database.models import AppSettings
        from database.database import db_session_scope
        with db_session_scope() as session:
            setting = (
                session.query(AppSettings)
                .filter(AppSettings.key == self._SETTINGS_KEY)
                .first()
            )
            value = config.model_dump_json()
            if setting:
                setting.value = value
            else:
                session.add(AppSettings(key=self._SETTINGS_KEY, value=value))
            session.commit()


_ranking_policy_store: Optional[RankingPolicyStore] = None


def get_ranking_policy_store() -> RankingPolicyStore:
    global _ranking_policy_store
    if _ranking_policy_store is None:
        _ranking_policy_store = RankingPolicyStore()
    return _ranking_policy_store
