"""Ranking configuration model and optional DB-backed runtime store."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class RankingConfig(BaseModel):
    """Immutable config snapshot for one ranking evaluation.

    Retrieve-stage bounds
    ─────────────────────
    max_ranking_candidates — how many DB rows are fetched into the ranking pool,
        ordered by fit_score DESC.  This is the intentional scaling boundary:
        candidates outside the pool are excluded before ranking, so this value
        must be large enough to contain every result the user might care about.
        Default 500 is appropriate for pre-production volumes; raise for larger
        job databases.

    Response bounds
    ───────────────
    default_top_k — returned when the caller does not specify top_k.
    max_top_k     — ceiling; caller-requested top_k is silently capped here.

    Balanced weights
    ────────────────
    balanced_w_pref + balanced_w_fit must equal 1.0 (validated at init).
    Initial values (0.6 / 0.4) are a starting point, not permanent policy.
    Re-evaluate against user behaviour data after rollout and update via config.
    """

    config_version: str = "1.0.0"
    active_default_mode: Literal["preference_first", "fit_first", "balanced"] = "balanced"

    balanced_w_pref: float = Field(default=0.6, ge=0.0, le=1.0)
    balanced_w_fit: float = Field(default=0.4, ge=0.0, le=1.0)

    stable_tie_break_key: Literal["job_id", "match_id"] = "match_id"

    # Retrieve-stage bound — explicit, config-driven scaling policy.
    max_ranking_candidates: int = Field(default=500, ge=10, le=10_000)

    # Response bounds.
    default_top_k: int = Field(default=25, ge=1, le=500)
    max_top_k: int = Field(default=100, ge=1, le=1_000)

    explanation_labels: Dict[str, str] = Field(
        default_factory=lambda: {
            "preference_first": "Sorted by your soft preference match",
            "fit_first": "Sorted by skill & requirement fit",
            "balanced": "Balanced blend of preference and fit",
        }
    )

    def model_post_init(self, __context: Any) -> None:
        del __context
        total = round(self.balanced_w_pref + self.balanced_w_fit, 10)
        if abs(total - 1.0) > 1e-9:
            raise ValueError(
                f"ranking.balanced_w_pref + ranking.balanced_w_fit must equal 1.0, "
                f"got {self.balanced_w_pref} + {self.balanced_w_fit} = {total}"
            )

    def label_for_mode(self, mode: str) -> str:
        return self.explanation_labels.get(mode, mode)

    def effective_top_k(self, requested: Optional[int]) -> int:
        """Return the effective top_k, applying default and max cap."""
        k = requested if requested is not None else self.default_top_k
        return min(k, self.max_top_k)


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
