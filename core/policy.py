"""Shared result-policy storage and resolution."""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

from core.config_loader import ResultPolicy, load_config
from database.database import db_session_scope

logger = logging.getLogger(__name__)


def _default_policy_from_config() -> ResultPolicy:
    """Use configured matching policy as the startup fallback/default."""
    try:
        config = load_config()
        matching_config = getattr(config, "matching", None)
        policy = getattr(matching_config, "result_policy", None)
        if policy is not None:
            return ResultPolicy.model_validate(policy.model_dump())
    except Exception as exc:
        logger.warning("Could not load default result policy from config: %s", exc)

    return ResultPolicy()


POLICY_PRESETS: Dict[str, ResultPolicy] = {
    "strict": ResultPolicy(min_fit=70.0, min_jd_required_coverage=0.80, top_k=25),
    "balanced": ResultPolicy(min_fit=55.0, min_jd_required_coverage=0.60, top_k=50),
    "discovery": ResultPolicy(min_fit=40.0, min_jd_required_coverage=None, top_k=100),
}


class ResultPolicyStore:
    """DB-backed result policy store with config fallback."""

    def __init__(self):
        self._default_policy = _default_policy_from_config()

    def get_current_policy(self) -> ResultPolicy:
        return self._load_from_db()

    def update_policy(
        self,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float],
    ) -> ResultPolicy:
        self._validate(
            min_fit=min_fit,
            top_k=top_k,
            min_jd_required_coverage=min_jd_required_coverage,
        )
        new_policy = ResultPolicy(
            min_fit=min_fit,
            top_k=top_k,
            min_jd_required_coverage=min_jd_required_coverage,
        )
        self._save_to_db(new_policy)
        return new_policy

    def apply_preset(self, preset_name: str) -> ResultPolicy:
        normalized = preset_name.lower()
        if normalized not in POLICY_PRESETS:
            raise ValueError(
                f"Invalid preset '{preset_name}'. Valid options: {', '.join(POLICY_PRESETS.keys())}"
            )
        policy = POLICY_PRESETS[normalized]
        self._save_to_db(policy)
        return policy

    def get_presets(self) -> Dict[str, ResultPolicy]:
        return POLICY_PRESETS.copy()

    def _load_from_db(self) -> ResultPolicy:
        try:
            from database.models import AppSettings

            with db_session_scope() as session:
                setting = session.query(AppSettings).filter(
                    AppSettings.key == "result_policy"
                ).first()

                if setting and setting.value:
                    data = self._parse_value(setting.value)
                    return ResultPolicy(
                        min_fit=data.get("min_fit", self._default_policy.min_fit),
                        top_k=data.get("top_k", self._default_policy.top_k),
                        min_jd_required_coverage=data.get(
                            "min_jd_required_coverage",
                            self._default_policy.min_jd_required_coverage,
                        ),
                    )
        except Exception as exc:
            logger.warning("Could not load policy from database: %s", exc)

        return self._default_policy

    def _save_to_db(self, policy: ResultPolicy) -> None:
        from database.models import AppSettings

        with db_session_scope() as session:
            setting = session.query(AppSettings).filter(
                AppSettings.key == "result_policy"
            ).first()

            value = json.dumps(
                {
                    "min_fit": policy.min_fit,
                    "top_k": policy.top_k,
                    "min_jd_required_coverage": policy.min_jd_required_coverage,
                }
            )

            if setting:
                setting.value = value
            else:
                setting = AppSettings(key="result_policy", value=value)
                session.add(setting)

            session.commit()

    @staticmethod
    def _parse_value(value):
        if isinstance(value, str):
            return json.loads(value)
        return value

    @staticmethod
    def _validate(
        *,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float],
    ) -> None:
        if not (0 <= min_fit <= 100):
            raise ValueError(f"min_fit must be between 0 and 100, got {min_fit}")

        if not (1 <= top_k <= 500):
            raise ValueError(f"top_k must be between 1 and 500, got {top_k}")

        if min_jd_required_coverage is not None and not (0.0 <= min_jd_required_coverage <= 1.0):
            raise ValueError(
                "min_jd_required_coverage must be between 0.0 and 1.0, "
                f"got {min_jd_required_coverage}"
            )


_policy_store = ResultPolicyStore()


def get_result_policy_store() -> ResultPolicyStore:
    return _policy_store
