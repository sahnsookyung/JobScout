"""Shared result-policy storage and resolution."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from core.config_loader import ResultPolicy, load_config
from database.database import db_session_scope

logger = logging.getLogger(__name__)

LLM_JUDGE_FEATURE_KEY = "match.llm_judge"


@dataclass(frozen=True)
class LlmJudgePolicy:
    enabled: bool
    auto_enqueue_enabled: bool
    top_n: int
    top_n_max: int
    available: bool
    unavailable_reason: str = "available"
    revision: int = 0


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

def _llm_judge_config():
    try:
        config = load_config()
        matching_config = getattr(config, "matching", None)
        judge_config = getattr(matching_config, "llm_judge", None)
        runtime_config = getattr(judge_config, "runtime", None)
        return judge_config, runtime_config
    except Exception as exc:
        logger.warning("Could not load LLM judge config: %s", exc)
        return None, None


def _is_local_llm_endpoint(base_url: str) -> bool:
    lowered = base_url.lower()
    return any(
        host in lowered
        for host in ("localhost", "127.0.0.1", "host.docker.internal", "ollama")
    )


def _llm_judge_availability(judge_config=None, runtime_config=None) -> tuple[bool, str]:
    if judge_config is None or runtime_config is None:
        judge_config, runtime_config = _llm_judge_config()
    if judge_config is None:
        return False, "config_missing"
    if not getattr(judge_config, "enabled", False):
        return False, "disabled"
    if runtime_config is None:
        return False, "runtime_missing"

    try:
        from core.llm.provider_chain import configured_provider_entries

        if configured_provider_entries(runtime_config):
            return True, "available"
    except Exception as exc:
        logger.warning("Could not inspect LLM judge provider chain: %s", exc)

    base_url = str(getattr(runtime_config, "base_url", "") or "").strip()
    if not base_url:
        return False, "base_url_missing"
    if not str(getattr(runtime_config, "model", "") or "").strip():
        return False, "model_missing"
    has_auth = bool(
        getattr(runtime_config, "api_key", None)
        or getattr(runtime_config, "api_secret", None)
        or getattr(runtime_config, "headers", None)
        or _is_local_llm_endpoint(base_url)
    )
    if not has_auth:
        return False, "credentials_missing"
    return True, "available"


def _llm_judge_available(judge_config=None, llm_config=None) -> bool:
    return _llm_judge_availability(judge_config, llm_config)[0]


def _default_llm_judge_policy() -> LlmJudgePolicy:
    judge_config, runtime_config = _llm_judge_config()
    if judge_config is None:
        return LlmJudgePolicy(
            enabled=False,
            auto_enqueue_enabled=False,
            top_n=5,
            top_n_max=10,
            available=False,
            unavailable_reason="config_missing",
        )
    top_n_max = int(getattr(judge_config, "top_n_max", 10) or 10)
    top_n = min(int(getattr(judge_config, "top_n_default", 5) or 5), top_n_max)
    available, unavailable_reason = _llm_judge_availability(judge_config, runtime_config)
    return LlmJudgePolicy(
        enabled=bool(getattr(judge_config, "enabled", False)) and available,
        auto_enqueue_enabled=bool(getattr(judge_config, "auto_enqueue_enabled", False)),
        top_n=top_n,
        top_n_max=top_n_max,
        available=available,
        unavailable_reason=unavailable_reason,
    )


POLICY_PRESETS: Dict[str, ResultPolicy] = {
    "strict": ResultPolicy(min_fit=70.0, min_jd_required_coverage=0.80, top_k=25),
    "balanced": ResultPolicy(min_fit=55.0, min_jd_required_coverage=0.60, top_k=50),
    "discovery": ResultPolicy(min_fit=40.0, min_jd_required_coverage=None, top_k=100),
}


class ResultPolicyStore:
    """DB-backed result policy store with config fallback."""

    def __init__(self):
        self._default_policy = _default_policy_from_config()

    def get_current_policy(self, owner_id: object | None = None) -> ResultPolicy:
        return self._load_from_db(owner_id)

    def get_llm_judge_policy(self, owner_id: object | None = None) -> LlmJudgePolicy:
        default_policy = _default_llm_judge_policy()
        if owner_id is None:
            return default_policy

        try:
            from database.models import UserFeatureCapability

            with db_session_scope() as session:
                capability = session.query(UserFeatureCapability).filter(
                    UserFeatureCapability.owner_id == owner_id,
                    UserFeatureCapability.feature_key == LLM_JUDGE_FEATURE_KEY,
                ).first()
                if capability is None:
                    return default_policy

                value = capability.value_json if isinstance(capability.value_json, dict) else {}
                top_n = self._clamp_llm_top_n(
                    value.get("top_n", default_policy.top_n),
                    default_policy.top_n_max,
                )
                auto_enqueue_enabled = bool(
                    value.get(
                        "auto_enqueue_enabled",
                        default_policy.auto_enqueue_enabled,
                    )
                )
                revision = int(value.get("revision", 0) or 0)
                return LlmJudgePolicy(
                    enabled=bool(capability.enabled) and default_policy.available,
                    auto_enqueue_enabled=auto_enqueue_enabled,
                    top_n=top_n,
                    top_n_max=default_policy.top_n_max,
                    available=default_policy.available,
                    unavailable_reason=default_policy.unavailable_reason,
                    revision=revision,
                )
        except Exception as exc:
            logger.warning("Could not load LLM judge policy from database: %s", exc)
            return default_policy

    def update_llm_judge_policy(
        self,
        *,
        owner_id: object | None,
        enabled: Optional[bool] = None,
        auto_enqueue_enabled: Optional[bool] = None,
        top_n: Optional[int] = None,
    ) -> LlmJudgePolicy:
        current = self.get_llm_judge_policy(owner_id)
        next_enabled = current.enabled if enabled is None else bool(enabled)
        next_auto_enqueue_enabled = (
            current.auto_enqueue_enabled
            if auto_enqueue_enabled is None
            else bool(auto_enqueue_enabled)
        )
        next_top_n = current.top_n if top_n is None else self._clamp_llm_top_n(top_n, current.top_n_max)

        if owner_id is None:
            return LlmJudgePolicy(
                enabled=next_enabled and current.available,
                auto_enqueue_enabled=next_auto_enqueue_enabled,
                top_n=next_top_n,
                top_n_max=current.top_n_max,
                available=current.available,
                unavailable_reason=current.unavailable_reason,
                revision=current.revision,
            )

        from database.models import UserFeatureCapability

        with db_session_scope() as session:
            capability = session.query(UserFeatureCapability).filter(
                UserFeatureCapability.owner_id == owner_id,
                UserFeatureCapability.feature_key == LLM_JUDGE_FEATURE_KEY,
            ).first()
            next_revision = current.revision + 1
            value_json = {
                "top_n": next_top_n,
                "auto_enqueue_enabled": next_auto_enqueue_enabled,
                "revision": next_revision,
            }
            if capability is None:
                capability = UserFeatureCapability(
                    owner_id=owner_id,
                    feature_key=LLM_JUDGE_FEATURE_KEY,
                )
                session.add(capability)

            capability.enabled = next_enabled
            capability.value_json = value_json
            capability.source = "user"
            session.commit()

        return LlmJudgePolicy(
            enabled=next_enabled and current.available,
            auto_enqueue_enabled=next_auto_enqueue_enabled,
            top_n=next_top_n,
            top_n_max=current.top_n_max,
            available=current.available,
            unavailable_reason=current.unavailable_reason,
            revision=next_revision,
        )

    def update_policy(
        self,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float],
        *,
        owner_id: object | None = None,
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
        self._save_to_db(new_policy, owner_id=owner_id)
        return new_policy

    def apply_preset(
        self,
        preset_name: str,
        *,
        owner_id: object | None = None,
    ) -> ResultPolicy:
        normalized = preset_name.lower()
        if normalized not in POLICY_PRESETS:
            raise ValueError(
                f"Invalid preset '{preset_name}'. Valid options: {', '.join(POLICY_PRESETS.keys())}"
            )
        policy = POLICY_PRESETS[normalized]
        self._save_to_db(policy, owner_id=owner_id)
        return policy

    def get_presets(self) -> Dict[str, ResultPolicy]:
        return POLICY_PRESETS.copy()

    def _load_from_db(self, owner_id: object | None = None) -> ResultPolicy:
        try:
            from database.models import AppSettings, CandidatePreferences

            with db_session_scope() as session:
                if owner_id is not None:
                    preferences = session.query(CandidatePreferences).filter(
                        CandidatePreferences.owner_id == owner_id
                    ).first()
                    if preferences and isinstance(preferences.result_policy, dict):
                        return self._policy_from_mapping(preferences.result_policy)

                setting = session.query(AppSettings).filter(
                    AppSettings.key == "result_policy"
                ).first()

                if setting and setting.value:
                    return self._policy_from_mapping(self._parse_value(setting.value))
        except Exception as exc:
            logger.warning("Could not load policy from database: %s", exc)

        return self._default_policy

    def _save_to_db(
        self,
        policy: ResultPolicy,
        *,
        owner_id: object | None,
    ) -> None:
        if owner_id is None:
            raise ValueError("owner_id is required to update result policy")

        from database.models import CandidatePreferences

        with db_session_scope() as session:
            preferences = session.query(CandidatePreferences).filter(
                CandidatePreferences.owner_id == owner_id
            ).first()
            if preferences is None:
                preferences = CandidatePreferences(owner_id=owner_id)
                session.add(preferences)

            preferences.result_policy = {
                "min_fit": policy.min_fit,
                "top_k": policy.top_k,
                "min_jd_required_coverage": policy.min_jd_required_coverage,
            }
            preferences.revision = int(preferences.revision or 0) + 1

            session.commit()

    def _policy_from_mapping(self, data: dict) -> ResultPolicy:
        return ResultPolicy(
            min_fit=data.get("min_fit", self._default_policy.min_fit),
            top_k=data.get("top_k", self._default_policy.top_k),
            min_jd_required_coverage=data.get(
                "min_jd_required_coverage",
                self._default_policy.min_jd_required_coverage,
            ),
        )

    @staticmethod
    def _parse_value(value):
        if isinstance(value, str):
            return json.loads(value)
        return value

    @staticmethod
    def _clamp_llm_top_n(value, top_n_max: int) -> int:
        try:
            parsed = int(value)
        except Exception as exc:
            raise ValueError("llm_judge_top_n must be an integer") from exc
        if parsed <= 0:
            raise ValueError("llm_judge_top_n must be positive")
        return min(parsed, int(top_n_max))

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
