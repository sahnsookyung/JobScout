"""Policy service - manages result filtering policies."""

from typing import Dict, Optional

from core.config_loader import RankingConfig, ResultPolicy
from core.policy import (
    LlmJudgePolicy,
    ResultPolicyStore,
    get_result_policy_store,
)
from core.ranking.policy import RankingPolicyStore, get_ranking_policy_store

from ..exceptions import InvalidPolicyException


class PolicyService:
    """Web-facing wrapper around the shared result policy store."""

    def __init__(
        self,
        store: ResultPolicyStore | None = None,
        ranking_store: RankingPolicyStore | None = None,
    ):
        self._store = store or get_result_policy_store()
        self._ranking_store = ranking_store or get_ranking_policy_store()

    def get_current_policy(self, owner_id: object | None = None) -> ResultPolicy:
        return self._store.get_current_policy(owner_id)

    def get_ranking_config(self, owner_id: object | None = None) -> RankingConfig:
        return self._ranking_store.get_current_config(owner_id)

    def update_ranking_config(
        self,
        config: RankingConfig,
        *,
        owner_id: object | None = None,
    ) -> RankingConfig:
        try:
            return self._ranking_store.update_config(config, owner_id=owner_id)
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def get_llm_judge_policy(self, owner_id: object | None = None) -> LlmJudgePolicy:
        return self._store.get_llm_judge_policy(owner_id)

    def update_policy(
        self,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float],
        *,
        owner_id: object | None = None,
    ) -> ResultPolicy:
        try:
            return self._store.update_policy(
                min_fit=min_fit,
                top_k=top_k,
                min_jd_required_coverage=min_jd_required_coverage,
                owner_id=owner_id,
            )
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def update_llm_judge_policy(
        self,
        *,
        owner_id: object | None,
        enabled: Optional[bool] = None,
        auto_enqueue_enabled: Optional[bool] = None,
        top_n: Optional[int] = None,
    ) -> LlmJudgePolicy:
        try:
            return self._store.update_llm_judge_policy(
                owner_id=owner_id,
                enabled=enabled,
                auto_enqueue_enabled=auto_enqueue_enabled,
                top_n=top_n,
            )
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def apply_preset(
        self,
        preset_name: str,
        *,
        owner_id: object | None = None,
    ) -> ResultPolicy:
        try:
            return self._store.apply_preset(preset_name, owner_id=owner_id)
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def get_presets(self) -> Dict[str, ResultPolicy]:
        return self._store.get_presets()


_policy_service = PolicyService()


def get_policy_service() -> PolicyService:
    """Get the global policy service instance."""
    return _policy_service
