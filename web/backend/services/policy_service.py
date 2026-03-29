"""Policy service - manages result filtering policies."""

from typing import Dict, Optional

from core.config_loader import ResultPolicy
from core.policy import (
    POLICY_PRESETS,
    ResultPolicyStore,
    get_result_policy_store,
)

from ..exceptions import InvalidPolicyException


class PolicyService:
    """Web-facing wrapper around the shared result policy store."""

    def __init__(self, store: ResultPolicyStore | None = None):
        self._store = store or get_result_policy_store()

    def get_current_policy(self) -> ResultPolicy:
        return self._store.get_current_policy()

    def update_policy(
        self,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float],
    ) -> ResultPolicy:
        try:
            return self._store.update_policy(
                min_fit=min_fit,
                top_k=top_k,
                min_jd_required_coverage=min_jd_required_coverage,
            )
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def apply_preset(self, preset_name: str) -> ResultPolicy:
        try:
            return self._store.apply_preset(preset_name)
        except ValueError as exc:
            raise InvalidPolicyException(str(exc)) from exc

    def get_presets(self) -> Dict[str, ResultPolicy]:
        return self._store.get_presets()


_policy_service = PolicyService()


def get_policy_service() -> PolicyService:
    """Get the global policy service instance."""
    return _policy_service
