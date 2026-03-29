"""Tests for web/backend/services/policy_service.py."""

import pytest
from unittest.mock import Mock

from core.config_loader import ResultPolicy
from web.backend.exceptions import InvalidPolicyException
from web.backend.services.policy_service import (
    POLICY_PRESETS,
    PolicyService,
    get_policy_service,
)


class TestPolicyService:
    def test_get_current_policy_delegates_to_store(self):
        store = Mock()
        store.get_current_policy.return_value = ResultPolicy(min_fit=61.0, top_k=20)

        service = PolicyService(store=store)

        policy = service.get_current_policy()

        assert policy.min_fit == 61.0
        assert policy.top_k == 20
        store.get_current_policy.assert_called_once_with()

    def test_update_policy_delegates_to_store(self):
        store = Mock()
        store.update_policy.return_value = ResultPolicy(min_fit=70.0, top_k=25, min_jd_required_coverage=0.8)

        service = PolicyService(store=store)
        policy = service.update_policy(70.0, 25, 0.8)

        assert policy.min_fit == 70.0
        store.update_policy.assert_called_once_with(
            min_fit=70.0,
            top_k=25,
            min_jd_required_coverage=0.8,
        )

    def test_update_policy_translates_validation_errors(self):
        store = Mock()
        store.update_policy.side_effect = ValueError("min_fit must be between 0 and 100")

        service = PolicyService(store=store)

        with pytest.raises(InvalidPolicyException, match="min_fit"):
            service.update_policy(-1.0, 25, 0.8)

    def test_apply_preset_delegates_to_store(self):
        store = Mock()
        store.apply_preset.return_value = POLICY_PRESETS["strict"]

        service = PolicyService(store=store)
        policy = service.apply_preset("strict")

        assert policy.min_fit == 70.0
        store.apply_preset.assert_called_once_with("strict")

    def test_apply_preset_translates_unknown_preset(self):
        store = Mock()
        store.apply_preset.side_effect = ValueError("Invalid preset 'weird'")

        service = PolicyService(store=store)

        with pytest.raises(InvalidPolicyException, match="Invalid preset"):
            service.apply_preset("weird")

    def test_get_presets_returns_store_values(self):
        store = Mock()
        store.get_presets.return_value = {"strict": POLICY_PRESETS["strict"]}

        service = PolicyService(store=store)

        presets = service.get_presets()

        assert presets["strict"].top_k == 25
        store.get_presets.assert_called_once_with()


def test_get_policy_service_returns_policy_service_instance():
    assert isinstance(get_policy_service(), PolicyService)
