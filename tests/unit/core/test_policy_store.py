"""Tests for core/policy.py."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from core.config_loader import ResultPolicy
from core.policy import POLICY_PRESETS, ResultPolicyStore


def _make_db(setting=None):
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = setting
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return session, cm


class TestResultPolicyStore:
    def test_get_current_policy_uses_db_value_when_present(self):
        setting = Mock()
        setting.value = json.dumps({"min_fit": 65.0, "top_k": 30, "min_jd_required_coverage": 0.7})
        _, cm = _make_db(setting=setting)

        with patch("core.policy.db_session_scope", return_value=cm), \
             patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy(
                min_fit=55.0,
                top_k=50,
                min_jd_required_coverage=0.6,
            )
            store = ResultPolicyStore()
            policy = store.get_current_policy()

            assert policy.min_fit == 65.0
            assert policy.top_k == 30
            assert policy.min_jd_required_coverage == 0.7

    def test_get_current_policy_falls_back_to_config_default(self):
        _, cm = _make_db(setting=None)

        with patch("core.policy.db_session_scope", return_value=cm), \
             patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy(
                min_fit=58.0,
                top_k=45,
                min_jd_required_coverage=0.65,
            )
            store = ResultPolicyStore()
            policy = store.get_current_policy()

            assert policy.min_fit == 58.0
            assert policy.top_k == 45
            assert policy.min_jd_required_coverage == 0.65

    def test_get_current_policy_refreshes_from_db_each_call(self):
        session, cm = _make_db(setting=None)
        setting = Mock()
        setting.value = json.dumps({"min_fit": 77.0, "top_k": 15, "min_jd_required_coverage": 0.9})
        session.query.return_value.filter.return_value.first.side_effect = [None, setting]

        with patch("core.policy.db_session_scope", return_value=cm), \
             patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy(
                min_fit=55.0,
                top_k=50,
                min_jd_required_coverage=0.6,
            )
            store = ResultPolicyStore()
            first = store.get_current_policy()
            second = store.get_current_policy()

            assert first.min_fit == 55.0
            assert second.min_fit == 77.0

    def test_update_policy_validates_and_saves(self):
        session, cm = _make_db(setting=None)

        with patch("core.policy.db_session_scope", return_value=cm), \
             patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy()
            store = ResultPolicyStore()
            policy = store.update_policy(70.0, 25, 0.8)

        assert policy.min_fit == 70.0
        session.add.assert_called_once()
        session.commit.assert_called()

    @pytest.mark.parametrize(
        ("min_fit", "top_k", "coverage", "error_match"),
        [
            (-1.0, 25, 0.8, "min_fit"),
            (50.0, 0, 0.8, "top_k"),
            (50.0, 25, 1.1, "min_jd_required_coverage"),
        ],
    )
    def test_update_policy_rejects_invalid_values(self, min_fit, top_k, coverage, error_match):
        with patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy()
            store = ResultPolicyStore()

        with pytest.raises(ValueError, match=error_match):
            store.update_policy(min_fit, top_k, coverage)

    def test_apply_preset_saves_preset(self):
        _, cm = _make_db(setting=None)

        with patch("core.policy.db_session_scope", return_value=cm), \
             patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy()
            store = ResultPolicyStore()
            policy = store.apply_preset("strict")

        assert policy.min_fit == POLICY_PRESETS["strict"].min_fit

    def test_apply_preset_rejects_unknown_values(self):
        with patch("core.policy.load_config") as mock_load_config:
            mock_load_config.return_value.matching.result_policy = ResultPolicy()
            store = ResultPolicyStore()

        with pytest.raises(ValueError, match="Invalid preset"):
            store.apply_preset("weird")
