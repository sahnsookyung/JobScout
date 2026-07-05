"""Tests for core/policy.py."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from core.config_loader import LlmJudgeRuntimeConfig, ResultPolicy
from core.policy import POLICY_PRESETS, ResultPolicyStore


def _make_db(setting=None):
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = setting
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return session, cm

def _make_config(
    *,
    judge_enabled=True,
    llm_enabled=True,
    base_url="https://llm.local",
    model="judge-model",
    top_n_default=5,
    top_n_max=10,
    auto_enqueue_enabled=False,
):
    return SimpleNamespace(
        matching=SimpleNamespace(
            result_policy=ResultPolicy(),
            llm_judge=SimpleNamespace(
                enabled=judge_enabled,
                auto_enqueue_enabled=auto_enqueue_enabled,
                top_n_default=top_n_default,
                top_n_max=top_n_max,
                runtime=SimpleNamespace(
                    base_url=base_url,
                    model=model,
                    api_key="judge-key" if llm_enabled else None,
                    api_secret=None,
                    headers=None,
                ),
            ),
            scorer=SimpleNamespace(
                semantic_fit=SimpleNamespace(
                    llm=SimpleNamespace(
                        enabled=llm_enabled,
                        base_url=base_url,
                        model=model,
                    )
                )
            ),
        )
    )


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

    def test_get_llm_judge_policy_uses_available_config_defaults(self):
        with patch("core.policy.load_config", return_value=_make_config(top_n_default=12, top_n_max=10)):
            store = ResultPolicyStore()
            policy = store.get_llm_judge_policy(owner_id=None)

        assert policy.enabled is True
        assert policy.auto_enqueue_enabled is False
        assert policy.available is True
        assert policy.top_n == 10
        assert policy.top_n_max == 10

    def test_get_llm_judge_policy_marks_unavailable_without_provider(self):
        with patch("core.policy.load_config", return_value=_make_config(base_url="")):
            store = ResultPolicyStore()
            policy = store.get_llm_judge_policy(owner_id=None)

        assert policy.enabled is False
        assert policy.available is False
        assert policy.auto_enqueue_enabled is False

    def test_get_llm_judge_policy_uses_configured_provider_chain_availability(self):
        with patch.dict("os.environ", {"NVIDIA_API_KEY": "nvidia-key"}, clear=True):
            config = SimpleNamespace(
                matching=SimpleNamespace(
                    result_policy=ResultPolicy(),
                    llm_judge=SimpleNamespace(
                        enabled=True,
                        top_n_default=5,
                        top_n_max=10,
                        runtime=LlmJudgeRuntimeConfig(),
                    ),
                )
            )

        with patch("core.policy.load_config", return_value=config):
            store = ResultPolicyStore()
            policy = store.get_llm_judge_policy(owner_id=None)

        assert policy.enabled is True
        assert policy.available is True
        assert policy.auto_enqueue_enabled is False

    def test_get_llm_judge_policy_merges_owner_setting_and_caps_top_n(self):
        capability = Mock()
        capability.enabled = True
        capability.value_json = {"top_n": 99, "auto_enqueue_enabled": True, "revision": 7}
        _, cm = _make_db(setting=capability)

        with patch("core.policy.load_config", return_value=_make_config(top_n_default=4, top_n_max=8)), \
             patch("core.policy.db_session_scope", return_value=cm):
            store = ResultPolicyStore()
            policy = store.get_llm_judge_policy(owner_id="user-1")

        assert policy.enabled is True
        assert policy.auto_enqueue_enabled is True
        assert policy.top_n == 8
        assert policy.revision == 7

    def test_get_llm_judge_policy_uses_config_auto_enqueue_default_when_missing(self):
        capability = Mock()
        capability.enabled = True
        capability.value_json = {"top_n": 5, "revision": 2}
        _, cm = _make_db(setting=capability)

        with patch(
            "core.policy.load_config",
            return_value=_make_config(auto_enqueue_enabled=True),
        ), patch("core.policy.db_session_scope", return_value=cm):
            store = ResultPolicyStore()
            policy = store.get_llm_judge_policy(owner_id="user-1")

        assert policy.auto_enqueue_enabled is True

    def test_update_llm_judge_policy_persists_owner_capability(self):
        session, cm = _make_db(setting=None)

        with patch("core.policy.load_config", return_value=_make_config(top_n_default=4, top_n_max=8)), \
             patch("core.policy.db_session_scope", return_value=cm):
            store = ResultPolicyStore()
            policy = store.update_llm_judge_policy(
                owner_id="user-1",
                enabled=True,
                auto_enqueue_enabled=True,
                top_n=99,
            )

        assert policy.enabled is True
        assert policy.auto_enqueue_enabled is True
        assert policy.top_n == 8
        assert policy.revision == 1
        session.add.assert_called_once()
        added = session.add.call_args.args[0]
        assert added.feature_key == "match.llm_judge"
        assert added.value_json == {
            "top_n": 8,
            "auto_enqueue_enabled": True,
            "revision": 1,
        }
        assert added.source == "user"
        session.commit.assert_called()
