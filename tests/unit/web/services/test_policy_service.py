"""
Tests for web/backend/services/policy_service.py

Covers: get_current_policy, update_policy (validation + save),
apply_preset, get_presets, _load_from_db, _save_to_db, _parse_value,
get_policy_service.
"""

import json
import pytest
from unittest.mock import MagicMock, Mock, patch

from web.backend.services.policy_service import (
    PolicyService,
    ResultPolicy,
    POLICY_PRESETS,
    get_policy_service,
)
from web.backend.exceptions import InvalidPolicyException


def _make_db(setting=None):
    """Return (mock_session, mock_cm) for patching db_session_scope."""
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = setting
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=session)
    cm.__exit__ = MagicMock(return_value=False)
    return session, cm


# ---------------------------------------------------------------------------
# ResultPolicy dataclass
# ---------------------------------------------------------------------------

class TestResultPolicy:
    def test_defaults(self):
        p = ResultPolicy()
        assert p.min_fit == 55.0
        assert p.top_k == 50
        assert p.min_jd_required_coverage == 0.6

    def test_custom_values(self):
        p = ResultPolicy(min_fit=70.0, top_k=25, min_jd_required_coverage=None)
        assert p.min_fit == 70.0
        assert p.top_k == 25
        assert p.min_jd_required_coverage is None


# ---------------------------------------------------------------------------
# POLICY_PRESETS constant
# ---------------------------------------------------------------------------

class TestPolicyPresets:
    def test_strict(self):
        p = POLICY_PRESETS["strict"]
        assert p.min_fit == 70.0
        assert p.top_k == 25
        assert p.min_jd_required_coverage == 0.80

    def test_balanced(self):
        p = POLICY_PRESETS["balanced"]
        assert p.min_fit == 55.0
        assert p.top_k == 50
        assert p.min_jd_required_coverage == 0.60

    def test_discovery(self):
        p = POLICY_PRESETS["discovery"]
        assert p.min_fit == 40.0
        assert p.top_k == 100
        assert p.min_jd_required_coverage is None


# ---------------------------------------------------------------------------
# get_current_policy
# ---------------------------------------------------------------------------

class TestGetCurrentPolicy:
    def test_returns_policy_instance(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        policy = svc.get_current_policy()
        assert isinstance(policy, ResultPolicy)


# ---------------------------------------------------------------------------
# update_policy — validation
# ---------------------------------------------------------------------------

class TestUpdatePolicyValidation:
    @pytest.fixture
    def svc(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            return PolicyService()

    def test_min_fit_below_zero_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="min_fit"):
            svc.update_policy(min_fit=-1.0, top_k=50, min_jd_required_coverage=None)

    def test_min_fit_above_100_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="min_fit"):
            svc.update_policy(min_fit=100.1, top_k=50, min_jd_required_coverage=None)

    def test_top_k_zero_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="top_k"):
            svc.update_policy(min_fit=50.0, top_k=0, min_jd_required_coverage=None)

    def test_top_k_above_500_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="top_k"):
            svc.update_policy(min_fit=50.0, top_k=501, min_jd_required_coverage=None)

    def test_coverage_below_zero_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="min_jd_required_coverage"):
            svc.update_policy(min_fit=50.0, top_k=50, min_jd_required_coverage=-0.1)

    def test_coverage_above_one_raises(self, svc):
        with pytest.raises(InvalidPolicyException, match="min_jd_required_coverage"):
            svc.update_policy(min_fit=50.0, top_k=50, min_jd_required_coverage=1.1)


# ---------------------------------------------------------------------------
# update_policy — valid paths
# ---------------------------------------------------------------------------

class TestUpdatePolicyValid:
    def test_valid_update_returns_policy(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.update_policy(min_fit=70.0, top_k=25, min_jd_required_coverage=0.8)
        assert policy.min_fit == 70.0
        assert policy.top_k == 25
        assert policy.min_jd_required_coverage == 0.8

    def test_coverage_none_is_valid(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.update_policy(min_fit=50.0, top_k=50, min_jd_required_coverage=None)
        assert policy.min_jd_required_coverage is None

    def test_boundary_values_pass(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.update_policy(min_fit=0.0, top_k=1, min_jd_required_coverage=0.0)
        assert policy.min_fit == 0.0
        assert policy.top_k == 1

    def test_update_sets_current_policy(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            svc.update_policy(min_fit=80.0, top_k=10, min_jd_required_coverage=0.9)
        assert svc.get_current_policy().min_fit == 80.0


# ---------------------------------------------------------------------------
# apply_preset
# ---------------------------------------------------------------------------

class TestApplyPreset:
    def test_apply_strict(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.apply_preset("strict")
        assert policy.min_fit == 70.0
        assert policy.top_k == 25

    def test_apply_balanced(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.apply_preset("balanced")
        assert policy.min_fit == 55.0

    def test_apply_discovery(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.apply_preset("discovery")
        assert policy.min_jd_required_coverage is None

    def test_case_normalized(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            policy = svc.apply_preset("STRICT")
        assert policy.min_fit == 70.0

    def test_unknown_preset_raises(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        with pytest.raises(InvalidPolicyException, match="not_a_preset"):
            svc.apply_preset("not_a_preset")

    def test_apply_updates_current_policy(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            svc.apply_preset("strict")
        assert svc.get_current_policy().min_fit == 70.0


# ---------------------------------------------------------------------------
# get_presets
# ---------------------------------------------------------------------------

class TestGetPresets:
    def test_returns_all_three(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        presets = svc.get_presets()
        assert set(presets.keys()) == {"strict", "balanced", "discovery"}

    def test_returns_copy_not_original(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        p = svc.get_presets()
        p["injected"] = ResultPolicy()
        assert "injected" not in svc.get_presets()


# ---------------------------------------------------------------------------
# _load_from_db
# ---------------------------------------------------------------------------

class TestLoadFromDb:
    def test_existing_setting_loaded(self):
        setting = Mock()
        setting.value = json.dumps({"min_fit": 65.0, "top_k": 30, "min_jd_required_coverage": 0.7})
        _, cm = _make_db(setting=setting)
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        policy = svc.get_current_policy()
        assert policy.min_fit == 65.0
        assert policy.top_k == 30
        assert policy.min_jd_required_coverage == 0.7

    def test_no_setting_falls_back_to_default(self):
        _, cm = _make_db(setting=None)
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
        assert svc.get_current_policy().min_fit == 55.0  # balanced default

    def test_db_error_falls_back_to_default(self):
        with patch("web.backend.services.policy_service.db_session_scope", side_effect=Exception("DB error")):
            svc = PolicyService()
        assert svc.get_current_policy().min_fit == 55.0


# ---------------------------------------------------------------------------
# _save_to_db (via update_policy)
# ---------------------------------------------------------------------------

class TestSaveToDb:
    def test_creates_new_setting_when_absent(self):
        session, cm = _make_db(setting=None)
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            svc.update_policy(min_fit=60.0, top_k=40, min_jd_required_coverage=0.5)
        session.add.assert_called_once()
        session.commit.assert_called()

    def test_updates_existing_setting(self):
        existing = Mock()
        existing.value = json.dumps({"min_fit": 55.0, "top_k": 50, "min_jd_required_coverage": 0.6})
        session, cm = _make_db(setting=existing)
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            svc = PolicyService()
            svc.update_policy(min_fit=70.0, top_k=25, min_jd_required_coverage=0.8)
        saved = json.loads(existing.value)
        assert saved["min_fit"] == 70.0
        assert saved["top_k"] == 25
        session.add.assert_not_called()


# ---------------------------------------------------------------------------
# _parse_value
# ---------------------------------------------------------------------------

class TestParseValue:
    @pytest.fixture
    def svc(self):
        _, cm = _make_db()
        with patch("web.backend.services.policy_service.db_session_scope", return_value=cm):
            return PolicyService()

    def test_parses_json_string(self, svc):
        result = svc._parse_value('{"min_fit": 60.0}')
        assert result == {"min_fit": 60.0}

    def test_passes_through_dict(self, svc):
        d = {"min_fit": 60.0}
        result = svc._parse_value(d)
        assert result is d


# ---------------------------------------------------------------------------
# get_policy_service
# ---------------------------------------------------------------------------

class TestGetPolicyService:
    def test_returns_policy_service_instance(self):
        svc = get_policy_service()
        assert isinstance(svc, PolicyService)
