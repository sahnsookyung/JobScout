"""Unit tests for core/ranking/policy.py — RankingPolicyStore."""

from unittest.mock import MagicMock, patch

from core.ranking.policy import RankingConfig, RankingPolicyStore, get_ranking_policy_store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg(**kw) -> RankingConfig:
    base = {"balanced_w_pref": 0.6, "balanced_w_fit": 0.4}
    base.update(kw)
    return RankingConfig(**base)


def _mock_session_scope(setting):
    """Return a context-manager mock that yields *session* and returns *setting* on query."""
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = setting
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=mock_session)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, mock_session


# ---------------------------------------------------------------------------
# RankingPolicyStore.__init__
# ---------------------------------------------------------------------------

class TestRankingPolicyStoreInit:
    def test_accepts_explicit_default_config(self):
        cfg = _cfg()
        store = RankingPolicyStore(default_config=cfg)
        assert store._default is cfg

    @patch("core.ranking.policy.RankingPolicyStore._load_default_from_yaml")
    def test_calls_load_default_when_no_config_given(self, mock_load):
        mock_load.return_value = _cfg()
        RankingPolicyStore()
        mock_load.assert_called_once()

    def test_load_default_from_yaml_returns_valid_config(self):
        """_load_default_from_yaml succeeds when config.yaml has a ranking section."""
        store = RankingPolicyStore()
        assert isinstance(store._default, RankingConfig)

    def test_load_default_from_yaml_falls_back_on_exception(self):
        with patch("core.config_loader.load_config", side_effect=RuntimeError("no config")):
            store = RankingPolicyStore()
        assert isinstance(store._default, RankingConfig)


# ---------------------------------------------------------------------------
# _load_from_db
# ---------------------------------------------------------------------------

class TestGetCurrentConfig:
    def _store(self):
        return RankingPolicyStore(default_config=_cfg(config_version="default"))

    def test_returns_config_from_db_when_setting_has_json_string(self):
        stored = _cfg(config_version="2.0.0")
        mock_setting = MagicMock()
        mock_setting.value = stored.model_dump_json()

        cm, _ = _mock_session_scope(mock_setting)
        with patch("database.database.db_session_scope", return_value=cm):
            result = self._store().get_current_config()

        assert result.config_version == "2.0.0"

    def test_returns_config_from_db_when_setting_value_is_dict(self):
        stored = _cfg(config_version="3.0.0")
        mock_setting = MagicMock()
        mock_setting.value = stored.model_dump()  # dict, not JSON string

        cm, _ = _mock_session_scope(mock_setting)
        with patch("database.database.db_session_scope", return_value=cm):
            result = self._store().get_current_config()

        assert result.config_version == "3.0.0"

    def test_falls_back_to_default_when_no_setting_row(self):
        cm, _ = _mock_session_scope(None)  # .first() returns None
        with patch("database.database.db_session_scope", return_value=cm):
            result = self._store().get_current_config()

        assert result.config_version == "default"

    def test_falls_back_to_default_on_db_exception(self):
        with patch("database.database.db_session_scope", side_effect=Exception("db down")):
            result = self._store().get_current_config()

        assert result.config_version == "default"

    def test_falls_back_to_default_when_setting_value_is_falsy(self):
        """Empty string / None value on the setting row → fallback."""
        mock_setting = MagicMock()
        mock_setting.value = ""

        cm, _ = _mock_session_scope(mock_setting)
        with patch("database.database.db_session_scope", return_value=cm):
            result = self._store().get_current_config()

        assert result.config_version == "default"


# ---------------------------------------------------------------------------
# _save_to_db / update_config
# ---------------------------------------------------------------------------

class TestUpdateConfig:
    def _store(self):
        return RankingPolicyStore(default_config=_cfg())

    def test_updates_existing_setting_value(self):
        mock_setting = MagicMock()
        cm, mock_session = _mock_session_scope(mock_setting)

        with patch("database.database.db_session_scope", return_value=cm):
            result = self._store().update_config(_cfg(config_version="updated"))

        assert result.config_version == "updated"
        assert mock_setting.value is not None
        mock_session.commit.assert_called_once()

    def test_adds_new_setting_when_none_exists(self):
        cm, mock_session = _mock_session_scope(None)

        with patch("database.database.db_session_scope", return_value=cm):
            self._store().update_config(_cfg())

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_returns_the_config_passed_in(self):
        cm, _ = _mock_session_scope(None)
        cfg = _cfg(config_version="v99")

        with patch("database.database.db_session_scope", return_value=cm):
            returned = self._store().update_config(cfg)

        assert returned is cfg


# ---------------------------------------------------------------------------
# get_ranking_policy_store singleton
# ---------------------------------------------------------------------------

class TestGetRankingPolicyStoreSingleton:
    def test_returns_same_instance_on_repeated_calls(self):
        import sys
        policy_module = sys.modules['core.ranking.policy']
        policy_module._ranking_policy_store = None  # reset
        s1 = get_ranking_policy_store()
        s2 = get_ranking_policy_store()
        assert s1 is s2
        policy_module._ranking_policy_store = None  # clean up
