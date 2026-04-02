import argparse
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "manage_feature_entitlement.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("manage_feature_entitlement_script", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_resolved_value_prefers_raw_json():
    script = _load_script_module()
    args = argparse.Namespace(
        json_value='{"mode": "llm"}',
        modes=["cross_encoder"],
        preferred_mode="cross_encoder",
    )

    assert script._resolved_value(args) == {"mode": "llm"}


def test_resolved_value_supports_modes_helper():
    script = _load_script_module()
    args = argparse.Namespace(
        json_value=None,
        modes=["cross_encoder", "llm"],
        preferred_mode=None,
    )

    assert script._resolved_value(args) == {"modes": ["cross_encoder", "llm"]}


def test_serialize_none_entitlement():
    script = _load_script_module()

    assert script._serialize(None) == {"entitlement": None}


def test_main_show_prints_entitlement_json(monkeypatch, capsys):
    script = _load_script_module()
    fake_entitlement = SimpleNamespace(
        id="1",
        owner_id="owner",
        feature_key="fit.semantic.allowed_modes",
        enabled=True,
        value_json={"modes": ["cross_encoder"]},
        source="manual-cli",
        created_at=None,
        updated_at=None,
    )
    repo = MagicMock()
    repo.get_entitlement.return_value = fake_entitlement
    session = MagicMock()

    class _Ctx:
        def __enter__(self):
            return session

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        script,
        "_parse_args",
        lambda: argparse.Namespace(
            command="show",
            owner_id="00000000-0000-0000-0000-000000000001",
            feature_key="fit.semantic.allowed_modes",
        ),
    )
    monkeypatch.setattr(script, "db_session_scope", lambda: _Ctx())
    monkeypatch.setattr(script, "JobRepository", lambda _session: repo)

    exit_code = script.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["feature_key"] == "fit.semantic.allowed_modes"
    repo.get_entitlement.assert_called_once()


def test_main_set_upserts_entitlement(monkeypatch, capsys):
    script = _load_script_module()
    fake_entitlement = SimpleNamespace(
        id="1",
        owner_id="owner",
        feature_key="fit.semantic.preferred_mode",
        enabled=True,
        value_json={"mode": "llm"},
        source="manual-cli",
        created_at=None,
        updated_at=None,
    )
    repo = MagicMock()
    repo.upsert_entitlement.return_value = fake_entitlement
    session = MagicMock()

    class _Ctx:
        def __enter__(self):
            return session

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        script,
        "_parse_args",
        lambda: argparse.Namespace(
            command="set",
            owner_id="00000000-0000-0000-0000-000000000001",
            feature_key="fit.semantic.preferred_mode",
            disable=False,
            source="manual-cli",
            json_value=None,
            modes=None,
            preferred_mode="llm",
        ),
    )
    monkeypatch.setattr(script, "db_session_scope", lambda: _Ctx())
    monkeypatch.setattr(script, "JobRepository", lambda _session: repo)

    exit_code = script.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["value_json"] == {"mode": "llm"}
    repo.upsert_entitlement.assert_called_once()
