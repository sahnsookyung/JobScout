from database.database import _resolve_database_url


def test_resolve_database_url_uses_configured_value(monkeypatch) -> None:
    monkeypatch.setattr(
        "database.database.load_config_data",
        lambda: {"database": {"url": "postgresql://cfg:pass@db-host:5432/jobscout"}},
    )

    assert _resolve_database_url() == "postgresql://cfg:pass@db-host:5432/jobscout"


def test_resolve_database_url_falls_back_to_default(monkeypatch) -> None:
    monkeypatch.setattr("database.database.load_config_data", lambda: {})

    assert _resolve_database_url() == "postgresql://user:password@localhost:5432/jobscout"
