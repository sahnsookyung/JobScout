from unittest.mock import Mock, patch

from database import schema_snapshot


def test_write_and_load_snapshot_round_trip(tmp_path):
    path = tmp_path / "snapshot.json"
    snapshot = {"tables": {"job_post": {"columns": []}}}

    schema_snapshot.write(snapshot, path)

    assert path.read_text().endswith("\n")
    assert schema_snapshot.load(path) == snapshot


def test_normalize_type_handles_length_numeric_user_defined_and_vector():
    assert (
        schema_snapshot._normalize_type(
            "character varying",
            "varchar",
            255,
            None,
            None,
        )
        == "character varying(255)"
    )
    assert schema_snapshot._normalize_type("numeric", "numeric", None, 10, 2) == "numeric(10,2)"
    assert schema_snapshot._normalize_type("numeric", "numeric", None, 10, 0) == "numeric(10)"
    assert schema_snapshot._normalize_type("USER-DEFINED", "job_status", None, None, None) == "job_status"
    assert schema_snapshot._normalize_type("USER-DEFINED", "vector", None, None, None) == "vector"
    assert schema_snapshot._normalize_type("integer", "int4", None, None, None) == "integer"


def test_normalize_default_and_index_definition():
    assert schema_snapshot._normalize_default(None) is None
    assert schema_snapshot._normalize_default("  now()  ") == "now()"
    assert schema_snapshot._normalize_indexdef("CREATE INDEX idx ON table;") == "CREATE INDEX idx ON table"


def test_parse_args_uses_database_url_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://env/db")

    args = schema_snapshot._parse_args(["--write", "--path", "custom.json"])

    assert args.write is True
    assert args.url == "postgresql://env/db"
    assert args.path == "custom.json"


def test_main_reports_missing_database_url(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with patch("database.schema_snapshot.sys.argv", ["schema_snapshot"]):
        result = schema_snapshot.main()

    captured = capsys.readouterr()
    assert result == 2
    assert "No database URL provided" in captured.err


def test_main_writes_snapshot_and_disposes_engine(tmp_path, capsys):
    path = tmp_path / "snapshot.json"
    engine = Mock()
    snapshot = {"extensions": ["vector"]}

    with patch("database.schema_snapshot.create_engine", return_value=engine) as create_engine, patch(
        "database.schema_snapshot.capture",
        return_value=snapshot,
    ) as capture:
        result = schema_snapshot.main(
            [
                "--url",
                "postgresql://example/db",
                "--write",
                "--path",
                str(path),
            ],
        )

    assert result == 0
    create_engine.assert_called_once_with("postgresql://example/db")
    capture.assert_called_once_with(engine)
    engine.dispose.assert_called_once_with()
    assert path.read_text() == schema_snapshot.dump(snapshot)
    assert f"Wrote snapshot to {path}" in capsys.readouterr().out


def test_main_prints_snapshot_when_not_writing(capsys):
    engine = Mock()
    snapshot = {"tables": {}}

    with patch("database.schema_snapshot.create_engine", return_value=engine), patch(
        "database.schema_snapshot.capture",
        return_value=snapshot,
    ):
        result = schema_snapshot.main(["--url", "postgresql://example/db"])

    assert result == 0
    assert capsys.readouterr().out == schema_snapshot.dump(snapshot)
    engine.dispose.assert_called_once_with()
