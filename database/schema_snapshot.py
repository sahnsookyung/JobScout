"""Deterministic PostgreSQL schema snapshot utilities for the current DB contract."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

SNAPSHOT_PATH = Path(__file__).with_name("schema_snapshot.json")

_SYSTEM_TABLE_NAMES = frozenset({"schema_migrations"})


def capture(target: Engine | Connection) -> dict[str, Any]:
    """Capture a full public-schema snapshot via direct catalog queries."""
    if isinstance(target, Engine):
        with target.connect() as conn:
            return _capture_from_connection(conn)
    return _capture_from_connection(target)


def dump(snapshot: dict[str, Any]) -> str:
    """Serialize the snapshot to a stable JSON string."""
    return json.dumps(snapshot, indent=2, sort_keys=True) + "\n"


def write(snapshot: dict[str, Any], path: Path = SNAPSHOT_PATH) -> None:
    path.write_text(dump(snapshot))


def load(path: Path = SNAPSHOT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _capture_from_connection(conn: Connection) -> dict[str, Any]:
    return {
        "extensions": _extensions(conn),
        "enums": _enums(conn),
        "tables": _tables(conn),
        "indexes": _indexes(conn),
        "constraints": _constraints(conn),
    }


def _extensions(conn: Connection) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT extname
            FROM pg_extension
            WHERE extname NOT IN ('plpgsql')
            ORDER BY extname
            """
        )
    ).fetchall()
    return [row.extname for row in rows]


def _enums(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT t.typname AS name,
                   array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
            FROM pg_type t
            JOIN pg_enum e ON e.enumtypid = t.oid
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = 'public'
            GROUP BY t.typname
            ORDER BY t.typname
            """
        )
    ).fetchall()
    return [{"name": row.name, "labels": list(row.labels)} for row in rows]


def _tables(conn: Connection) -> dict[str, Any]:
    rows = conn.execute(
        text(
            """
            SELECT c.table_name AS table_name,
                   c.column_name AS column_name,
                   c.ordinal_position AS ord,
                   c.is_nullable AS is_nullable,
                   c.data_type AS data_type,
                   c.udt_name AS udt_name,
                   c.character_maximum_length AS max_len,
                   c.numeric_precision AS num_prec,
                   c.numeric_scale AS num_scale,
                   c.column_default AS default_expr
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema = c.table_schema
             AND t.table_name = c.table_name
            WHERE c.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_name, c.ordinal_position
            """
        )
    ).fetchall()

    tables: dict[str, Any] = {}
    for row in rows:
        if row.table_name in _SYSTEM_TABLE_NAMES:
            continue
        entry = tables.setdefault(row.table_name, {"columns": []})
        entry["columns"].append(
            {
                "name": row.column_name,
                "type": _normalize_type(
                    row.data_type,
                    row.udt_name,
                    row.max_len,
                    row.num_prec,
                    row.num_scale,
                ),
                "nullable": row.is_nullable == "YES",
                "default": _normalize_default(row.default_expr),
            }
        )
    for entry in tables.values():
        entry["columns"].sort(key=lambda column: column["name"])
    return tables


def _indexes(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT c.relname AS index_name,
                   t.relname AS table_name,
                   am.amname AS access_method,
                   pg_get_indexdef(i.indexrelid) AS indexdef,
                   pg_get_expr(i.indpred, i.indrelid) AS predicate,
                   i.indisunique AS is_unique,
                   c.reloptions AS reloptions
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_am am ON am.oid = c.relam
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_constraint pc ON pc.conindid = i.indexrelid
            WHERE n.nspname = 'public'
              AND t.relname NOT IN :system_tables
              AND pc.oid IS NULL
            ORDER BY t.relname, c.relname
            """
        ).bindparams(system_tables=tuple(_SYSTEM_TABLE_NAMES) or ("",))
    ).fetchall()
    return [
        {
            "name": row.index_name,
            "table": row.table_name,
            "access_method": row.access_method,
            "unique": row.is_unique,
            "predicate": row.predicate,
            "reloptions": sorted(row.reloptions) if row.reloptions else None,
            "definition": _normalize_indexdef(row.indexdef),
        }
        for row in rows
    ]


def _constraints(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT tc.table_name AS table_name,
                   tc.constraint_name AS name,
                   tc.constraint_type AS type,
                   pg_get_constraintdef(pgc.oid) AS definition
            FROM information_schema.table_constraints tc
            JOIN pg_constraint pgc ON pgc.conname = tc.constraint_name
            JOIN pg_class cls ON cls.oid = pgc.conrelid AND cls.relname = tc.table_name
            JOIN pg_namespace n ON n.oid = cls.relnamespace AND n.nspname = tc.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name NOT IN :system_tables
            ORDER BY tc.table_name, tc.constraint_name
            """
        ).bindparams(system_tables=tuple(_SYSTEM_TABLE_NAMES) or ("",))
    ).fetchall()
    return [
        {
            "table": row.table_name,
            "name": row.name,
            "type": row.type,
            "definition": row.definition,
        }
        for row in rows
    ]


def _normalize_type(data_type: str, udt_name: str, max_len, num_prec, num_scale) -> str:
    if udt_name == "vector":
        return "vector"
    if data_type in {"character varying", "character"}:
        return f"{data_type}({max_len})" if max_len else data_type
    if data_type == "numeric" and num_prec is not None:
        if num_scale:
            return f"numeric({num_prec},{num_scale})"
        return f"numeric({num_prec})"
    if data_type == "USER-DEFINED":
        return udt_name
    return data_type


def _normalize_default(expr) -> Any:
    if expr is None:
        return None
    return str(expr).strip()


def _normalize_indexdef(indexdef: str) -> str:
    return indexdef.rstrip(";")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Overwrite the checked-in snapshot file")
    parser.add_argument(
        "--url",
        default=os.environ.get("DATABASE_URL"),
        help="Database URL (defaults to $DATABASE_URL)",
    )
    parser.add_argument(
        "--path",
        default=str(SNAPSHOT_PATH),
        help="Snapshot file path (defaults to database/schema_snapshot.json)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if not args.url:
        print("No database URL provided (use --url or DATABASE_URL)", file=sys.stderr)
        return 2

    engine = create_engine(args.url)
    try:
        snapshot = capture(engine)
    finally:
        engine.dispose()

    rendered = dump(snapshot)
    if args.write:
        Path(args.path).write_text(rendered)
        print(f"Wrote snapshot to {args.path}")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
