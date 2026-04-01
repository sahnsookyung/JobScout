#!/usr/bin/env python3
"""Internal CLI for reading and updating user feature entitlements."""

from __future__ import annotations

import argparse
import json
import uuid

from database.database import db_session_scope
from database.repository import JobRepository


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage JobScout feature entitlements.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    show_parser = subparsers.add_parser("show", help="Show one entitlement row.")
    show_parser.add_argument("--owner-id", required=True, help="User UUID.")
    show_parser.add_argument("--feature-key", required=True)

    set_parser = subparsers.add_parser("set", help="Create or update one entitlement row.")
    set_parser.add_argument("--owner-id", required=True, help="User UUID.")
    set_parser.add_argument("--feature-key", required=True)
    set_parser.add_argument("--disable", action="store_true", help="Mark the entitlement disabled.")
    set_parser.add_argument("--source", default="manual-cli")
    set_parser.add_argument(
        "--json-value",
        default=None,
        help="Raw JSON payload for value_json. Overrides mode-specific helpers below.",
    )
    set_parser.add_argument(
        "--modes",
        nargs="*",
        help="Helper for fit.semantic.allowed_modes, for example: --modes cross_encoder llm",
    )
    set_parser.add_argument(
        "--preferred-mode",
        choices=["cross_encoder", "llm"],
        help="Helper for fit.semantic.preferred_mode.",
    )

    return parser.parse_args()


def _serialize(entitlement) -> dict:
    if entitlement is None:
        return {"entitlement": None}
    return {
        "id": str(entitlement.id),
        "owner_id": str(entitlement.owner_id),
        "feature_key": entitlement.feature_key,
        "enabled": bool(entitlement.enabled),
        "value_json": entitlement.value_json,
        "source": entitlement.source,
        "created_at": entitlement.created_at.isoformat() if entitlement.created_at else None,
        "updated_at": entitlement.updated_at.isoformat() if entitlement.updated_at else None,
    }


def _resolved_value(args: argparse.Namespace) -> dict | None:
    if args.json_value:
        return json.loads(args.json_value)
    if args.modes is not None:
        return {"modes": list(args.modes)}
    if args.preferred_mode:
        return {"mode": args.preferred_mode}
    return None


def main() -> int:
    args = _parse_args()
    owner_id = uuid.UUID(args.owner_id)

    with db_session_scope() as session:
        repo = JobRepository(session)
        if args.command == "show":
            entitlement = repo.get_entitlement(owner_id, args.feature_key)
            print(json.dumps(_serialize(entitlement), indent=2, sort_keys=True))
            return 0

        entitlement = repo.upsert_entitlement(
            owner_id,
            args.feature_key,
            enabled=not args.disable,
            value_json=_resolved_value(args),
            source=args.source,
        )
        print(json.dumps(_serialize(entitlement), indent=2, sort_keys=True))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
