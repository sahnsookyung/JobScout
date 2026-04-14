"""Helpers for ensuring docker compose env-file prerequisites exist."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def ensure_compose_env_file(project_root: Path | None = None) -> bool:
    """Ensure docker compose env_file=.env resolves for local and CI runs.

    Returns True when this helper created `.env` and the caller should remove
    it during teardown if appropriate.
    """

    root = project_root or DEFAULT_PROJECT_ROOT
    dotenv_path = root / ".env"

    if dotenv_path.exists():
        return False

    # Keep E2E tests deterministic. Copying .env.example can reintroduce
    # live provider endpoints or credentials into local test runs.
    dotenv_path.write_text("", encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ensure docker compose env_file=.env exists for E2E runs."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root containing .env/.env.example",
    )
    args = parser.parse_args()

    created = ensure_compose_env_file(args.project_root.resolve())
    if created:
        print(f"Created {args.project_root.resolve() / '.env'}")
    else:
        print(f"Using existing {args.project_root.resolve() / '.env'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
