"""Trusted attribution for optional cloud AI-budget exemptions."""

from __future__ import annotations

import os
from uuid import UUID


def admin_budget_exempt() -> bool:
    """Verify the current operation's owner against the protected admin record.

    A tenant membership, retention exemption, email claim or system owner is
    insufficient. Resolve from the database on each operation so revocation
    applies to already queued work as well as new requests.
    """
    if os.getenv("JOBSCOUT_CLOUD_ADMIN_LLM_BUDGET_EXEMPT", "false").lower() != "true":
        return False
    email = os.getenv("JOBSCOUT_CLOUD_PLATFORM_ADMIN_EMAIL", "").strip().lower()
    if not email:
        return False

    # Lazy imports keep the provider/configuration bootstrap free of DB cycles.
    from sqlalchemy import func, select

    from database.database import SessionLocal, current_database_user_id
    from database.models import SYSTEM_OWNER_ID, User

    owner_id = current_database_user_id()
    if owner_id is None:
        return False
    try:
        owner_uuid = UUID(owner_id)
    except (ValueError, TypeError, AttributeError):
        return False
    if str(owner_uuid) == str(SYSTEM_OWNER_ID):
        return False
    with SessionLocal() as session:
        return session.execute(
            select(User.id).where(
                User.id == owner_uuid,
                User.is_platform_admin.is_(True),
                User.is_active.is_(True),
                User.deletion_started_at.is_(None),
                func.lower(func.trim(User.email)) == email,
            )
        ).scalar_one_or_none() is not None
