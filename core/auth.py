"""Shared auth helpers for backend and internal services."""

import os
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from database.models import User, UserAuthIdentity

DEV_BYPASS_AUTH_MODE = "dev-bypass"
ALLOWED_DEV_ENVIRONMENTS = {"development", "dev", "test"}
DEFAULT_DEV_USER_EMAIL = "dev-user@jobscout.local"
DEFAULT_DEV_USER_NAME = "JobScout Dev User"


def _current_environment() -> str:
    return (
        os.getenv("JOBSCOUT_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or "development"
    ).strip().lower()


def _auth_mode() -> str:
    return os.getenv("AUTH_MODE", DEV_BYPASS_AUTH_MODE).strip().lower()


def _ensure_dev_bypass_allowed() -> None:
    environment = _current_environment()
    auth_mode = _auth_mode()
    if auth_mode != DEV_BYPASS_AUTH_MODE:
        return
    if environment not in ALLOWED_DEV_ENVIRONMENTS:
        raise RuntimeError(
            "AUTH_MODE=dev-bypass is only allowed in development/test environments"
        )


def _ensure_dev_user(session: Session) -> User:
    email = os.getenv("DEV_BYPASS_EMAIL", DEFAULT_DEV_USER_EMAIL).strip().lower()
    stmt = select(User).where(User.email == email)
    user = session.execute(stmt).scalar_one_or_none()
    if user is not None:
        return user

    user = User(
        id=uuid.UUID(
            os.getenv(
                "DEV_BYPASS_USER_ID",
                "00000000-0000-0000-0000-000000000001",
            )
        ),
        email=email,
        display_name=os.getenv("DEV_BYPASS_NAME", DEFAULT_DEV_USER_NAME),
        is_active=True,
    )
    session.add(user)
    session.flush()
    session.add(
        UserAuthIdentity(
            user_id=user.id,
            provider="password",
            provider_subject=f"dev-bypass:{email}",
            email=email,
            email_normalized=email,
            email_verified=True,
        )
    )
    session.commit()
    session.refresh(user)
    return user
