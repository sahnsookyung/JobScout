"""
FastAPI dependencies for dependency injection.
"""

import os
import uuid
from typing import Generator

from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from database.models import User, UserAuthIdentity
from .config import get_config

DEV_BYPASS_AUTH_MODE = "dev-bypass"
ALLOWED_DEV_ENVIRONMENTS = {"development", "dev", "test"}
DEFAULT_DEV_USER_EMAIL = "dev-user@jobscout.local"
DEFAULT_DEV_USER_NAME = "JobScout Dev User"


class DatabaseManager:
    """Manages database connections and sessions."""

    def __init__(self):
        config = get_config()
        self.engine = create_engine(
            config.database.url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def get_session(self) -> Generator[Session, None, None]:
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()


_db_manager = DatabaseManager()


def get_db() -> Generator[Session, None, None]:
    yield from _db_manager.get_session()


def get_db_engine():
    """Get the database engine (for advanced use cases)."""
    return _db_manager.engine


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
        id=uuid.UUID(os.getenv("DEV_BYPASS_USER_ID", "00000000-0000-0000-0000-000000000001")),
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


def get_current_user():
    """Resolve the current authenticated user.

    In local development/tests, explicit dev bypass mode returns a seeded user.
    In non-dev environments, missing auth is a hard error.
    """
    _ensure_dev_bypass_allowed()
    auth_mode = _auth_mode()
    if auth_mode == DEV_BYPASS_AUTH_MODE:
        session = _db_manager.SessionLocal()
        try:
            user = _ensure_dev_user(session)
            session.expunge(user)
            return user
        finally:
            session.close()

    raise HTTPException(status_code=401, detail="Authentication required")
