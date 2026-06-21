"""
FastAPI dependencies for dependency injection.
"""

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Generator

from fastapi import HTTPException, Request
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from core.auth import (
    DEFAULT_DEV_USER_EMAIL,
    DEFAULT_DEV_USER_NAME,
    _auth_mode,
    _ensure_dev_bypass_allowed,
    _ensure_dev_user,
)
from .config import get_config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TenantContext:
    tenant_id: uuid.UUID | None = None
    source: str = "none"


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
        self.session_local = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def get_session(self) -> Generator[Session, None, None]:
        session = self.session_local()
        try:
            yield session
        finally:
            session.close()


_db_manager = DatabaseManager()


def _fallback_dev_user() -> SimpleNamespace:
    """Return a non-persistent dev user when local Postgres is unavailable."""
    email = os.getenv("DEV_BYPASS_EMAIL", DEFAULT_DEV_USER_EMAIL).strip().lower()
    return SimpleNamespace(
        id=uuid.UUID(
            os.getenv(
                "DEV_BYPASS_USER_ID",
                "00000000-0000-0000-0000-000000000001",
            )
        ),
        email=email,
        display_name=os.getenv("DEV_BYPASS_NAME", DEFAULT_DEV_USER_NAME),
        is_active=True,
        email_verified_at=datetime.now(timezone.utc),
    )


def get_db() -> Generator[Session, None, None]:
    yield from _db_manager.get_session()


def get_db_engine():
    """Get the database engine (for advanced use cases)."""
    return _db_manager.engine


def _parse_tenant_id(value: object, *, source: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError) as exc:
        detail = (
            "Trusted tenant context must be a UUID."
            if source == "request.state.tenant_id"
            else "X-Tenant-Id must be a UUID."
        )
        raise HTTPException(status_code=400, detail=detail) from exc


def get_tenant_context(request: Request) -> TenantContext:
    """Resolve tenant context from trusted request state or the tenant header."""
    state_tenant_id = getattr(request.state, "tenant_id", None)
    if state_tenant_id is not None:
        return TenantContext(
            tenant_id=_parse_tenant_id(state_tenant_id, source="request.state.tenant_id"),
            source="state",
        )

    header_tenant_id = (request.headers.get("X-Tenant-Id") or "").strip()
    if not header_tenant_id:
        return TenantContext()
    return TenantContext(
        tenant_id=_parse_tenant_id(header_tenant_id, source="X-Tenant-Id"),
        source="header",
    )


def get_current_user():
    """Resolve the current authenticated user.

    In local development/tests, explicit dev bypass mode returns a seeded user.
    In non-dev environments, missing auth is a hard error.
    """
    _ensure_dev_bypass_allowed()
    auth_mode = _auth_mode()
    if auth_mode == "dev-bypass":
        session = _db_manager.session_local()
        try:
            user = _ensure_dev_user(session)
            session.expunge(user)
            return user
        except SQLAlchemyError as exc:
            logger.warning(
                "Falling back to in-memory dev-bypass user because the database is unavailable: %s",
                exc.__class__.__name__,
            )
            return _fallback_dev_user()
        finally:
            session.close()

    raise HTTPException(status_code=401, detail="Authentication required")
