import contextlib
import threading
from contextvars import ContextVar

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from core.config_loader import load_config_data

DEFAULT_DATABASE_URL = "postgresql://user:password@localhost:5432/jobscout"
_engine_lock = threading.Lock()
RLS_USER_INFO_KEY = "jobscout.user_id"
RLS_TENANT_INFO_KEY = "jobscout.tenant_id"
_worker_user_id: ContextVar[str | None] = ContextVar("jobscout_worker_user_id", default=None)
_worker_tenant_id: ContextVar[str | None] = ContextVar("jobscout_worker_tenant_id", default=None)


class ContextSession(Session):
    """Session carrying transaction-scoped identity for PostgreSQL RLS."""


def _apply_database_context(connection, session: Session) -> None:
    for setting_name in (RLS_USER_INFO_KEY, RLS_TENANT_INFO_KEY):
        setting_value = session.info.get(setting_name)
        if setting_value is None:
            setting_value = (
                _worker_user_id.get()
                if setting_name == RLS_USER_INFO_KEY
                else _worker_tenant_id.get()
            )
        connection.execute(
            text("SELECT set_config(:name, :value, true)"),
            {
                "name": setting_name,
                "value": "" if setting_value is None else str(setting_value),
            },
        )


@event.listens_for(ContextSession, "after_begin")
def _reapply_database_context(session, transaction, connection) -> None:
    """Reinstall RLS settings after commit and on pooled connection reuse."""
    del transaction
    _apply_database_context(connection, session)


def set_database_context(
    session: Session,
    *,
    user_id: object,
    tenant_id: object | None = None,
) -> None:
    """Store RLS identity on a session and apply it to the current transaction."""
    session.info[RLS_USER_INFO_KEY] = str(user_id)
    if tenant_id is None:
        session.info.pop(RLS_TENANT_INFO_KEY, None)
    else:
        session.info[RLS_TENANT_INFO_KEY] = str(tenant_id)

    if session.in_transaction():
        connection = session.connection()
        _apply_database_context(connection, session)


@contextlib.contextmanager
def worker_database_context(
    *,
    user_id: object | None,
    tenant_id: object | None,
):
    """Install one message's owner and tenant for every session it opens."""
    user_token = _worker_user_id.set(str(user_id) if user_id is not None else None)
    tenant_token = _worker_tenant_id.set(str(tenant_id) if tenant_id is not None else None)
    try:
        yield
    finally:
        _worker_tenant_id.reset(tenant_token)
        _worker_user_id.reset(user_token)


class _EngineCache:
    __slots__ = ("database_url", "engine", "session_factory")

    def __init__(self) -> None:
        self.database_url: str | None = None
        self.engine: Engine | None = None
        self.session_factory = None


_cache = _EngineCache()


def _resolve_database_url() -> str:
    raw_config = load_config_data()
    database_config = raw_config.get("database") or {}
    return database_config.get("url") or DEFAULT_DATABASE_URL

def get_database_url() -> str:
    """Resolve the current database URL from config and environment."""
    return _resolve_database_url()


def _ensure_session_factory():
    """Build or reuse a session factory for the current database URL."""
    database_url = get_database_url()
    with _engine_lock:
        if _cache.session_factory is None or _cache.database_url != database_url:
            if _cache.engine is not None:
                _cache.engine.dispose()
            _cache.engine = create_engine(database_url)
            _cache.session_factory = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=_cache.engine,
                class_=ContextSession,
            )
            _cache.database_url = database_url
    return _cache.session_factory


def get_engine() -> Engine:
    """Return the active engine for the current configured database."""
    _ensure_session_factory()
    assert _cache.engine is not None
    return _cache.engine


class _SessionLocalProxy:
    """Callable proxy that binds sessions lazily to the active database."""

    def __call__(self, *args, **kwargs):
        return _ensure_session_factory()(*args, **kwargs)


class _EngineProxy:
    """Attribute proxy that exposes the current active engine lazily."""

    def __getattr__(self, name):
        return getattr(get_engine(), name)


engine = _EngineProxy()
SessionLocal = _SessionLocalProxy()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextlib.contextmanager
def db_session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
