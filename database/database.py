import contextlib
import threading

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from core.config_loader import load_config_data

DEFAULT_DATABASE_URL = "postgresql://user:password@localhost:5432/jobscout"
_engine_lock = threading.Lock()


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
