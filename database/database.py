import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config_loader import load_config_data

DEFAULT_DATABASE_URL = "postgresql://user:password@localhost:5432/jobscout"


def _resolve_database_url() -> str:
    raw_config = load_config_data()
    database_config = raw_config.get("database") or {}
    return database_config.get("url") or DEFAULT_DATABASE_URL


DATABASE_URL = _resolve_database_url()

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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
