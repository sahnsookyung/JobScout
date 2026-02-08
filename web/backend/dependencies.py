#!/usr/bin/env python3
"""
FastAPI dependencies for dependency injection.
"""

from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from .config import get_config


class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self):
        config = get_config()
        self.engine = create_engine(
            config.database.url,
            pool_pre_ping=True,  # Verify connections before using
            pool_size=10,
            max_overflow=20
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a database session.
        
        Yields:
            Session: SQLAlchemy database session.
        """
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()


# Global database manager instance
_db_manager = DatabaseManager()


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session.
    
    Usage:
        @app.get("/endpoint")
        def my_endpoint(db: Session = Depends(get_db)):
            ...
    
    Yields:
        Session: Database session that will be automatically closed.
    """
    yield from _db_manager.get_session()


def get_db_engine():
    """Get the database engine (for advanced use cases)."""
    return _db_manager.engine
