import contextlib
import logging

from database.database import SessionLocal
from database.repository import JobRepository

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def job_uow():
    """Per-unit-of-work transaction scope.

    Yields a JobRepository bound to a fresh Session. Commits on success,
    rolls back on exception, always closes.

    Usage:
        with job_uow() as repo:
            job = repo.get_by_id(job_id)
            # perform operations...
        # commit happens automatically on successful exit
    """
    session = SessionLocal()
    try:
        repo = JobRepository(session)
        yield repo
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
