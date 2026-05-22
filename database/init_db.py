import logging

from tenacity import retry, stop_after_attempt, wait_fixed

from database.database import engine
from database.bootstrap import bootstrap_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def init_db():
    logger.info("Initializing database...")
    try:
        bootstrap_database(engine=engine)
        logger.info("Database schema ready.")
    except Exception:
        logger.exception("Error initializing DB")
        raise

if __name__ == "__main__":  # pragma: no cover
    init_db()
