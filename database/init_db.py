import logging
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed
from database.database import engine
from database.migrate import check_database_schema

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def init_db():
    logger.info("Initializing database...")
    try:
        # Create extensions if they don't exist
        with engine.connect() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            connection.commit()
            logger.info("Checked/Created 'vector' extension.")

        check_database_schema(engine=engine)
        logger.info("Database schema verified.")
        
    except Exception as e:
        logger.error(f"Error initializing DB: {e}")
        raise

if __name__ == "__main__":  # pragma: no cover
    init_db()
