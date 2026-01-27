import logging
import time
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed
from main_driver.database import engine
from main_driver.models import Base

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

        # Create tables
        Base.metadata.create_all(bind=engine)
        logger.info("Tables created or verified.")
        
    except Exception as e:
        logger.error(f"Error initializing DB: {e}")
        raise

if __name__ == "__main__":
    init_db()
