import logging
from typing import Optional

from etl.resume.parser import ResumeParser

logger = logging.getLogger(__name__)


def load_resume_with_parser(resume_file_path: str) -> Optional[dict]:
    """Load resume using ResumeParser for multi-format support."""
    logger.info("Loading resume from %s", resume_file_path)
    try:
        parser = ResumeParser()
        parsed = parser.parse(resume_file_path)
        return parsed.data if parsed.data is not None else {"raw_text": parsed.text}
    except FileNotFoundError:
        logger.error("Resume file not found: %s", resume_file_path)
        return None
    except ValueError as e:
        logger.error("Failed to parse resume: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error loading resume: %s", e)
        return None
