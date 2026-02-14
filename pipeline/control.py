import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

LOCK_FILE_PATH = "pipeline.lock"

class PipelineController:
    """
    Manages exclusive access to the pipeline execution resources.
    
    NOTE: Locking is now a no-op since the database handles concurrency
    via ACID transactions. This class is kept for backward compatibility
    but does nothing.
    """
    def __init__(self, lock_file: str = LOCK_FILE_PATH):
        self.lock_file = lock_file

    @staticmethod
    def clear_stale_lock(lock_file: str = LOCK_FILE_PATH):
        """No-op - kept for backward compatibility."""
        pass

    def acquire_lock(self, source: str, metadata: Optional[Dict] = None) -> bool:
        """No-op - always returns True."""
        return True

    def release_lock(self):
        """No-op - kept for backward compatibility."""
        pass

    def get_lock_info(self) -> Optional[Dict]:
        """No-op - returns None."""
        return None
