import os
import fcntl
import json
import time
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

LOCK_FILE_PATH = "pipeline.lock"

class PipelineController:
    """
    Manages exclusive access to the pipeline execution resources using a file lock.
    Allows distinguishing between 'frontend' and 'main' execution sources.
    """
    def __init__(self, lock_file: str = LOCK_FILE_PATH):
        self.lock_file = lock_file
        self.file_handle = None

    def _open_file(self):
        if not self.file_handle:
            self.file_handle = open(self.lock_file, "a+")

    def acquire_lock(self, source: str, metadata: Optional[Dict] = None) -> bool:
        """
        Attempt to acquire the exclusive lock for the pipeline.
        
        Args:
            source: Identifier for the source ('frontend' or 'main')
            metadata: Additional info to store (e.g., task_id, pid)
            
        Returns:
            True if lock acquired, False otherwise.
        """
        try:
            self._open_file()
            # Try to acquire an exclusive lock, non-blocking
            fcntl.flock(self.file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # If successful, truncate and write info
            self.file_handle.truncate(0)
            self.file_handle.seek(0)
            
            info = {
                "source": source,
                "pid": os.getpid(),
                "timestamp": time.time(),
                **(metadata or {})
            }
            json.dump(info, self.file_handle)
            self.file_handle.flush()
            
            return True
        except BlockingIOError:
            # Lock is held by another process
            return False
        except Exception as e:
            logger.error(f"Error acquiring pipeline lock: {e}")
            return False

    def release_lock(self):
        """Release the pipeline lock."""
        if self.file_handle:
            try:
                # Truncate content before releasing to indicate cleanliness?
                # actually, keeping the last owner info might be useful for debugging,
                # but let's clear it to avoid confusion if lock is lost otherwise.
                # However, strict flock release is enough.
                # Let's clean up content.
                self.file_handle.truncate(0)
                self.file_handle.seek(0)
                fcntl.flock(self.file_handle, fcntl.LOCK_UN)
                self.file_handle.close()
                self.file_handle = None
            except Exception as e:
                logger.error(f"Error releasing pipeline lock: {e}")

    def get_lock_info(self) -> Optional[Dict]:
        """
        Read information about the current lock owner.
        Returns None if file doesn't exist or is empty/corrupt.
        """
        if not os.path.exists(self.lock_file):
            return None
            
        try:
            with open(self.lock_file, "r") as f:
                content = f.read().strip()
                if not content:
                    return None
                return json.loads(content)
        except Exception as e:
            logger.warning(f"Could not read lock info: {e}")
            return None
