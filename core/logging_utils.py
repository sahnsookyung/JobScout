"""Logging utilities for JobScout services."""

import logging
import sys


class NilCharacterFilter(logging.Filter):
    """Filter that removes NIL (null) characters from log records.
    
    Prevents log forging and ensures clean log output in Docker/JSON environments.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg:
            record.msg = str(record.msg).replace('\x00', '')
        if record.args:
            record.args = tuple(
                str(arg).replace('\x00', '') if isinstance(arg, str) else arg
                for arg in record.args
            )
        return True


def setup_logging(name: str = None, level: int = logging.INFO) -> None:
    """Setup logging with NIL character filtering.
    
    Args:
        name: Logger name (None for root logger)
        level: Logging level (default: INFO)
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.addFilter(NilCharacterFilter())
        
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
