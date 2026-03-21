"""Logging utilities for JobScout services."""

import logging
import sys

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class NilCharacterFilter(logging.Filter):
    """Filter that removes NIL (null) characters from log records.
    
    Prevents log forging and ensures clean log output in Docker/JSON environments.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg:
            msg_str = str(record.msg)
            if '\x00' in msg_str:
                record.msg = msg_str.replace('\x00', '')
        if record.args:
            record.args = tuple(
                arg.replace('\x00', '') if isinstance(arg, str) and '\x00' in arg else arg
                for arg in record.args
            )
        return True

def _ensure_nil_filter(handler: logging.Handler) -> None:
    """Attach NIL filter to handler once."""
    for existing_filter in handler.filters:
        if isinstance(existing_filter, NilCharacterFilter):
            return
    handler.addFilter(NilCharacterFilter())

def _ensure_default_formatter(handler: logging.Handler) -> None:
    """Attach default formatter if handler has none."""
    if handler.formatter is None:
        handler.setFormatter(
            logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
        )

def _sanitize_logger_handlers(
    logger: logging.Logger,
    level: int,
    add_handler_if_missing: bool = False
) -> None:
    """Apply NIL sanitization to all handlers for this logger."""
    if logger.level == logging.NOTSET:
        logger.setLevel(level)

    if add_handler_if_missing and not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        _ensure_nil_filter(handler)
        _ensure_default_formatter(handler)
        logger.addHandler(handler)

    for handler in logger.handlers:
        _ensure_nil_filter(handler)
        _ensure_default_formatter(handler)

def is_nil_filter_active(logger_names: list[str] | None = None) -> bool:
    """Return True if all inspected handlers include NilCharacterFilter."""
    names = logger_names or ["", "uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]
    for name in names:
        logger = logging.getLogger(name or None)
        for handler in logger.handlers:
            if not any(isinstance(f, NilCharacterFilter) for f in handler.filters):
                return False
    return True

def setup_logging(name: str = None, level: int = logging.INFO) -> None:
    """Setup logging with NIL character filtering.
    
    Args:
        name: Logger name (None for root logger)
        level: Logging level (default: INFO)
    """
    if name is None:
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        _sanitize_logger_handlers(root_logger, level, add_handler_if_missing=True)

        # Uvicorn/FastAPI may install dedicated handlers.
        for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
            _sanitize_logger_handlers(
                logging.getLogger(logger_name),
                level,
                add_handler_if_missing=False
            )
        return

    logger = logging.getLogger(name)
    logger.setLevel(level)
    _sanitize_logger_handlers(logger, level, add_handler_if_missing=True)
