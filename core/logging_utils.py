"""Logging utilities for JobScout services."""

import logging
import sys

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _strip_nil(s: str) -> str:
    return s.replace('\x00', '') if '\x00' in s else s


class NilCharacterFilter(logging.Filter):
    """Pre-format filter: strips NIL bytes from msg and args before formatting.

    Handles tuple args (standard API) and dict args (%-dict style).
    Exception tracebacks are caught by NilSafeFormatter after formatting.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg:
            record.msg = _strip_nil(str(record.msg))
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: _strip_nil(v) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, (list, tuple)):
                record.args = type(record.args)(
                    _strip_nil(arg) if isinstance(arg, str) else arg
                    for arg in record.args
                )
        return True


class NilSafeFormatter(logging.Formatter):
    """Post-format pass: strips any NIL bytes remaining in the final output.

    Catches NIL chars that survive pre-format filtering (e.g. exception
    tracebacks, stack frames, or format strings that embed raw data).
    """

    def format(self, record: logging.LogRecord) -> str:
        return _strip_nil(super().format(record))


def _ensure_nil_filter(handler: logging.Handler) -> None:
    """Attach NIL filter to handler once."""
    for existing_filter in handler.filters:
        if isinstance(existing_filter, NilCharacterFilter):
            return
    handler.addFilter(NilCharacterFilter())

def _ensure_default_formatter(handler: logging.Handler) -> None:
    """Attach NilSafeFormatter to handler, overriding any existing formatter."""
    handler.setFormatter(NilSafeFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))

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


def setup_service_logging(logger: logging.Logger) -> None:
    """Initialize logging for a microservice and log NIL sanitization status.

    Replaces the boilerplate ``_setup_logging()`` defined identically in each
    service, so they can all call this single shared helper instead.
    """
    setup_logging()
    logger.debug("NIL log sanitization active=%s", is_nil_filter_active())
