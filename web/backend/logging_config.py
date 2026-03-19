"""Logging configuration for JobScout web backend."""

import logging


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


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "access": {
            "format": "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "filters": {
        "nil_filter": {
            "()": "web.backend.logging_config.NilCharacterFilter",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "filters": ["nil_filter"],
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "filters": ["nil_filter"],
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
    "root": {"level": "INFO", "handlers": ["default"]},
}
