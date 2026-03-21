import io
import logging

from core.logging_utils import NilCharacterFilter, is_nil_filter_active, setup_logging


def test_nil_character_filter_strips_null_bytes() -> None:
    logger = logging.getLogger("test.nil.filter")
    logger.handlers = []
    logger.propagate = False
    logger.setLevel(logging.INFO)

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.addFilter(NilCharacterFilter())
    logger.addHandler(handler)

    logger.info("hello\x00world")

    output = stream.getvalue()
    assert "\x00" not in output
    assert "helloworld" in output


def test_setup_logging_attaches_filter_to_existing_uvicorn_handler() -> None:
    uvicorn_logger = logging.getLogger("uvicorn.error")
    original_handlers = list(uvicorn_logger.handlers)
    original_level = uvicorn_logger.level

    try:
        uvicorn_logger.handlers = []
        uvicorn_logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(io.StringIO())
        uvicorn_logger.addHandler(handler)

        setup_logging()

        assert any(isinstance(f, NilCharacterFilter) for f in handler.filters)
        assert is_nil_filter_active(["uvicorn.error"]) is True
    finally:
        uvicorn_logger.handlers = original_handlers
        uvicorn_logger.setLevel(original_level)


def test_integration_emitted_log_contains_no_null_bytes() -> None:
    logger_name = "test.nil.integration"
    logger = logging.getLogger(logger_name)
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate

    try:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))

        logger.handlers = [handler]
        logger.setLevel(logging.INFO)
        logger.propagate = False

        setup_logging(name=logger_name, level=logging.INFO)
        logger.info("abc\x00def")

        output = stream.getvalue()
        assert "\x00" not in output
        assert "abcdef" in output
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
