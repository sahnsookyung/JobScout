import io
import logging

from core.logging_utils import NilCharacterFilter, NilSafeFormatter, is_nil_filter_active, setup_logging


def _make_test_logger(name: str, stream: io.StringIO) -> logging.Logger:
    """Return an isolated logger writing to *stream* with NilCharacterFilter + NilSafeFormatter."""
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(NilSafeFormatter("%(message)s"))
    handler.addFilter(NilCharacterFilter())
    logger.addHandler(handler)
    return logger


def test_nil_character_filter_strips_null_bytes_from_msg() -> None:
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.msg", stream)
    logger.info("hello\x00world")
    output = stream.getvalue()
    assert "\x00" not in output
    assert "helloworld" in output


def test_nil_character_filter_strips_null_bytes_from_tuple_args() -> None:
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.args_tuple", stream)
    logger.info("val: %s", "abc\x00def")
    output = stream.getvalue()
    assert "\x00" not in output
    assert "abcdef" in output


def test_nil_character_filter_strips_null_bytes_from_dict_args() -> None:
    """Dict-style % formatting must also have NIL chars stripped."""
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.args_dict", stream)
    logger.info("key=%(key)s", {"key": "v\x00al"})
    output = stream.getvalue()
    assert "\x00" not in output
    assert "val" in output


def test_nil_safe_formatter_strips_null_bytes_from_exception_traceback() -> None:
    """NIL chars in exception messages must be removed from the formatted output."""
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.exc", stream)
    try:
        raise ValueError("bad\x00data")
    except ValueError:
        logger.exception("caught error")
    output = stream.getvalue()
    assert "\x00" not in output
    assert "baddata" in output


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
