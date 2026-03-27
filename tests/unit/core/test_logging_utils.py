import io
import logging
import sys

from core.logging_utils import (
    NulCharacterFilter,
    NulSafeFormatter,
    NulSafeTextIO,
    is_nul_filter_active,
    setup_logging,
)


def _make_test_logger(name: str, stream: io.StringIO) -> logging.Logger:
    """Return an isolated logger writing to *stream* with NulCharacterFilter + NulSafeFormatter."""
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(NulSafeFormatter("%(message)s"))
    handler.addFilter(NulCharacterFilter())
    logger.addHandler(handler)
    return logger


def test_nul_character_filter_strips_null_bytes_from_msg() -> None:
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.msg", stream)
    logger.info("hello\x00world")
    output = stream.getvalue()
    assert "\x00" not in output
    assert "helloworld" in output


def test_nul_character_filter_strips_null_bytes_from_tuple_args() -> None:
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.args_tuple", stream)
    logger.info("val: %s", "abc\x00def")
    output = stream.getvalue()
    assert "\x00" not in output
    assert "abcdef" in output


def test_nul_character_filter_strips_null_bytes_from_dict_args() -> None:
    """Dict-style % formatting must also have NUL chars stripped."""
    stream = io.StringIO()
    logger = _make_test_logger("test.nil.args_dict", stream)
    logger.info("key=%(key)s", {"key": "v\x00al"})
    output = stream.getvalue()
    assert "\x00" not in output
    assert "val" in output


def test_nul_safe_formatter_strips_null_bytes_from_exception_traceback() -> None:
    """NUL chars in exception messages must be removed from the formatted output."""
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

        assert any(isinstance(f, NulCharacterFilter) for f in handler.filters)
        assert is_nul_filter_active(["uvicorn.error"]) is True
    finally:
        uvicorn_logger.handlers = original_handlers
        uvicorn_logger.setLevel(original_level)


def test_sanitize_logger_handlers_adds_handler_when_missing() -> None:
    """_sanitize_logger_handlers with add_handler_if_missing=True adds a handler when none exist."""
    from core.logging_utils import _sanitize_logger_handlers

    logger_name = "test.add.handler.when.missing"
    logger = logging.getLogger(logger_name)
    original_handlers = list(logger.handlers)
    original_level = logger.level

    try:
        logger.handlers = []
        logger.setLevel(logging.NOTSET)
        logger.propagate = False

        _sanitize_logger_handlers(logger, logging.INFO, add_handler_if_missing=True)

        assert len(logger.handlers) == 1
        assert any(isinstance(f, NulCharacterFilter) for f in logger.handlers[0].filters)
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)


def test_is_nul_filter_active_returns_false_when_filter_missing() -> None:
    """is_nul_filter_active returns False when a handler lacks NulCharacterFilter."""
    logger_name = "test.no.nil.filter"
    logger = logging.getLogger(logger_name)
    original_handlers = list(logger.handlers)
    original_level = logger.level

    try:
        logger.handlers = []
        logger.propagate = False

        # Handler WITHOUT NulCharacterFilter
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        result = is_nul_filter_active([logger_name])
        assert result is False
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)


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


def test_setup_logging_wraps_stdout_and_stderr_with_nul_safe_stream(monkeypatch) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)

    setup_logging(name="test.nul.streams")

    assert isinstance(sys.stdout, NulSafeTextIO)
    assert isinstance(sys.stderr, NulSafeTextIO)

    sys.stdout.write("a\x00b")
    sys.stderr.write("c\x00d")

    assert stdout.getvalue() == "ab"
    assert stderr.getvalue() == "cd"
