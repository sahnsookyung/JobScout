import queue
import uuid
import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from pypdf import PdfWriter

from etl.resume import file_safety


def _docx_bytes(extra_entries: dict[str, bytes] | None = None) -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", b"<Types />")
        archive.writestr("word/document.xml", b"<document />")
        for name, content in (extra_entries or {}).items():
            archive.writestr(name, content)
    return output.getvalue()


def _pdf_bytes(*, pages: int, encrypted: bool = False) -> bytes:
    output = BytesIO()
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=612, height=792)
    if encrypted:
        writer.encrypt("secret")
    writer.write(output)
    return output.getvalue()


@pytest.mark.parametrize(
    ("filename", "content"),
    [
        ("resume.pdf", b"not a pdf"),
        ("resume.docx", b"not a zip"),
        ("resume.txt", b"bad\x00text"),
        ("resume.txt", b"\xff"),
    ],
)
def test_rejects_mismatched_or_unsafe_content(filename, content):
    with pytest.raises(file_safety.ResumeFileSafetyError):
        file_safety.validate_resume_content(filename, content)


def test_accepts_minimal_safe_docx_and_pdf():
    file_safety.validate_resume_content("resume.docx", _docx_bytes())
    file_safety.validate_resume_content("resume.pdf", _pdf_bytes(pages=1))


@pytest.mark.parametrize(
    "entry_name",
    ["../outside.xml", "word/vbaProject.bin", "word/active.bin"],
)
def test_rejects_unsafe_or_executable_docx_entries(entry_name):
    with pytest.raises(file_safety.ResumeFileSafetyError):
        file_safety.validate_resume_content(
            "resume.docx",
            _docx_bytes({entry_name: b"payload"}),
        )


def test_rejects_docx_compression_bomb():
    content = _docx_bytes({"word/large.xml": b"A" * (1024 * 1024)})

    with pytest.raises(file_safety.ResumeFileSafetyError) as exc_info:
        file_safety.validate_resume_content("resume.docx", content)

    assert exc_info.value.status_code == 413


def test_rejects_long_or_encrypted_pdf():
    with pytest.raises(file_safety.ResumeFileSafetyError) as too_long:
        file_safety.validate_resume_content("resume.pdf", _pdf_bytes(pages=21))
    assert too_long.value.status_code == 413

    with pytest.raises(file_safety.ResumeFileSafetyError, match="Encrypted"):
        file_safety.validate_resume_content(
            "resume.pdf",
            _pdf_bytes(pages=1, encrypted=True),
        )


def test_rejects_excessive_extracted_text():
    with pytest.raises(file_safety.ResumeFileSafetyError) as exc_info:
        file_safety.validate_extracted_text("x" * 100_001)

    assert exc_info.value.status_code == 413


def test_nonisolated_parser_delegates(monkeypatch):
    monkeypatch.setenv("RESUME_PARSER_ISOLATION_ENABLED", "false")
    parser = MagicMock()
    parser.parse.return_value = {"id": str(uuid.uuid4())}

    assert file_safety.parse_resume_file(parser, "/tmp/resume.txt") == parser.parse.return_value
    parser.parse.assert_called_once_with("/tmp/resume.txt")


def test_isolated_parser_reads_result_before_join(monkeypatch):
    monkeypatch.setenv("RESUME_PARSER_ISOLATION_ENABLED", "true")
    events: list[str] = []
    result_queue = MagicMock()
    result_queue.get.side_effect = lambda **_kwargs: (events.append("get") or (True, {"ok": True}))
    process = MagicMock()
    process.is_alive.return_value = False
    process.join.side_effect = lambda *_args: events.append("join")
    context = MagicMock()
    context.Queue.return_value = result_queue
    context.Process.return_value = process

    with patch("etl.resume.file_safety.multiprocessing.get_context", return_value=context):
        result = file_safety.parse_resume_file(MagicMock(), "/tmp/resume.txt")

    assert result == {"ok": True}
    assert events[:2] == ["get", "join"]
    result_queue.get.assert_called_once_with(timeout=file_safety.PARSER_TIMEOUT_SECONDS)
    result_queue.close.assert_called_once()


def test_isolated_validation_runs_in_child(monkeypatch):
    monkeypatch.setenv("RESUME_PARSER_ISOLATION_ENABLED", "true")
    result_queue = MagicMock()
    result_queue.get.return_value = (True, None, None)
    process = MagicMock()
    process.is_alive.return_value = False
    context = MagicMock()
    context.Queue.return_value = result_queue
    context.Process.return_value = process

    with patch("etl.resume.file_safety.multiprocessing.get_context", return_value=context):
        file_safety.validate_resume_content_safely("resume.pdf", b"%PDF-safe")

    context.Process.assert_called_once_with(
        target=file_safety._validate_child,
        args=("resume.pdf", b"%PDF-safe", result_queue),
        daemon=True,
    )
    process.start.assert_called_once()
    result_queue.get.assert_called_once_with(timeout=file_safety.PARSER_TIMEOUT_SECONDS)
    result_queue.close.assert_called_once()


def test_isolated_validation_preserves_child_error_status(monkeypatch):
    monkeypatch.setenv("RESUME_PARSER_ISOLATION_ENABLED", "true")
    result_queue = MagicMock()
    result_queue.get.return_value = (False, "PDF files are limited to 20 pages.", 413)
    process = MagicMock()
    process.is_alive.return_value = False
    context = MagicMock()
    context.Queue.return_value = result_queue
    context.Process.return_value = process

    with (
        patch("etl.resume.file_safety.multiprocessing.get_context", return_value=context),
        patch("etl.resume.file_safety.record_public_security_event") as record_event,
    ):
        with pytest.raises(file_safety.ResumeFileSafetyError) as exc_info:
            file_safety.validate_resume_content_safely("resume.pdf", b"%PDF-unsafe")

    assert exc_info.value.status_code == 413
    record_event.assert_called_once_with("parser_failed")


def test_isolated_parser_timeout_terminates_child(monkeypatch):
    monkeypatch.setenv("RESUME_PARSER_ISOLATION_ENABLED", "true")
    result_queue = MagicMock()
    result_queue.get.side_effect = queue.Empty
    process = MagicMock()
    process.is_alive.side_effect = [True, True, False]
    context = MagicMock()
    context.Queue.return_value = result_queue
    context.Process.return_value = process

    with (
        patch("etl.resume.file_safety.multiprocessing.get_context", return_value=context),
        patch("etl.resume.file_safety.record_public_security_event") as record_event,
    ):
        with pytest.raises(file_safety.ResumeFileSafetyError) as exc_info:
            file_safety.parse_resume_file(MagicMock(), "/tmp/resume.txt")

    assert exc_info.value.status_code == 413
    record_event.assert_called_once_with("parser_failed")
    process.terminate.assert_called_once()
    result_queue.close.assert_called_once()
