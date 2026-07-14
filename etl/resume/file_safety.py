"""Strict validation and isolated parsing helpers for untrusted resume files."""

from __future__ import annotations

import multiprocessing
import os
import queue
import zipfile
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any

from pypdf import PdfReader

from core.metrics import record_public_security_event

MAX_PDF_PAGES = 20
MAX_DOCX_ENTRIES = 256
MAX_DOCX_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
MAX_DOCX_COMPRESSION_RATIO = 100
MAX_EXTRACTED_TEXT_CHARS = 100_000
PARSER_TIMEOUT_SECONDS = 15


class ResumeFileSafetyError(ValueError):
    def __init__(self, message: str, *, status_code: int = 415) -> None:
        super().__init__(message)
        self.status_code = status_code


def _validate_text(content: bytes) -> None:
    if b"\x00" in content:
        raise ResumeFileSafetyError("Text resumes cannot contain NUL bytes.")
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ResumeFileSafetyError("Text resumes must use UTF-8 encoding.") from exc


def _safe_archive_name(name: str) -> bool:
    normalized = name.replace("\\", "/")
    path = PurePosixPath(normalized)
    return bool(normalized) and not path.is_absolute() and ".." not in path.parts


def _validate_docx(content: bytes) -> None:
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_DOCX_ENTRIES:
                raise ResumeFileSafetyError("DOCX contains too many archive entries.", status_code=413)
            names = {entry.filename for entry in entries}
            if "[Content_Types].xml" not in names or "word/document.xml" not in names:
                raise ResumeFileSafetyError("Malformed DOCX structure.")

            total_uncompressed = 0
            total_compressed = 0
            for entry in entries:
                if not _safe_archive_name(entry.filename):
                    raise ResumeFileSafetyError("DOCX contains an unsafe archive path.")
                lowered = entry.filename.lower()
                if "vbaproject" in lowered or lowered.endswith(".bin"):
                    raise ResumeFileSafetyError("Macro-enabled DOCX files are not supported.")
                if entry.flag_bits & 0x1:
                    raise ResumeFileSafetyError("Encrypted DOCX files are not supported.")
                total_uncompressed += entry.file_size
                total_compressed += entry.compress_size
                if (
                    entry.file_size > 0
                    and entry.file_size
                    > max(entry.compress_size, 1) * MAX_DOCX_COMPRESSION_RATIO
                ):
                    raise ResumeFileSafetyError(
                        "DOCX compression ratio is unsafe.",
                        status_code=413,
                    )
            if total_uncompressed > MAX_DOCX_UNCOMPRESSED_BYTES:
                raise ResumeFileSafetyError(
                    "DOCX decompressed content is too large.",
                    status_code=413,
                )
            if total_uncompressed > max(total_compressed, 1) * MAX_DOCX_COMPRESSION_RATIO:
                raise ResumeFileSafetyError("DOCX compression ratio is unsafe.", status_code=413)
            content_types = archive.read("[Content_Types].xml").lower()
            if b"macroenabled" in content_types or b"vbaproject" in content_types:
                raise ResumeFileSafetyError("Macro-enabled DOCX files are not supported.")
    except zipfile.BadZipFile as exc:
        raise ResumeFileSafetyError("Malformed DOCX archive.") from exc


def _validate_pdf(content: bytes) -> None:
    try:
        reader = PdfReader(BytesIO(content))
        if reader.is_encrypted:
            raise ResumeFileSafetyError("Encrypted PDF files are not supported.")
        page_count = len(reader.pages)
    except ResumeFileSafetyError:
        raise
    except Exception as exc:
        raise ResumeFileSafetyError("Malformed PDF file.") from exc
    if page_count == 0:
        raise ResumeFileSafetyError("PDF file has no pages.")
    if page_count > MAX_PDF_PAGES:
        raise ResumeFileSafetyError("PDF files are limited to 20 pages.", status_code=413)


def validate_resume_content(filename: str, content: bytes) -> None:
    suffix = Path(filename).suffix.lower()
    if suffix == ".pdf":
        if not content.startswith(b"%PDF-"):
            raise ResumeFileSafetyError("File content does not match the PDF format.")
        _validate_pdf(content)
    elif suffix == ".docx":
        if not content.startswith(b"PK\x03\x04"):
            raise ResumeFileSafetyError("File content does not match the DOCX format.")
        _validate_docx(content)
    elif suffix in {".json", ".yaml", ".yml", ".txt"}:
        _validate_text(content)
    else:
        raise ResumeFileSafetyError("Unsupported resume format.")


def validate_extracted_text(text: str) -> str:
    if len(text) > MAX_EXTRACTED_TEXT_CHARS:
        raise ResumeFileSafetyError(
            "Extracted resume text exceeds 100,000 characters.",
            status_code=413,
        )
    return text


def parser_isolation_enabled() -> bool:
    return os.getenv("RESUME_PARSER_ISOLATION_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _parse_child(file_path: str, result_queue: Any) -> None:
    try:
        try:
            import resource

            resource.setrlimit(resource.RLIMIT_CPU, (PARSER_TIMEOUT_SECONDS, PARSER_TIMEOUT_SECONDS))
            memory_limit = 512 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
            resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
        except (ImportError, OSError, ValueError):
            pass
        from etl.resume.parser import ResumeParser

        result_queue.put((True, ResumeParser().parse(file_path)))
    except BaseException as exc:
        result_queue.put((False, f"{exc.__class__.__name__}: {exc}"))


def _stop_parser_process(process: Any) -> None:
    if not process.is_alive():
        process.join(0)
        return
    process.terminate()
    process.join(2)
    if process.is_alive():
        process.kill()
        process.join(1)


def parse_resume_file(parser: Any, file_path: str):
    """Parse normally outside hosted mode and in a killable child when enabled."""
    if not parser_isolation_enabled():
        return parser.parse(file_path)

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue(maxsize=1)
    process = context.Process(target=_parse_child, args=(file_path, result_queue), daemon=True)
    process.start()
    try:
        # Read before joining: multiprocessing.Queue uses a feeder pipe, and a
        # large parsed result can otherwise block the child while the parent is
        # waiting for that child to exit.
        success, value = result_queue.get(timeout=PARSER_TIMEOUT_SECONDS)
    except queue.Empty as exc:
        was_running = process.is_alive()
        _stop_parser_process(process)
        record_public_security_event("parser_failed")
        if was_running:
            raise ResumeFileSafetyError(
                "Resume parsing exceeded the 15-second limit.",
                status_code=413,
            ) from exc
        raise ResumeFileSafetyError("Resume parser exited without a result.") from exc
    finally:
        result_queue.close()
    process.join(2)
    if process.is_alive():
        _stop_parser_process(process)
    if not success:
        record_public_security_event("parser_failed")
        raise ResumeFileSafetyError(f"Resume parsing failed: {value}")
    return value
