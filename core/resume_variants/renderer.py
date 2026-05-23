"""Safe renderers for generated resume variants."""

from __future__ import annotations

import html
import re
from io import BytesIO
from typing import Any

from docx import Document

MAX_RENDERED_BYTES = 256 * 1024


def safe_filename(stem: str, extension: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", stem).strip(".-") or "resume-variant"
    return f"{cleaned[:80]}.{extension}"


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\x00", "").strip()


def _claims(content: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = content.get(key)
    return value if isinstance(value, list) else []


class ResumeVariantRenderer:
    """Render variant JSON without persisting generated binaries."""

    def render_markdown(self, content: dict[str, Any]) -> bytes:
        job = content.get("job") if isinstance(content.get("job"), dict) else {}
        lines = [
            f"# Resume draft for {_text(job.get('title')) or 'target role'}",
            "",
        ]
        for claim in _claims(content, "summary"):
            lines.extend([_markdown_text(claim.get("text")), ""])
        if _claims(content, "targeted_evidence"):
            lines.append("## Targeted evidence")
            for claim in _claims(content, "targeted_evidence"):
                lines.append(f"- {_markdown_text(claim.get('text'))}")
            lines.append("")
        if _claims(content, "skills"):
            lines.append("## Skills")
            lines.append(", ".join(_markdown_text(claim.get("text")) for claim in _claims(content, "skills")))
            lines.append("")
        experience = content.get("experience")
        if isinstance(experience, list) and experience:
            lines.append("## Experience")
            for entry in experience:
                if not isinstance(entry, dict):
                    continue
                heading = " - ".join(part for part in [_markdown_text(entry.get("title")), _markdown_text(entry.get("company"))] if part)
                if heading:
                    lines.append(f"### {heading}")
                bullets = entry.get("bullets")
                if isinstance(bullets, list):
                    for bullet in bullets:
                        if isinstance(bullet, dict):
                            lines.append(f"- {_markdown_text(bullet.get('text'))}")
                lines.append("")
        return _bounded("\n".join(lines).encode("utf-8"))

    def render_html(self, content: dict[str, Any]) -> bytes:
        job = content.get("job") if isinstance(content.get("job"), dict) else {}
        sections = [
            "<!doctype html><html><head><meta charset=\"utf-8\">",
            "<title>Resume draft</title></head><body>",
            f"<h1>Resume draft for {html.escape(_text(job.get('title')) or 'target role')}</h1>",
        ]
        for claim in _claims(content, "summary"):
            sections.append(f"<p>{html.escape(_text(claim.get('text')))}</p>")
        if _claims(content, "targeted_evidence"):
            sections.append("<h2>Targeted evidence</h2><ul>")
            for claim in _claims(content, "targeted_evidence"):
                sections.append(f"<li>{html.escape(_text(claim.get('text')))}</li>")
            sections.append("</ul>")
        if _claims(content, "skills"):
            skills = ", ".join(html.escape(_text(claim.get("text"))) for claim in _claims(content, "skills"))
            sections.append(f"<h2>Skills</h2><p>{skills}</p>")
        experience = content.get("experience")
        if isinstance(experience, list) and experience:
            sections.append("<h2>Experience</h2>")
            for entry in experience:
                if not isinstance(entry, dict):
                    continue
                heading = " - ".join(part for part in [_text(entry.get("title")), _text(entry.get("company"))] if part)
                if heading:
                    sections.append(f"<h3>{html.escape(heading)}</h3>")
                bullets = entry.get("bullets")
                if isinstance(bullets, list):
                    sections.append("<ul>")
                    for bullet in bullets:
                        if isinstance(bullet, dict):
                            sections.append(f"<li>{html.escape(_text(bullet.get('text')))}</li>")
                    sections.append("</ul>")
        sections.append("</body></html>")
        return _bounded("".join(sections).encode("utf-8"))

    def render_docx(self, content: dict[str, Any]) -> bytes:
        document = Document()
        job = content.get("job") if isinstance(content.get("job"), dict) else {}
        document.add_heading(f"Resume draft for {_text(job.get('title')) or 'target role'}", level=1)
        for claim in _claims(content, "summary"):
            document.add_paragraph(_text(claim.get("text")))
        if _claims(content, "targeted_evidence"):
            document.add_heading("Targeted evidence", level=2)
            for claim in _claims(content, "targeted_evidence"):
                document.add_paragraph(_text(claim.get("text")), style="List Bullet")
        if _claims(content, "skills"):
            document.add_heading("Skills", level=2)
            document.add_paragraph(", ".join(_text(claim.get("text")) for claim in _claims(content, "skills")))
        experience = content.get("experience")
        if isinstance(experience, list) and experience:
            document.add_heading("Experience", level=2)
            for entry in experience:
                if not isinstance(entry, dict):
                    continue
                heading = " - ".join(part for part in [_text(entry.get("title")), _text(entry.get("company"))] if part)
                if heading:
                    document.add_heading(heading, level=3)
                bullets = entry.get("bullets")
                if isinstance(bullets, list):
                    for bullet in bullets:
                        if isinstance(bullet, dict):
                            document.add_paragraph(_text(bullet.get("text")), style="List Bullet")
        output = BytesIO()
        document.save(output)
        return _bounded(output.getvalue())


def _markdown_text(value: Any) -> str:
    return html.escape(_text(value), quote=False)


def _bounded(payload: bytes) -> bytes:
    if len(payload) > MAX_RENDERED_BYTES:
        raise ValueError("Rendered resume variant exceeds size limit.")
    return payload
