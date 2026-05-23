from __future__ import annotations

from io import BytesIO
from zipfile import ZipFile

import pytest

from core.resume_variants.renderer import ResumeVariantRenderer, safe_filename


def _malicious_content() -> dict:
    return {
        "job": {"title": '<img src=x onerror="alert(1)">'},
        "summary": [
            {
                "text": '<script>alert("owned")</script> Python engineer',
                "sources": [{"kind": "structured_resume", "path": "profile.summary.text"}],
            }
        ],
        "skills": [
            {
                "text": 'FastAPI"><svg onload=alert(1)>',
                "sources": [{"kind": "structured_resume", "path": "profile.skills.all[0].name"}],
            }
        ],
        "targeted_evidence": [],
        "experience": [],
    }


@pytest.mark.security
def test_html_renderer_escapes_script_and_event_attributes() -> None:
    rendered = ResumeVariantRenderer().render_html(_malicious_content()).decode("utf-8")

    assert "<script>" not in rendered
    assert "<img" not in rendered
    assert "onerror=\"" not in rendered
    assert "<svg" not in rendered
    assert "onload=\"" not in rendered
    assert "&lt;script&gt;" in rendered


@pytest.mark.security
def test_markdown_renderer_does_not_emit_raw_html() -> None:
    rendered = ResumeVariantRenderer().render_markdown(_malicious_content()).decode("utf-8")

    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered


@pytest.mark.security
def test_docx_renderer_has_no_external_relationships() -> None:
    payload = ResumeVariantRenderer().render_docx(_malicious_content())

    with ZipFile(BytesIO(payload)) as package:
        relationship_files = [
            name for name in package.namelist()
            if name.endswith(".rels")
        ]
        rels_xml = "\n".join(package.read(name).decode("utf-8") for name in relationship_files)

    assert "TargetMode=\"External\"" not in rels_xml


def test_safe_filename_removes_unsafe_characters() -> None:
    assert safe_filename('../../resume "draft"', "docx") == "resume-draft.docx"
