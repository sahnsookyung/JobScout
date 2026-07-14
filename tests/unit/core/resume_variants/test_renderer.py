from __future__ import annotations

from io import BytesIO
from zipfile import ZipFile

import pytest

from core.resume_variants.renderer import MAX_RENDERED_BYTES, ResumeVariantRenderer, safe_filename


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


def _complete_content() -> dict:
    return {
        "contact": {"name": "Ada Engineer", "email": "ada@example.com", "location": "Tokyo"},
        "job": {"title": "Platform Engineer"},
        "summary": [{"text": "Ships reliable APIs\x00"}],
        "targeted_evidence": [{"text": "Cut incident response time"}],
        "skills": [{"text": "Python"}, {"text": "Postgres"}],
        "experience": [
            {
                "title": "Senior Engineer",
                "company": "Acme",
                "bullets": [{"text": "Led migration"}, "ignored"],
            },
            "ignored",
            {"title": "", "company": "No Heading", "bullets": [{"text": "Kept services healthy"}]},
        ],
        "projects": [
            {
                "name": "Reliability Toolkit",
                "technologies": ["Python", "Postgres"],
                "bullets": [{"text": "Automated incident triage"}],
            }
        ],
        "education": [
            {
                "degree": "BSc",
                "field_of_study": "Computer Science",
                "institution": "Example University",
                "graduation_year": 2020,
                "details": [],
            }
        ],
        "certifications": [{"name": "Cloud Architect", "issuer": "Example", "issued_year": 2024}],
        "languages": [{"language": "Japanese", "proficiency": "Professional"}],
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


def test_renderers_include_all_resume_sections() -> None:
    renderer = ResumeVariantRenderer()

    markdown = renderer.render_markdown(_complete_content()).decode("utf-8")
    html = renderer.render_html(_complete_content()).decode("utf-8")
    docx = renderer.render_docx(_complete_content())

    assert "# Ada Engineer" in markdown
    assert "ada@example.com | Tokyo" in markdown
    assert "## Professional Summary" in markdown
    assert "## Targeted evidence" not in markdown
    assert "## Skills" in markdown
    assert "### Senior Engineer - Acme" in markdown
    assert "- Led migration" in markdown
    assert "## Projects" in markdown
    assert "## Education" in markdown
    assert "<h2>Professional Summary</h2>" in html
    assert "<h2>Skills</h2><p>Python, Postgres</p>" in html
    assert "<h3>Senior Engineer - Acme</h3>" in html
    with ZipFile(BytesIO(docx)) as package:
        document_xml = package.read("word/document.xml").decode("utf-8")
    assert "Senior Engineer - Acme" in document_xml


def test_renderer_uses_target_role_fallback_and_bounds_output() -> None:
    renderer = ResumeVariantRenderer()

    markdown = renderer.render_markdown({"job": None}).decode("utf-8")

    assert markdown.startswith("# Resume")
    with pytest.raises(ValueError, match="size limit"):
        renderer.render_markdown({"summary": [{"text": "x" * (MAX_RENDERED_BYTES + 1)}]})


def test_safe_filename_removes_unsafe_characters() -> None:
    assert safe_filename('../../resume "draft"', "docx") == "resume-draft.docx"
    assert safe_filename("***", "md") == "resume-variant.md"
