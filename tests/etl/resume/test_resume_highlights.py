
import pytest
from unittest.mock import MagicMock
from etl.resume.profiler import ResumeProfiler
from etl.resume.models import ResumeEvidenceUnit
from core.llm.schema_models import (
    ResumeSchema, Profile, ExperienceItem, ProjectItem, EducationItem,
    Summary, Projects, SkillsBlock
)

def test_extract_highlights_evidence():
    """Verify that highlights from Experience, Projects, and Education are extracted as evidence units."""
    
    # Mock AI service
    mock_ai = MagicMock()
    profiler = ResumeProfiler(ai_service=mock_ai)
    
    # Create a dummy profile with highlights
    profile = Profile(
        summary=Summary(text="Summary", total_experience_years=5.0),
        experience=[
            ExperienceItem(
                company="TechCorp",
                title="Senior Dev",
                description="Built things.",
                highlights=["Reduced latency by 50%", "Led team of 5"],
                tech_keywords=["Python"],
                start_date=None,
                end_date=None,
                is_current=True,
                years_value=2.0
            )
        ],
        projects=Projects(items=[
            ProjectItem(
                name="Side Project",
                description="A cool app.",
                highlights=["10k users", "Featured on HN"],
                technologies=["React"],
                url=None,
                date=None
            )
        ]),
        education=[
            EducationItem(
                institution="University",
                degree="BS CS",
                field_of_study="Computer Science",
                description="Good grades.",
                highlights=["Summa Cum Laude", "President's List"],
                graduation_year=2020
            )
        ],
        skills=SkillsBlock(groups=[], all=[]),
        certifications=[],
        languages=[]
    )
    
    # Run evidence extraction
    evidence_units = profiler.extract_resume_evidence(profile)
    
    # Verify Experience Highlights
    exp_highlights = [u for u in evidence_units if u.source_section == "Experience" and u.tags.get('type') == 'highlight']
    assert len(exp_highlights) == 2
    assert "Reduced latency by 50%" in [u.text for u in exp_highlights]
    assert "Led team of 5" in [u.text for u in exp_highlights]
    
    # Verify Project Highlights
    proj_highlights = [u for u in evidence_units if u.source_section == "Projects" and u.tags.get('type') == 'highlight']
    assert len(proj_highlights) == 2
    assert "10k users" in [u.text for u in proj_highlights]
    assert "Featured on HN" in [u.text for u in proj_highlights]
    
    # Verify Education Highlights
    edu_highlights = [u for u in evidence_units if u.source_section == "Education" and u.tags.get('type') == 'highlight']
    assert len(edu_highlights) == 2
    assert "Summa Cum Laude" in [u.text for u in edu_highlights]
    assert "President's List" in [u.text for u in edu_highlights]
