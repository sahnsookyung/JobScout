"""Unit tests for etl/resume/profiler.py"""

import threading
import pytest
from unittest.mock import MagicMock

from etl.resume.profiler import ResumeProfiler
from etl.resume.models import ResumeEvidenceUnit
from core.llm.schema_models import (
    ResumeSchema,
    Profile,
    ResumeContact,
    ExperienceItem,
    EducationItem,
    SkillsBlock,
    SkillItem,
    Projects,
    ProjectItem,
    Summary,
    Extraction,
)


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def make_profile(
    experience=None,
    projects_items=None,
    education=None,
    skills_all=None,
    summary_text="5 years of Python engineering",
    total_years=5.0,
):
    if experience is None:
        experience = []
    if projects_items is None:
        projects_items = []
    if education is None:
        education = []
    if skills_all is None:
        skills_all = []

    return Profile(
        contact=ResumeContact(name="Test Candidate"),
        summary=Summary(text=summary_text, total_experience_years=total_years),
        experience=experience,
        projects=Projects(items=projects_items),
        education=education,
        skills=SkillsBlock(groups=[], all=skills_all),
        certifications=[],
        languages=[],
    )


def make_resume_schema(profile=None):
    if profile is None:
        profile = make_profile()
    return ResumeSchema(
        profile=profile,
        extraction=Extraction(confidence=0.9, warnings=[]),
    )


def make_experience(
    company="ACME Corp",
    title="Software Engineer",
    description="Built scalable systems",
    highlights=None,
    tech_keywords=None,
    years_value=3.0,
    is_current=True,
):
    if highlights is None:
        highlights = ["Improved perf by 30%"]
    if tech_keywords is None:
        tech_keywords = ["Python", "Docker"]
    return ExperienceItem(
        company=company,
        title=title,
        description=description,
        highlights=highlights,
        tech_keywords=tech_keywords,
        years_value=years_value,
        is_current=is_current,
        start_date=None,
        end_date=None,
    )


def make_profiler(ai_service=None, store=None):
    if ai_service is None:
        ai_service = MagicMock()
        ai_service.generate_embedding.return_value = [0.1, 0.2, 0.3]
    return ResumeProfiler(ai_service=ai_service, store=store)


# ---------------------------------------------------------------------------
# extract_structured_resume
# ---------------------------------------------------------------------------


class TestExtractStructuredResume:
    def test_success_with_raw_text(self):
        profiler = make_profiler()
        profile_data = make_profile()
        make_resume_schema(profile_data)
        profiler.ai.extract_resume_data.return_value = {
            "profile": profile_data.model_dump(),
            "extraction": {"confidence": 0.9, "warnings": []},
        }

        result = profiler.extract_structured_resume({"raw_text": "resume content"})
        assert isinstance(result, ResumeSchema)

    def test_success_uses_json_when_no_raw_text(self):
        profiler = make_profiler()
        profile_data = make_profile()
        profiler.ai.extract_resume_data.return_value = {
            "profile": profile_data.model_dump(),
            "extraction": {"confidence": 0.9, "warnings": []},
        }

        result = profiler.extract_structured_resume({"name": "Alice"})
        assert result is not None
        # Verify JSON string was passed (no raw_text key)
        call_args = profiler.ai.extract_resume_data.call_args[0][0]
        assert '"name": "Alice"' in call_args

    def test_returns_none_when_ai_returns_empty(self):
        profiler = make_profiler()
        profiler.ai.extract_resume_data.return_value = None

        result = profiler.extract_structured_resume({"raw_text": "text"})
        assert result is None

    def test_returns_none_when_no_profile_key(self):
        profiler = make_profiler()
        profiler.ai.extract_resume_data.return_value = {"not_profile": {}}

        result = profiler.extract_structured_resume({"raw_text": "text"})
        assert result is None

    def test_returns_none_on_exception(self):
        profiler = make_profiler()
        profiler.ai.extract_resume_data.side_effect = Exception("API error")

        result = profiler.extract_structured_resume({"raw_text": "text"})
        assert result is None


# ---------------------------------------------------------------------------
# _extract_experience_evidence
# ---------------------------------------------------------------------------


class TestExtractExperienceEvidence:
    def test_description_unit(self):
        profiler = make_profiler()
        exp = make_experience(description="Built systems", highlights=[], tech_keywords=[])
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        desc_units = [
            u
            for u in units
            if u.tags.get("type") == "description" and u.source_section == "Experience"
        ]
        assert len(desc_units) == 1
        assert desc_units[0].text == "Built systems"
        assert desc_units[0].tags["company"] == "ACME Corp"
        assert desc_units[0].tags["title"] == "Software Engineer"
        assert desc_units[0].years_value == 3.0
        assert desc_units[0].is_total_years_claim is False

    def test_highlight_units(self):
        profiler = make_profiler()
        exp = make_experience(
            description=None, highlights=["Achieved X", "Improved Y"], tech_keywords=[]
        )
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        highlight_units = [u for u in units if u.tags.get("type") == "highlight"]
        assert len(highlight_units) == 2
        texts = {u.text for u in highlight_units}
        assert "Achieved X" in texts
        assert "Improved Y" in texts

    def test_tech_keyword_units(self):
        profiler = make_profiler()
        exp = make_experience(
            description="Used Python", highlights=[], tech_keywords=["Python", "Docker"]
        )
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        tech_units = [u for u in units if u.tags.get("type") == "tech_keyword"]
        assert len(tech_units) == 2
        tech_texts = {u.text for u in tech_units}
        assert "Experience with Python" in tech_texts
        assert "Experience with Docker" in tech_texts

    def test_tech_keyword_years_when_in_description(self):
        """Tech keyword gets years_value when tech appears in description."""
        profiler = make_profiler()
        exp = make_experience(
            description="Used python for 3 years",
            highlights=[],
            tech_keywords=["python"],
            years_value=3.0,
        )
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        python_unit = next(u for u in units if u.tags.get("technology") == "python")
        # "python" is in the description (case-insensitive check uses .lower())
        assert python_unit.years_value == 3.0
        assert python_unit.years_context == "python_experience"

    def test_tech_keyword_no_years_when_not_in_description(self):
        profiler = make_profiler()
        exp = make_experience(
            description="Built systems",
            highlights=[],
            tech_keywords=["Kubernetes"],
            years_value=3.0,
        )
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        k8s_unit = next(u for u in units if u.tags.get("technology") == "Kubernetes")
        assert k8s_unit.years_value is None

    def test_no_description_no_unit(self):
        profiler = make_profiler()
        exp = make_experience(description=None, highlights=[], tech_keywords=[])
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        desc_units = [u for u in units if u.tags.get("type") == "description"]
        assert len(desc_units) == 0

    def test_ids_are_sequential(self):
        profiler = make_profiler()
        exp = make_experience(
            description="Built systems",
            highlights=["Highlight 1"],
            tech_keywords=["Python"],
        )
        profile = make_profile(experience=[exp])

        units = profiler.extract_resume_evidence(profile)
        ids = [u.id for u in units]
        expected = [f"reu_{i}" for i in range(len(ids))]
        assert ids == expected


# ---------------------------------------------------------------------------
# _extract_project_evidence
# ---------------------------------------------------------------------------


class TestExtractProjectEvidence:
    def test_project_description_and_highlights(self):
        profiler = make_profiler()
        proj = ProjectItem(
            name="My App",
            description="A great app",
            technologies=["Python"],
            url=None,
            date=None,
            highlights=["Feature X", "Feature Y"],
        )
        profile = make_profile(projects_items=[proj])

        units = profiler.extract_resume_evidence(profile)
        project_units = [u for u in units if u.source_section == "Projects"]
        # 1 description + 2 highlights = 3 units
        assert len(project_units) == 3
        desc_units = [u for u in project_units if u.tags.get("type") == "description"]
        assert desc_units[0].text == "A great app"
        assert desc_units[0].tags["project"] == "My App"

    def test_project_no_description_yields_only_highlights(self):
        profiler = make_profiler()
        proj = ProjectItem(
            name="Proj",
            description=None,
            technologies=[],
            url=None,
            date=None,
            highlights=["h1"],
        )
        profile = make_profile(projects_items=[proj])

        units = profiler.extract_resume_evidence(profile)
        project_units = [u for u in units if u.source_section == "Projects"]
        assert len(project_units) == 1
        assert project_units[0].text == "h1"

    def test_no_projects_yields_nothing(self):
        profiler = make_profiler()
        profile = make_profile(projects_items=[])

        units = profiler.extract_resume_evidence(profile)
        project_units = [u for u in units if u.source_section == "Projects"]
        assert len(project_units) == 0


# ---------------------------------------------------------------------------
# _extract_education_evidence
# ---------------------------------------------------------------------------


class TestExtractEducationEvidence:
    def test_education_description_and_highlights(self):
        profiler = make_profiler()
        edu = EducationItem(
            degree="BS Computer Science",
            field_of_study="CS",
            institution="State University",
            graduation_year=2020,
            description="Studied algorithms",
            highlights=["Dean's list"],
        )
        profile = make_profile(education=[edu])

        units = profiler.extract_resume_evidence(profile)
        edu_units = [u for u in units if u.source_section == "Education"]
        assert len(edu_units) == 2
        desc = next(u for u in edu_units if u.tags.get("type") == "description")
        assert desc.text == "Studied algorithms"
        assert desc.tags["institution"] == "State University"

    def test_no_description_yields_only_highlights(self):
        profiler = make_profiler()
        edu = EducationItem(
            degree="BS",
            field_of_study=None,
            institution="Uni",
            graduation_year=None,
            description=None,
            highlights=["Award"],
        )
        profile = make_profile(education=[edu])

        units = profiler.extract_resume_evidence(profile)
        edu_units = [u for u in units if u.source_section == "Education"]
        assert len(edu_units) == 1

    def test_empty_education_yields_nothing(self):
        profiler = make_profiler()
        profile = make_profile(education=[])

        units = profiler.extract_resume_evidence(profile)
        edu_units = [u for u in units if u.source_section == "Education"]
        assert len(edu_units) == 0


# ---------------------------------------------------------------------------
# _extract_skill_evidence
# ---------------------------------------------------------------------------


class TestExtractSkillEvidence:
    def test_skill_with_proficiency_and_years(self):
        profiler = make_profiler()
        skill = SkillItem(
            name="Python", kind="language", proficiency="expert", years_experience=5.0
        )
        profile = make_profile(skills_all=[skill])

        units = profiler.extract_resume_evidence(profile)
        skill_units = [u for u in units if u.source_section == "Skills"]
        assert len(skill_units) == 1
        assert skill_units[0].tags["skill"] == "Python"
        assert skill_units[0].years_value == 5.0
        assert skill_units[0].years_context == "Python_skill"

    def test_skill_without_name_skipped(self):
        profiler = make_profiler()
        skill = SkillItem(name=None, kind=None, proficiency=None, years_experience=None)
        profile = make_profile(skills_all=[skill])

        units = profiler.extract_resume_evidence(profile)
        skill_units = [u for u in units if u.source_section == "Skills"]
        assert len(skill_units) == 0

    def test_skill_embedding_text_falls_back_to_name(self):
        """When to_embedding_text() returns empty, use skill name as text."""
        profiler = make_profiler()
        skill = SkillItem(name="Rust", kind=None, proficiency=None, years_experience=None)
        profile = make_profile(skills_all=[skill])

        units = profiler.extract_resume_evidence(profile)
        assert units[0].text == "Rust"

    def test_no_skills_yields_nothing(self):
        profiler = make_profiler()
        profile = make_profile(skills_all=[])

        units = profiler.extract_resume_evidence(profile)
        skill_units = [u for u in units if u.source_section == "Skills"]
        assert len(skill_units) == 0


# ---------------------------------------------------------------------------
# embed_evidence_units
# ---------------------------------------------------------------------------


class TestEmbedEvidenceUnits:
    def test_generates_embeddings_for_units_without_embedding(self):
        profiler = make_profiler()
        unit = ResumeEvidenceUnit(id="reu_0", text="Python expert", source_section="Skills")
        assert unit.embedding is None

        profiler.embed_evidence_units([unit])

        profiler.ai.generate_embedding.assert_called_once_with("Python expert")
        assert unit.embedding == [0.1, 0.2, 0.3]

    def test_skips_units_that_already_have_embedding(self):
        profiler = make_profiler()
        unit = ResumeEvidenceUnit(id="reu_0", text="text", source_section="Skills", embedding=[0.9])

        profiler.embed_evidence_units([unit])

        profiler.ai.generate_embedding.assert_not_called()
        assert unit.embedding == [0.9]

    def test_multiple_units(self):
        profiler = make_profiler()
        profiler.ai.generate_embedding.side_effect = [[0.1], [0.2], [0.3]]
        units = [
            ResumeEvidenceUnit(id=f"reu_{i}", text=f"text {i}", source_section="Skills")
            for i in range(3)
        ]

        profiler.embed_evidence_units(units)

        assert profiler.ai.generate_embedding.call_count == 3
        for i, unit in enumerate(units):
            assert unit.embedding is not None


# ---------------------------------------------------------------------------
# save_evidence_unit_embeddings
# ---------------------------------------------------------------------------


class TestSaveEvidenceUnitEmbeddings:
    def test_saves_when_store_and_units_present(self):
        mock_store = MagicMock()
        profiler = ResumeProfiler(ai_service=MagicMock(), store=mock_store)

        unit = ResumeEvidenceUnit(
            id="reu_0", text="Python", source_section="Skills", embedding=[0.1, 0.2]
        )

        profiler.save_evidence_unit_embeddings("fp-1", [unit])

        mock_store.save_evidence_unit_embeddings.assert_called_once()
        call_args = mock_store.save_evidence_unit_embeddings.call_args
        assert call_args[0][0] == "fp-1"
        payload = call_args[0][1]
        assert len(payload) == 1
        assert payload[0]["evidence_unit_id"] == "reu_0"

    def test_skips_units_without_embedding(self):
        mock_store = MagicMock()
        profiler = ResumeProfiler(ai_service=MagicMock(), store=mock_store)

        unit_no_embed = ResumeEvidenceUnit(id="reu_0", text="text", source_section="Skills")
        unit_with_embed = ResumeEvidenceUnit(
            id="reu_1", text="Python", source_section="Skills", embedding=[0.1]
        )

        profiler.save_evidence_unit_embeddings("fp-1", [unit_no_embed, unit_with_embed])

        payload = mock_store.save_evidence_unit_embeddings.call_args[0][1]
        assert len(payload) == 1
        assert payload[0]["evidence_unit_id"] == "reu_1"

    def test_does_nothing_without_store(self):
        profiler = ResumeProfiler(ai_service=MagicMock(), store=None)
        unit = ResumeEvidenceUnit(
            id="reu_0", text="Python", source_section="Skills", embedding=[0.1]
        )

        # Should not raise
        profiler.save_evidence_unit_embeddings("fp-1", [unit])

    def test_does_nothing_with_empty_units(self):
        mock_store = MagicMock()
        profiler = ResumeProfiler(ai_service=MagicMock(), store=mock_store)

        profiler.save_evidence_unit_embeddings("fp-1", [])

        mock_store.save_evidence_unit_embeddings.assert_not_called()


# ---------------------------------------------------------------------------
# embed_only
# ---------------------------------------------------------------------------


class TestEmbedOnly:
    def test_raises_on_empty_fingerprint(self):
        profiler = make_profiler()
        resume = make_resume_schema()

        with pytest.raises(ValueError, match="resume_fingerprint is required"):
            profiler.embed_only("", resume)

    def test_stop_event_raises_interrupted(self):
        profiler = make_profiler()
        resume = make_resume_schema()
        stop_event = threading.Event()
        stop_event.set()

        with pytest.raises(InterruptedError):
            profiler.embed_only("fp-1", resume, stop_event=stop_event)

    def test_returns_evidence_units(self):
        profiler = make_profiler()
        exp = make_experience()
        profile = make_profile(experience=[exp])
        resume = make_resume_schema(profile)

        result = profiler.embed_only("fp-1", resume)

        assert isinstance(result, list)
        assert all(isinstance(u, ResumeEvidenceUnit) for u in result)

    def test_embeddings_generated(self):
        profiler = make_profiler()
        exp = make_experience(highlights=[], tech_keywords=[])
        profile = make_profile(experience=[exp])
        resume = make_resume_schema(profile)

        result = profiler.embed_only("fp-1", resume)

        for unit in result:
            assert unit.embedding is not None


# ---------------------------------------------------------------------------
# profile_resume
# ---------------------------------------------------------------------------


class TestProfileResume:
    def test_full_pipeline(self):
        profiler = make_profiler()
        exp = make_experience()
        profile_data = make_profile(experience=[exp])
        make_resume_schema(profile_data)
        profiler.ai.extract_resume_data.return_value = {
            "profile": profile_data.model_dump(),
            "extraction": {"confidence": 0.9, "warnings": []},
        }

        resume, evidence_units, payload = profiler.profile_resume(
            resume_data={"raw_text": "my resume"},
            resume_fingerprint="fp-1",
        )

        assert resume is not None
        assert isinstance(evidence_units, list)
        assert isinstance(payload, list)

    def test_uses_pre_extracted_resume(self):
        profiler = make_profiler()
        profile_data = make_profile()
        pre_extracted = make_resume_schema(profile_data)

        resume, _, _ = profiler.profile_resume(
            resume_data={"raw_text": "ignored"},
            resume_fingerprint="fp-1",
            pre_extracted_resume=pre_extracted,
        )

        # AI extraction should not be called when pre_extracted is given
        profiler.ai.extract_resume_data.assert_not_called()
        assert resume is pre_extracted

    def test_raises_when_no_fingerprint_and_not_pre_extracted(self):
        profiler = make_profiler()

        with pytest.raises(ValueError, match="resume_fingerprint is required"):
            profiler.profile_resume(
                resume_data={"raw_text": "text"},
                resume_fingerprint="",
            )

    def test_returns_empty_when_extraction_fails(self):
        profiler = make_profiler()
        profiler.ai.extract_resume_data.return_value = None  # extraction fails

        resume, evidence_units, payload = profiler.profile_resume(
            resume_data={"raw_text": "bad data"},
            resume_fingerprint="fp-bad",
        )

        assert resume is None
        assert evidence_units == []
        assert payload == []

    def test_stop_event_interrupts_before_extraction(self):
        profiler = make_profiler()
        stop_event = threading.Event()
        stop_event.set()

        with pytest.raises(InterruptedError):
            profiler.profile_resume(
                resume_data={"raw_text": "text"},
                resume_fingerprint="fp-1",
                stop_event=stop_event,
            )

    def test_stop_event_interrupts_after_extraction(self):
        profiler = make_profiler()
        profile_data = make_profile()
        profiler.ai.extract_resume_data.return_value = {
            "profile": profile_data.model_dump(),
            "extraction": {"confidence": 0.9, "warnings": []},
        }
        stop_event = threading.Event()

        # Set stop event after extraction completes
        original_extract = profiler.extract_structured_resume

        def extract_then_stop(*args, **kwargs):
            result = original_extract(*args, **kwargs)
            stop_event.set()
            return result

        profiler.extract_structured_resume = extract_then_stop

        with pytest.raises(InterruptedError):
            profiler.profile_resume(
                resume_data={"raw_text": "text"},
                resume_fingerprint="fp-1",
                stop_event=stop_event,
            )
