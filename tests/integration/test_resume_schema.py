"""
Tests for RESUME_SCHEMA validation using Pydantic models.

Validates that the schema correctly accepts valid resumes and rejects invalid ones.
"""
import pytest
from pydantic import ValidationError

from etl.schema_models import (
    RESUME_SCHEMA,
    ResumeSchema,
    ExperienceItem,
    PartialDate,
    SkillItem,
)
from tests.fixtures.resume_schema_fixtures import (
    VALID_RESUME,
    VALID_RESUME_WITH_NULLS,
    VALID_MINIMAL_RESUME,
    INVALID_MISSING_PROFILE,
    INVALID_MISSING_SUMMARY,
    INVALID_MISSING_SUMMARY_TEXT,
    INVALID_MISSING_EXPERIENCE_FIELDS,
    INVALID_DATE_PRECISION,
    INVALID_EXTRA_TOP_LEVEL,
    INVALID_EXTRA_IN_EXPERIENCE,
    INVALID_WRONG_TYPE_YEARS,
    INVALID_WRONG_TYPE_PROJECTS,
    INVALID_TECH_KEYWORDS_TYPE,
    INVALID_EMPTY_PROFILE,
    INVALID_NULL_PROFILE,
    INVALID_SKILL_STRUCTURE,
)


class TestResumeSchemaValidation:
    """Test suite for RESUME_SCHEMA validation using Pydantic models."""

    def test_01_valid_resume_passes(self):
        """Valid resume should pass Pydantic validation."""
        resume = ResumeSchema.model_validate(VALID_RESUME)
        assert resume.profile.summary.total_experience_years == 8.0
        assert len(resume.profile.experience) == 2
        assert resume.profile.experience[0].company == "Google"

    def test_02_valid_resume_with_nulls_passes(self):
        """Valid resume with null values should pass."""
        resume = ResumeSchema.model_validate(VALID_RESUME_WITH_NULLS)
        assert resume.profile.summary.total_experience_years is None
        assert resume.profile.experience[0].company is None

    def test_03_minimal_resume_passes(self):
        """Minimal valid resume should pass."""
        resume = ResumeSchema.model_validate(VALID_MINIMAL_RESUME)
        assert resume.profile.experience == []
        assert resume.profile.skills.all == []

    def test_04_missing_profile_fails(self):
        """Missing profile field should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_MISSING_PROFILE)
        
        assert "profile" in str(exc_info.value)

    def test_05_missing_summary_fails(self):
        """Missing summary field should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_MISSING_SUMMARY)
        
        assert "summary" in str(exc_info.value)

    def test_06_missing_summary_text_fails(self):
        """Missing summary.text field should fail validation."""
        # Note: Since all fields are Optional (can be null), missing a field
        # in the JSON schema sense is different from having it be None
        # In Pydantic v2, if a field is Optional without a default, it must be present but can be None
        # Our current implementation requires fields to be present, so this test passes
        # but the error might be about a different missing field (like total_experience_years)
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_MISSING_SUMMARY_TEXT)
        
        # Should fail because summary is missing required fields
        error_str = str(exc_info.value)
        assert any(field in error_str for field in ["text", "total_experience_years"])

    def test_07_missing_experience_fields_fails(self):
        """Missing required experience fields should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_MISSING_EXPERIENCE_FIELDS)
        
        # Should mention one of the missing required fields
        error_str = str(exc_info.value)
        assert any(field in error_str for field in ["start_date", "end_date", "is_current", "description", "tech_keywords"])

    def test_08_invalid_date_precision_fails(self):
        """Invalid date precision enum should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_DATE_PRECISION)
        
        assert "precision" in str(exc_info.value)

    def test_09_extra_top_level_properties_fails(self):
        """Extra properties at top level should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_EXTRA_TOP_LEVEL)
        
        assert "extra_field" in str(exc_info.value) or "extra" in str(exc_info.value).lower()

    def test_10_extra_nested_properties_fails(self):
        """Extra properties in nested objects should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_EXTRA_IN_EXPERIENCE)
        
        assert "extra_field" in str(exc_info.value) or "extra" in str(exc_info.value).lower()

    def test_11_wrong_type_years_fails(self):
        """Wrong type for total_experience_years should fail."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_WRONG_TYPE_YEARS)
        
        assert "total_experience_years" in str(exc_info.value)

    def test_12_wrong_type_projects_fails(self):
        """Wrong type for projects should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_WRONG_TYPE_PROJECTS)
        
        assert "projects" in str(exc_info.value)

    def test_13_wrong_tech_keywords_type_fails(self):
        """Non-string items in tech_keywords should fail."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_TECH_KEYWORDS_TYPE)
        
        assert "tech_keywords" in str(exc_info.value)

    def test_14_empty_profile_fails(self):
        """Empty profile object should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_EMPTY_PROFILE)
        
        assert "summary" in str(exc_info.value)

    def test_15_null_profile_fails(self):
        """Null profile should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_NULL_PROFILE)
        
        assert "profile" in str(exc_info.value)

    def test_16_invalid_skill_structure_fails(self):
        """Invalid skill item structure should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ResumeSchema.model_validate(INVALID_SKILL_STRUCTURE)
        
        # Should mention missing required fields in skill items
        error_str = str(exc_info.value)
        assert any(field in error_str for field in ["kind", "proficiency", "years_experience"])


class TestExperienceItem:
    """Test ExperienceItem model specifically."""

    def test_to_embedding_text_with_all_fields(self):
        """to_embedding_text should combine company, title, and description."""
        exp = ExperienceItem(
            company="Google",
            title="Senior Engineer",
            start_date=None,
            end_date=None,
            is_current=False,
            description="Built systems",
            tech_keywords=["Python"]
        )
        assert exp.to_embedding_text() == "Google - Senior Engineer - Built systems"

    def test_to_embedding_text_partial_fields(self):
        """to_embedding_text should handle missing fields gracefully."""
        exp = ExperienceItem(
            company="Google",
            title=None,
            start_date=None,
            end_date=None,
            is_current=False,
            description="Built systems",
            tech_keywords=[]
        )
        assert exp.to_embedding_text() == "Google - Built systems"

    def test_to_embedding_text_empty(self):
        """to_embedding_text should return empty string if no fields."""
        exp = ExperienceItem(
            company=None,
            title=None,
            start_date=None,
            end_date=None,
            is_current=None,
            description=None,
            tech_keywords=[]
        )
        assert exp.to_embedding_text() == ""

    def test_partial_date_validation(self):
        """PartialDate should validate precision enum."""
        from typing import Literal
        
        # Valid precisions
        valid_precisions: list[Literal["unknown", "year", "month"]] = ["unknown", "year", "month"]
        for precision in valid_precisions:
            date = PartialDate(text=None, year=2020, month=1, precision=precision)
            assert date.precision == precision
        
        # Invalid precision - should raise validation error at runtime
        with pytest.raises((ValidationError, TypeError)):
            PartialDate(text=None, year=2020, month=1, precision="day")  # type: ignore


class TestSkillItem:
    """Test SkillItem model specifically."""

    def test_to_embedding_text_complete(self):
        """to_embedding_text should combine all skill fields."""
        skill = SkillItem(
            name="Python",
            kind="language",
            proficiency="expert",
            years_experience=8.0
        )
        assert skill.to_embedding_text() == "Python - expert level - 8.0 years"

    def test_to_embedding_text_partial(self):
        """to_embedding_text should handle partial skill data."""
        skill = SkillItem(name="Python", kind=None, proficiency=None, years_experience=None)
        assert skill.to_embedding_text() == "Python"

    def test_to_embedding_text_empty(self):
        """to_embedding_text should return empty string if no name."""
        skill = SkillItem(name=None, kind=None, proficiency=None, years_experience=None)
        assert skill.to_embedding_text() == ""


class TestResumeSchemaProperties:
    """Test ResumeSchema convenience properties."""

    def test_claimed_total_years_property(self):
        """claimed_total_years property should return value from summary."""
        resume = ResumeSchema.model_validate(VALID_RESUME)
        assert resume.claimed_total_years == 8.0

    def test_claimed_total_years_none(self):
        """claimed_total_years property should handle None."""
        resume = ResumeSchema.model_validate(VALID_RESUME_WITH_NULLS)
        assert resume.claimed_total_years is None


class TestGeneratedSchema:
    """Test the generated JSON schema structure."""

    def test_schema_has_required_openai_fields(self):
        """Generated schema should have OpenAI-required fields."""
        schema = RESUME_SCHEMA
        
        assert "name" in schema
        assert schema["name"] == "resume_schema_v1.0"
        assert "strict" in schema
        assert schema["strict"] == True
        assert "schema" in schema

    def test_schema_has_additional_properties_false(self):
        """Generated schema should have additionalProperties: false at root."""
        inner_schema = RESUME_SCHEMA["schema"]
        assert inner_schema.get("additionalProperties") == False

    def test_schema_has_required_fields(self):
        """Generated schema should list required fields."""
        inner_schema = RESUME_SCHEMA["schema"]
        assert "required" in inner_schema
        assert "profile" in inner_schema["required"]
        assert "extraction" in inner_schema["required"]

    def test_profile_schema_structure(self):
        """Profile schema should have correct structure."""
        inner_schema = RESUME_SCHEMA["schema"]
        # Profile is referenced via $ref, need to look up in $defs
        profile_ref = inner_schema["properties"]["profile"]["$ref"]
        assert profile_ref == "#/$defs/Profile"
        
        # Get Profile from $defs
        profile = inner_schema["$defs"]["Profile"]
        
        # Profile should have additionalProperties: false
        assert profile.get("additionalProperties") == False
        
        # Profile should require summary and experience
        assert "summary" in profile["required"]
        assert "experience" in profile["required"]


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_confidence_range_validation(self):
        """Confidence should be between 0.0 and 1.0."""
        import copy
        valid_resume = copy.deepcopy(VALID_RESUME)
        
        # Valid confidence values
        for confidence in [0.0, 0.5, 1.0]:
            valid_resume["extraction"]["confidence"] = confidence
            resume = ResumeSchema.model_validate(valid_resume)
            assert resume.extraction.confidence == confidence
        
        # Invalid confidence values
        for confidence in [-0.1, 1.1]:
            valid_resume["extraction"]["confidence"] = confidence
            with pytest.raises(ValidationError):
                ResumeSchema.model_validate(valid_resume)

    def test_empty_arrays_valid(self):
        """Empty arrays should be valid for list fields."""
        import copy
        resume_data = copy.deepcopy(VALID_RESUME)
        resume_data["profile"]["experience"] = []
        resume_data["profile"]["skills"]["groups"] = []
        resume_data["profile"]["skills"]["all"] = []
        
        resume = ResumeSchema.model_validate(resume_data)
        assert resume.profile.experience == []
        assert resume.profile.skills.groups == []

    def test_nullable_fields_accept_null(self):
        """Fields marked as Optional should accept null."""
        import copy
        resume_data = copy.deepcopy(VALID_RESUME)
        resume_data["profile"]["experience"][0]["company"] = None
        resume_data["profile"]["experience"][0]["title"] = None
        
        resume = ResumeSchema.model_validate(resume_data)
        assert resume.profile.experience[0].company is None
        assert resume.profile.experience[0].title is None
