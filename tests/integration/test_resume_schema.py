"""
Tests for RESUME_SCHEMA validation.

Validates that the schema correctly accepts valid resumes and rejects invalid ones.
"""
import json
import pytest
from jsonschema import validate, ValidationError, Draft202012Validator

from etl.schemas import RESUME_SCHEMA
from tests.fixtures.resume_schema_fixtures import (
    VALID_RESUME,
    VALID_NULL_EMPLOYMENT_TYPE,
    INVALID_DATE_FORMAT,
    INVALID_MISSING_REQUIRED,
    INVALID_NULL_STRING,
    INVALID_EXTRA_PROPERTIES
)


class TestResumeSchemaValidation:
    """Test suite for RESUME_SCHEMA validation per SRS requirements."""
    
    @pytest.fixture
    def validator(self):
        """Create a JSON Schema validator for RESUME_SCHEMA."""
        return Draft202012Validator(RESUME_SCHEMA)
    
    def test_01_schema_validity(self, validator):
        """AC1: Schema loads and validates valid resume without errors."""
        # Should not raise any exception
        validator.validate(VALID_RESUME)
        assert True, "Valid resume passed validation"
    
    def test_02_employment_type_null_passes(self, validator):
        """AC2: employment_type: null passes, employment_type: "null" fails."""
        # Valid: actual null value
        validator.validate(VALID_NULL_EMPLOYMENT_TYPE)
        
        # Invalid: string "null" should fail
        with pytest.raises(ValidationError) as exc_info:
            validator.validate(INVALID_NULL_STRING)
        
        assert "null" in str(exc_info.value) or "enum" in str(exc_info.value)
    
    def test_03_missing_required_fields_fails(self, validator):
        """AC3: Missing company/title fails at experience entry level."""
        with pytest.raises(ValidationError) as exc_info:
            validator.validate(INVALID_MISSING_REQUIRED)
        
        error_path = list(exc_info.value.path)
        # Error should be at the experience entry level, not inside impact
        assert "experience" in str(error_path) or "company" in str(exc_info.value)
        assert "impact" not in str(error_path), "Error should not be inside impact"
    
    def test_04_invalid_date_format_fails(self, validator):
        """AC4: Invalid date format (not YYYY-MM) fails validation."""
        with pytest.raises(ValidationError) as exc_info:
            validator.validate(INVALID_DATE_FORMAT)
        
        # Error should mention pattern or format
        assert any(keyword in str(exc_info.value).lower() for keyword in ["pattern", "format", "date"])
    
    def test_05_extra_properties_fails(self, validator):
        """AC5: Additional properties at top level fails."""
        with pytest.raises(ValidationError) as exc_info:
            validator.validate(INVALID_EXTRA_PROPERTIES)
        
        assert "additionalProperties" in str(exc_info.value) or "extra_field" in str(exc_info.value)
    
    def test_06_schema_has_schema_declaration(self):
        """Schema must include $schema declaration."""
        assert "$schema" in RESUME_SCHEMA
        assert RESUME_SCHEMA["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    
    def test_07_schema_has_required_top_level(self):
        """Top-level required fields are schema_version and profile."""
        assert "required" in RESUME_SCHEMA
        required = RESUME_SCHEMA["required"]
        assert "schema_version" in required
        assert "profile" in required
    
    def test_08_employment_type_no_null_string_in_enum(self):
        """employment_type enum must not contain string 'null'."""
        experience_items = RESUME_SCHEMA["properties"]["profile"]["properties"]["experience"]["items"]
        employment_type = experience_items["properties"]["employment_type"]
        
        # Should be type array with null, not enum with "null" string
        assert "type" in employment_type
        assert "enum" not in employment_type or "null" not in employment_type.get("enum", [])
    
    def test_09_date_fields_have_pattern(self):
        """Date fields must have YYYY-MM pattern validation."""
        # Check that $defs.YYYY_MM_Date exists and has pattern
        assert "YYYY_MM_Date" in RESUME_SCHEMA.get("$defs", {})
        date_def = RESUME_SCHEMA["$defs"]["YYYY_MM_Date"]
        assert "pattern" in date_def
        assert date_def["pattern"] == "^[0-9]{4}-[0-9]{2}$"
    
    def test_10_experience_required_fields(self):
        """Experience entries require company and title at correct level."""
        experience_items = RESUME_SCHEMA["properties"]["profile"]["properties"]["experience"]["items"]
        assert "required" in experience_items
        required = experience_items["required"]
        assert "company" in required
        assert "title" in required
    
    def test_11_impact_no_required_company_title(self):
        """Impact items should NOT require company/title (they belong to experience)."""
        experience_items = RESUME_SCHEMA["properties"]["profile"]["properties"]["experience"]["items"]
        impact_items = experience_items["properties"]["impact"]["items"]
        
        # Impact should not have company/title in required
        if "required" in impact_items:
            required = impact_items["required"]
            assert "company" not in required
            assert "title" not in required
    
    def test_12_document_sections_items_typed(self):
        """document.sections[].items must have defined structure, not 'array of anything'."""
        section_items = RESUME_SCHEMA["properties"]["document"]["properties"]["sections"]["items"]
        items_schema = section_items["properties"]["items"]
        
        # Should have items definition with properties
        assert "items" in items_schema
        assert "properties" in items_schema["items"]
    
    def test_13_additional_properties_false_top_level(self):
        """Top level must have additionalProperties: False."""
        assert RESUME_SCHEMA.get("additionalProperties") == False
