"""
Test OpenAI Schema Compatibility

Verifies that generated JSON schemas from Pydantic models are compatible
with OpenAI's structured output requirements.

OpenAI strict mode requirements:
1. All fields must be defined in "required" arrays
2. additionalProperties must be false at all levels
3. No default values allowed in schema
4. No optional fields without explicit null type
5. Schema must have name, strict=true, and schema keys at top level
"""
import json
import pytest
from typing import Dict, Any, List

from etl.schema_models import (
    RESUME_SCHEMA,
    EXTRACTION_SCHEMA,
    FACET_EXTRACTION_SCHEMA_FOR_WANTS,
    ResumeSchema,
    JobExtraction,
    FacetExtraction,
)


class TestOpenAISchemaStructure:
    """Test that generated schemas meet OpenAI's structural requirements."""

    def test_resume_schema_has_required_top_level_fields(self):
        """Top-level schema must have name, strict, and schema keys."""
        assert "name" in RESUME_SCHEMA
        assert "strict" in RESUME_SCHEMA
        assert RESUME_SCHEMA["strict"] == True
        assert "schema" in RESUME_SCHEMA
        assert isinstance(RESUME_SCHEMA["schema"], dict)

    def test_job_extraction_schema_has_required_top_level_fields(self):
        """Top-level schema must have name, strict, and schema keys."""
        assert "name" in EXTRACTION_SCHEMA
        assert "strict" in EXTRACTION_SCHEMA
        assert EXTRACTION_SCHEMA["strict"] == True
        assert "schema" in EXTRACTION_SCHEMA
        assert isinstance(EXTRACTION_SCHEMA["schema"], dict)

    def test_facet_schema_has_required_top_level_fields(self):
        """Top-level schema must have name, strict, and schema keys."""
        assert "name" in FACET_EXTRACTION_SCHEMA_FOR_WANTS
        assert "strict" in FACET_EXTRACTION_SCHEMA_FOR_WANTS
        assert FACET_EXTRACTION_SCHEMA_FOR_WANTS["strict"] == True
        assert "schema" in FACET_EXTRACTION_SCHEMA_FOR_WANTS
        assert isinstance(FACET_EXTRACTION_SCHEMA_FOR_WANTS["schema"], dict)


class TestOpenAIStrictRequirements:
    """Test OpenAI strict mode requirements throughout schema."""

    def _check_additional_properties_false(self, schema: Dict[str, Any], path: str = "root") -> List[str]:
        """Recursively check that all objects have additionalProperties: false."""
        errors = []
        
        if isinstance(schema, dict):
            # Check if this is an object definition
            if schema.get("type") == "object":
                if schema.get("additionalProperties") != False:
                    errors.append(f"{path}: additionalProperties must be false for objects")
            
            # Recurse into properties
            if "properties" in schema:
                for prop_name, prop_schema in schema["properties"].items():
                    errors.extend(
                        self._check_additional_properties_false(
                            prop_schema, 
                            f"{path}.properties.{prop_name}"
                        )
                    )
            
            # Recurse into items (for arrays)
            if "items" in schema:
                errors.extend(
                    self._check_additional_properties_false(
                        schema["items"],
                        f"{path}.items"
                    )
                )
            
            # Recurse into anyOf/allOf/oneOf
            for key in ["anyOf", "allOf", "oneOf"]:
                if key in schema:
                    for i, sub_schema in enumerate(schema[key]):
                        errors.extend(
                            self._check_additional_properties_false(
                                sub_schema,
                                f"{path}.{key}[{i}]"
                            )
                        )
            
            # Recurse into $defs
            if "$defs" in schema:
                for def_name, def_schema in schema["$defs"].items():
                    errors.extend(
                        self._check_additional_properties_false(
                            def_schema,
                            f"{path}.$defs.{def_name}"
                        )
                    )
        
        return errors

    def test_resume_schema_has_additional_properties_false(self):
        """All objects in resume schema must have additionalProperties: false."""
        inner_schema = RESUME_SCHEMA["schema"]
        errors = self._check_additional_properties_false(inner_schema)
        
        if errors:
            pytest.fail(f"additionalProperties: false missing:\n" + "\n".join(errors))

    def test_job_extraction_schema_has_additional_properties_false(self):
        """All objects in job extraction schema must have additionalProperties: false."""
        inner_schema = EXTRACTION_SCHEMA["schema"]
        errors = self._check_additional_properties_false(inner_schema)
        
        if errors:
            pytest.fail(f"additionalProperties: false missing:\n" + "\n".join(errors))

    def test_facet_schema_has_additional_properties_false(self):
        """All objects in facet schema must have additionalProperties: false."""
        inner_schema = FACET_EXTRACTION_SCHEMA_FOR_WANTS["schema"]
        errors = self._check_additional_properties_false(inner_schema)
        
        if errors:
            pytest.fail(f"additionalProperties: false missing:\n" + "\n".join(errors))

    def _check_no_default_values(self, schema: Dict[str, Any], path: str = "root") -> List[str]:
        """Check that schema doesn't contain default values (not allowed in strict mode)."""
        OPTIONAL_FIELDS = set()  # All optional fields use nullable types, not defaults

        errors = []

        if isinstance(schema, dict):
            # Check for default (skip optional fields)
            if "default" in schema:
                # Extract the field name from the path
                field_name = path.split('.')[-1] if '.' in path else path
                if field_name not in OPTIONAL_FIELDS:
                    errors.append(f"{path}: has 'default' value (not allowed in strict mode)")

            # Recurse
            for key, value in schema.items():
                if isinstance(value, (dict, list)):
                    errors.extend(self._check_no_default_values(value, f"{path}.{key}"))
        elif isinstance(schema, list):
            for i, item in enumerate(schema):
                errors.extend(self._check_no_default_values(item, f"{path}[{i}]"))

        return errors

    def test_resume_schema_has_no_defaults(self):
        """OpenAI strict mode doesn't allow default values in schema."""
        inner_schema = RESUME_SCHEMA["schema"]
        errors = self._check_no_default_values(inner_schema)
        
        if errors:
            pytest.fail(f"Default values found:\n" + "\n".join(errors))

    def _check_all_fields_required(self, schema: Dict[str, Any], path: str = "root") -> List[str]:
        """Check that all objects have required arrays matching their properties."""
        errors = []

        if isinstance(schema, dict):
            if schema.get("type") == "object" and "properties" in schema:
                required = set(schema.get("required", []))
                properties = set(schema["properties"].keys())

                missing_required = properties - required
                if missing_required:
                    errors.append(
                        f"{path}: properties not in required: {missing_required}"
                    )

            # Recurse into properties
            if "properties" in schema:
                for prop_name, prop_schema in schema["properties"].items():
                    errors.extend(
                        self._check_all_fields_required(
                            prop_schema,
                            f"{path}.properties.{prop_name}"
                        )
                    )
            
            # Recurse into items
            if "items" in schema:
                errors.extend(
                    self._check_all_fields_required(
                        schema["items"],
                        f"{path}.items"
                    )
                )
            
            # Recurse into anyOf
            if "anyOf" in schema:
                for i, sub_schema in enumerate(schema["anyOf"]):
                    errors.extend(
                        self._check_all_fields_required(
                            sub_schema,
                            f"{path}.anyOf[{i}]"
                        )
                    )
            
            # Recurse into allOf
            if "allOf" in schema:
                for i, sub_schema in enumerate(schema["allOf"]):
                    errors.extend(
                        self._check_all_fields_required(
                            sub_schema,
                            f"{path}.allOf[{i}]"
                        )
                    )
            
            # Recurse into oneOf
            if "oneOf" in schema:
                for i, sub_schema in enumerate(schema["oneOf"]):
                    errors.extend(
                        self._check_all_fields_required(
                            sub_schema,
                            f"{path}.oneOf[{i}]"
                        )
                    )
            
            # Recurse into $defs
            if "$defs" in schema:
                for def_name, def_schema in schema["$defs"].items():
                    errors.extend(
                        self._check_all_fields_required(
                            def_schema,
                            f"{path}.$defs.{def_name}"
                        )
                    )
        
        return errors

    def test_resume_schema_all_fields_required(self):
        """All properties must be listed in required array for strict mode."""
        inner_schema = RESUME_SCHEMA["schema"]
        errors = self._check_all_fields_required(inner_schema)
        
        if errors:
            pytest.fail(f"Missing required fields:\n" + "\n".join(errors))

    def test_job_extraction_schema_has_no_defaults(self):
        """EXTRACTION_SCHEMA should have no default values."""
        inner_schema = EXTRACTION_SCHEMA["schema"]
        errors = self._check_no_default_values(inner_schema)
        
        if errors:
            pytest.fail(f"Default values found:\n" + "\n".join(errors))

    def test_job_extraction_schema_all_fields_required(self):
        """All properties in EXTRACTION_SCHEMA must be listed in required array."""
        inner_schema = EXTRACTION_SCHEMA["schema"]
        errors = self._check_all_fields_required(inner_schema)
        
        if errors:
            pytest.fail(f"Missing required fields:\n" + "\n".join(errors))

    def test_facet_schema_has_no_defaults(self):
        """FACET_EXTRACTION_SCHEMA_FOR_WANTS should have no default values."""
        inner_schema = FACET_EXTRACTION_SCHEMA_FOR_WANTS["schema"]
        errors = self._check_no_default_values(inner_schema)
        
        if errors:
            pytest.fail(f"Default values found:\n" + "\n".join(errors))

    def test_facet_schema_all_fields_required(self):
        """All properties in FACET_EXTRACTION_SCHEMA_FOR_WANTS must be listed in required array."""
        inner_schema = FACET_EXTRACTION_SCHEMA_FOR_WANTS["schema"]
        errors = self._check_all_fields_required(inner_schema)
        
        if errors:
            pytest.fail(f"Missing required fields:\n" + "\n".join(errors))


class TestOpenAIExampleValidation:
    """Test that valid examples pass schema validation."""

    def test_resume_schema_accepts_valid_resume(self):
        """Valid resume should generate a schema that accepts it."""
        from tests.fixtures.resume_schema_fixtures import VALID_RESUME
        
        # This validates the resume against the Pydantic model
        resume = ResumeSchema.model_validate(VALID_RESUME)
        assert resume is not None
        
        # Also verify the schema structure is valid
        inner_schema = RESUME_SCHEMA["schema"]
        assert "properties" in inner_schema
        assert "profile" in inner_schema["properties"]
        assert "extraction" in inner_schema["properties"]

    def test_job_extraction_schema_accepts_valid_job(self):
        """Valid job extraction should generate a schema that accepts it."""
        valid_job = {
            "thought_process": "This is a senior Python role requiring 5+ years experience",
            "job_summary": "Senior Python Developer for backend microservices",
            "seniority_level": "Senior",
            "remote_policy": "Hybrid",
            "visa_sponsorship_available": True,
            "min_years_experience": 5,
            "requires_degree": False,
            "security_clearance": False,
            "salary_min": 100000,
            "salary_max": 150000,
            "currency": "USD",
            "tech_stack": ["Python", "Django", "PostgreSQL", "AWS"],
            "requirements": [
                {
                    "req_type": "must_have",
                    "category": "technical",
                    "text": "5+ years Python experience",
                    "related_skills": ["Python"],
                    "proficiency": "expert"
                }
            ],
            "benefits": [
                {
                    "category": "health_insurance",
                    "text": "Comprehensive health coverage"
                }
            ]
        }
        
        job = JobExtraction.model_validate(valid_job)
        assert job is not None

    def test_facet_schema_accepts_valid_facets(self):
        """Valid facet extraction should generate a schema that accepts it."""
        valid_facets = {
            "remote_flexibility": "Fully remote with flexible hours",
            "compensation": "$100k-$150k base salary plus equity",
            "learning_growth": "$2000 learning budget annually",
            "company_culture": "Fast-paced startup environment",
            "work_life_balance": "Flexible PTO policy",
            "tech_stack": "Python, React, PostgreSQL, AWS",
            "visa_sponsorship": "H1B sponsorship available"
        }
        
        facets = FacetExtraction.model_validate(valid_facets)
        assert facets is not None


class TestSchemaSerialization:
    """Test that schemas can be serialized for OpenAI API."""

    def test_resume_schema_is_json_serializable(self):
        """Schema must be JSON serializable for API calls."""
        try:
            schema_json = json.dumps(RESUME_SCHEMA)
            assert schema_json is not None
            assert len(schema_json) > 0
            
            # Verify it can be parsed back
            parsed = json.loads(schema_json)
            assert parsed["name"] == RESUME_SCHEMA["name"]
        except (TypeError, ValueError) as e:
            pytest.fail(f"Schema is not JSON serializable: {e}")

    def test_job_extraction_schema_is_json_serializable(self):
        """Schema must be JSON serializable for API calls."""
        try:
            schema_json = json.dumps(EXTRACTION_SCHEMA)
            assert schema_json is not None
            assert len(schema_json) > 0
        except (TypeError, ValueError) as e:
            pytest.fail(f"Schema is not JSON serializable: {e}")

    def test_facet_schema_is_json_serializable(self):
        """Schema must be JSON serializable for API calls."""
        try:
            schema_json = json.dumps(FACET_EXTRACTION_SCHEMA_FOR_WANTS)
            assert schema_json is not None
            assert len(schema_json) > 0
        except (TypeError, ValueError) as e:
            pytest.fail(f"Schema is not JSON serializable: {e}")


class TestSchemaDocumentation:
    """Test that schemas have proper documentation."""

    def test_resume_schema_has_descriptions(self):
        """Schema fields should have descriptions for AI guidance."""
        inner_schema = RESUME_SCHEMA["schema"]
        
        def check_descriptions(schema: Dict[str, Any], path: str = "root") -> List[str]:
            errors = []
            if isinstance(schema, dict):
                if "properties" in schema:
                    for prop_name, prop_schema in schema["properties"].items():
                        if isinstance(prop_schema, dict):
                            if "description" not in prop_schema and "$ref" not in prop_schema:
                                errors.append(f"{path}.{prop_name} missing description")
                            if "properties" in prop_schema or "$defs" in prop_schema:
                                errors.extend(check_descriptions(prop_schema, f"{path}.{prop_name}"))
            return errors
        
        # This test enforces that all fields have descriptions
        errors = check_descriptions(inner_schema)
        assert not errors, f"Fields without descriptions: {errors}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
