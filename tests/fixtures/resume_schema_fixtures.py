#!/usr/bin/env python3
"""
Test fixtures for RESUME_SCHEMA validation.

Includes valid and invalid resume examples to verify schema correctness.
"""
import json

# Valid resume - should pass validation
VALID_RESUME = {
    "schema_version": "resume.v1",
    "document": {
        "source": {
            "filename": "resume.pdf",
            "content_type": "application/pdf",
            "language": "en"
        },
        "sections": [
            {
                "type": "experience",
                "title": "Work Experience",
                "order": 1,
                "items": [
                    {
                        "company": "Google",
                        "role": "Software Engineer",
                        "period": "2020-01 to 2023-12",
                        "description": "Built scalable systems",
                        "highlights": ["Led team of 5", "Improved performance by 50%"]
                    }
                ]
            }
        ]
    },
    "profile": {
        "person": {
            "full_name": "John Doe",
            "headline": "Senior Software Engineer",
            "location": {"city": "San Francisco", "region": "CA", "country": "USA"},
            "emails": ["john@example.com"],
            "phones": ["+1-555-1234"],
            "links": [{"platform": "linkedin", "url": "https://linkedin.com/in/johndoe"}]
        },
        "summary": {
            "text": "Experienced software engineer with 8 years in backend development",
            "claimed_total_experience_years": 8,
            "domain_focus": ["backend", "distributed systems"]
        },
        "experience": [
            {
                "company": "Google",
                "team": "Cloud Infrastructure",
                "title": "Senior Software Engineer",
                "employment_type": "full_time",
                "location": "Mountain View, CA",
                "start_date": "2020-01",
                "end_date": "2023-12",
                "is_current": False,
                "description": "Built distributed systems",
                "highlights": ["Led team of 5 engineers", "Reduced latency by 40%"],
                "tech": ["Python", "Kubernetes", "gRPC"],
                "impact": [
                    {"metric": "latency", "value": 40, "unit": "percent", "description": "Reduction in API response time"}
                ]
            }
        ],
        "education": [
            {
                "institution": "Stanford University",
                "degree": "Bachelor of Science",
                "field_of_study": "Computer Science",
                "start_year": 2012,
                "end_year": 2016
            }
        ],
        "skills": {
            "groups": [
                {"name": "languages", "skills": ["Python", "Go", "Java"]},
                {"name": "backend", "skills": ["Kubernetes", "Docker", "gRPC"]}
            ],
            "all": ["Python", "Go", "Java", "Kubernetes", "Docker"]
        }
    },
    "extraction": {
        "confidence": 0.95,
        "warnings": [],
        "provenance": [{"field": "experience", "source_section_index": 0}]
    }
}


# Invalid: Invalid date format (YYYY/MM instead of YYYY-MM)
INVALID_DATE_FORMAT = {
    "schema_version": "resume.v1",
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Software Engineer",
                "start_date": "2020/01",  # Wrong format - should be 2020-01
                "end_date": "2023-12"
            }
        ]
    }
}


# Invalid: Missing required fields (company and title)
INVALID_MISSING_REQUIRED = {
    "schema_version": "resume.v1",
    "profile": {
        "experience": [
            {
                # Missing "company" and "title"
                "start_date": "2020-01",
                "end_date": "2023-12"
            }
        ]
    }
}


# Invalid: employment_type as string "null" instead of actual null
INVALID_NULL_STRING = {
    "schema_version": "resume.v1",
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Software Engineer",
                "employment_type": "null",  # Wrong - should be null (not string)
                "start_date": "2020-01"
            }
        ]
    }
}


# Invalid: Additional properties at top level (should fail additionalProperties: false)
INVALID_EXTRA_PROPERTIES = {
    "schema_version": "resume.v1",
    "profile": {
        "experience": [{"company": "Google", "title": "Engineer", "start_date": "2020-01"}]
    },
    "extra_field": "should not be allowed"  # Should fail validation
}


# Valid: employment_type as actual null (not string)
VALID_NULL_EMPLOYMENT_TYPE = {
    "schema_version": "resume.v1",
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Software Engineer",
                "employment_type": None,  # Correct - actual null
                "start_date": "2020-01"
            }
        ]
    }
}


if __name__ == "__main__":
    # Print all fixtures for inspection
    fixtures = {
        "valid_resume": VALID_RESUME,
        "valid_null_employment": VALID_NULL_EMPLOYMENT_TYPE,
        "invalid_date_format": INVALID_DATE_FORMAT,
        "invalid_missing_required": INVALID_MISSING_REQUIRED,
        "invalid_null_string": INVALID_NULL_STRING,
        "invalid_extra_properties": INVALID_EXTRA_PROPERTIES
    }
    
    for name, fixture in fixtures.items():
        print(f"\n{'='*60}")
        print(f"Fixture: {name}")
        print(f"{'='*60}")
        print(json.dumps(fixture, indent=2))
