#!/usr/bin/env python3
"""
Test fixtures for RESUME_SCHEMA validation.

Includes valid and invalid resume examples to verify schema correctness.
All fixtures match the Pydantic model structure in etl.schema_models.
"""
import json
import copy


# ============================================================================
# VALID RESUME FIXTURES
# ============================================================================

VALID_RESUME = {
    "profile": {
        "summary": {
            "text": "Experienced software engineer with 8 years in backend development and distributed systems. Passionate about building scalable solutions.",
            "total_experience_years": 8.0
        },
        "experience": [
            {
                "company": "Google",
                "title": "Senior Software Engineer",
                "start_date": {
                    "text": "2020-01",
                    "year": 2020,
                    "month": 1,
                    "precision": "month"
                },
                "end_date": {
                    "text": "2023-12",
                    "year": 2023,
                    "month": 12,
                    "precision": "month"
                },
                "is_current": False,
                "description": "Built distributed systems handling millions of requests per day. Led team of 5 engineers.",
                "years_value": None,
                "tech_keywords": ["Python", "Kubernetes", "gRPC", "PostgreSQL"]
            },
            {
                "company": "StartupXYZ",
                "title": "Software Engineer",
                "start_date": {
                    "text": "2018-06",
                    "year": 2018,
                    "month": 6,
                    "precision": "month"
                },
                "end_date": {
                    "text": "2019-12",
                    "year": 2019,
                    "month": 12,
                    "precision": "month"
                },
                "is_current": False,
                "description": "Full-stack development using React and Node.js. Implemented CI/CD pipelines.",
                "years_value": None,
                "tech_keywords": ["React", "Node.js", "Docker", "AWS"]
            }
        ],
        "projects": {
            "items": [
                {
                    "name": "Open Source Contribution",
                    "description": "Contributed to open-source projects.",
                    "technologies": ["Python", "Git"],
                    "url": "https://github.com/example/contribution",
                    "date": {
                        "text": "2023",
                        "year": 2023,
                        "month": None,
                        "precision": "year"
                    }
                },
                {
                    "name": "Personal Portfolio",
                    "description": "Built personal portfolio site with Next.js.",
                    "technologies": ["Next.js", "React", "TypeScript"],
                    "url": "https://example.com",
                    "date": None
                }
            ]
        },
        "education": [
            {
                "degree": "Bachelor of Science",
                "field_of_study": "Computer Science",
                "institution": "Stanford University",
                "graduation_year": 2018,
                "description": "Graduated with honors. GPA 3.8/4.0."
            }
        ],
        "skills": {
            "groups": [
                {
                    "group_name": "Programming Languages",
                    "items": [
                        {"name": "Python", "kind": "language", "proficiency": "expert", "years_experience": 8.0},
                        {"name": "Go", "kind": "language", "proficiency": "proficient", "years_experience": 4.0},
                        {"name": "JavaScript", "kind": "language", "proficiency": "proficient", "years_experience": 6.0}
                    ]
                },
                {
                    "group_name": "Frameworks & Tools",
                    "items": [
                        {"name": "Kubernetes", "kind": "tool", "proficiency": "expert", "years_experience": 5.0},
                        {"name": "Docker", "kind": "tool", "proficiency": "expert", "years_experience": 6.0},
                        {"name": "PostgreSQL", "kind": "database", "proficiency": "proficient", "years_experience": 7.0}
                    ]
                }
            ],
            "all": [
                {"name": "Python", "kind": "language", "proficiency": "expert", "years_experience": 8.0},
                {"name": "Go", "kind": "language", "proficiency": "proficient", "years_experience": 4.0},
                {"name": "JavaScript", "kind": "language", "proficiency": "proficient", "years_experience": 6.0},
                {"name": "Kubernetes", "kind": "tool", "proficiency": "expert", "years_experience": 5.0},
                {"name": "Docker", "kind": "tool", "proficiency": "expert", "years_experience": 6.0},
                {"name": "PostgreSQL", "kind": "database", "proficiency": "proficient", "years_experience": 7.0}
            ]
        },
        "certifications": [
            {
                "name": "AWS Solutions Architect - Professional",
                "issuer": "Amazon Web Services",
                "issued_year": 2022,
                "expires_year": 2025
            }
        ],
        "languages": [
            {"language": "English", "proficiency": "native"},
            {"language": "Spanish", "proficiency": "conversational"}
        ]
    },
    "extraction": {
        "confidence": 0.95,
        "warnings": []
    }
}


# Valid resume with null values allowed
VALID_RESUME_WITH_NULLS = {
    "profile": {
        "summary": {
            "text": "Recent graduate seeking entry-level position.",
            "total_experience_years": None
        },
        "experience": [
            {
                "company": None,
                "title": None,
                "start_date": None,
                "end_date": None,
                "is_current": None,
                "description": None,
                "years_value": None,
                "tech_keywords": []
            }
        ],
        "projects": {
            "items": []
        },
        "education": [
            {
                "degree": None,
                "field_of_study": None,
                "institution": None,
                "graduation_year": None,
                "description": None
            }
        ],
        "skills": {
            "groups": [],
            "all": []
        },
        "certifications": [],
        "languages": []
    },
    "extraction": {
        "confidence": None,
        "warnings": ["Limited experience detected"]
    }
}


# Valid resume with only required fields
VALID_MINIMAL_RESUME = {
    "profile": {
        "summary": {
            "text": "Software engineer.",
            "total_experience_years": None
        },
        "experience": [],
        "projects": {"items": []},
        "education": [],
        "skills": {"groups": [], "all": []},
        "certifications": [],
        "languages": []
    },
    "extraction": {
        "confidence": 0.85,
        "warnings": []
    }
}


# ============================================================================
# INVALID RESUME FIXTURES
# ============================================================================

def make_invalid_resume(modifications: dict) -> dict:
    """Helper to create invalid resume from valid base.
    
    Note: This performs a deep merge. For list fields like 'experience',
    it will replace the entire list. To test individual items in a list,
    use direct fixture definitions instead of this helper.
    """
    invalid = copy.deepcopy(VALID_RESUME)
    
    def deep_update(d, u):
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                deep_update(d[k], v)
            else:
                d[k] = v
    
    deep_update(invalid, modifications)
    return invalid


# Missing required field at top level
INVALID_MISSING_PROFILE = {
    "extraction": {
        "confidence": 0.95,
        "warnings": []
    }
    # Missing "profile"
}


# Missing required field in profile
INVALID_MISSING_SUMMARY = make_invalid_resume({
    "profile": {
        "summary": None  # Should be an object
    }
})


# Missing required field in summary
INVALID_MISSING_SUMMARY_TEXT = {
    "profile": {
        "summary": {
            "total_experience_years": 5.0
            # Missing "text"
        },
        "experience": [],
        "projects": {"items": []},
        "education": [],
        "skills": {"groups": [], "all": []},
        "certifications": [],
        "languages": []
    },
    "extraction": {"confidence": 0.95, "warnings": []}
}


# Missing required field in summary
INVALID_MISSING_SUMMARY_TEXT = {
    "profile": {
        "summary": {
            "total_experience_years": 5.0
            # Missing "text"
        },
        "experience": [],
        "projects": {"items": []},
        "education": [],
        "skills": {"groups": [], "all": []},
        "certifications": [],
        "languages": []
    },
    "extraction": {"confidence": 0.95, "warnings": []}
}


# Missing required field in experience item
INVALID_MISSING_EXPERIENCE_FIELDS = make_invalid_resume({
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Engineer",
                "years_value": None
                # Missing required fields: start_date, end_date, is_current, description, tech_keywords
            }
        ]
    }
})


# Invalid date precision (not in enum) - complete experience item with invalid precision
INVALID_DATE_PRECISION = make_invalid_resume({
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Senior Engineer",
                "start_date": {
                    "text": "2020-01",
                    "year": 2020,
                    "month": 1,
                    "precision": "day"  # Invalid - should be "unknown", "year", or "month"
                },
                "end_date": {
                    "text": "2023-12",
                    "year": 2023,
                    "month": 12,
                    "precision": "month"
                },
                "is_current": False,
                "description": "Built distributed systems",
                "years_value": None,
                "tech_keywords": ["Python"]
            }
        ]
    }
})


# Extra properties at top level (additionalProperties: false)
INVALID_EXTRA_TOP_LEVEL = {
    "profile": {
        "summary": {"text": "Test", "total_experience_years": 5.0},
        "experience": [],
        "projects": {"items": []},
        "education": [],
        "skills": {"groups": [], "all": []},
        "certifications": [],
        "languages": []
    },
    "extraction": {"confidence": 0.95, "warnings": []},
    "extra_field": "should not be allowed"  # Extra property
}


# Extra properties in nested object - complete experience item with extra field
INVALID_EXTRA_IN_EXPERIENCE = make_invalid_resume({
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Senior Engineer",
                "start_date": {
                    "text": "2020-01",
                    "year": 2020,
                    "month": 1,
                    "precision": "month"
                },
                "end_date": {
                    "text": "2023-12",
                    "year": 2023,
                    "month": 12,
                    "precision": "month"
                },
                "is_current": False,
                "description": "Built distributed systems",
                "years_value": None,
                "tech_keywords": ["Python"],
                "extra_field": "not allowed"  # Extra property - this should trigger validation error
            }
        ]
    }
})


# Wrong type (string instead of number)
INVALID_WRONG_TYPE_YEARS = make_invalid_resume({
    "profile": {
        "summary": {
            "text": "Test",
            "total_experience_years": "eight"  # Should be number or null
        }
    }
})


# Wrong type (array instead of object for projects)
INVALID_WRONG_TYPE_PROJECTS = make_invalid_resume({
    "profile": {
        "projects": ["project1", "project2"]  # Should be object with items field
    }
})


# Invalid type in tech_keywords (should be strings) - complete experience item with invalid tech_keywords
INVALID_TECH_KEYWORDS_TYPE = make_invalid_resume({
    "profile": {
        "experience": [
            {
                "company": "Google",
                "title": "Senior Engineer",
                "start_date": {
                    "text": "2020-01",
                    "year": 2020,
                    "month": 1,
                    "precision": "month"
                },
                "end_date": {
                    "text": "2023-12",
                    "year": 2023,
                    "month": 12,
                    "precision": "month"
                },
                "is_current": False,
                "description": "Built distributed systems",
                "years_value": None,
                "tech_keywords": ["Python", 123, "Java"]  # 123 is not a string - should trigger validation error
            }
        ]
    }
})


# Empty object (missing required fields)
INVALID_EMPTY_PROFILE = {
    "profile": {},
    "extraction": {"confidence": 0.95, "warnings": []}
}


# Null instead of required object
INVALID_NULL_PROFILE = {
    "profile": None,
    "extraction": {"confidence": 0.95, "warnings": []}
}


# Invalid skill proficiency structure
INVALID_SKILL_STRUCTURE = make_invalid_resume({
    "profile": {
        "skills": {
            "groups": [
                {
                    "group_name": "Languages",
                    "items": [
                        {"name": "Python"}  # Missing required fields: kind, proficiency, years_experience
                    ]
                }
            ],
            "all": [
                {"name": "Python"}  # Missing required fields
            ]
        }
    }
})


if __name__ == "__main__":
    # Print all fixtures for inspection
    fixtures = {
        "VALID_RESUME": VALID_RESUME,
        "VALID_RESUME_WITH_NULLS": VALID_RESUME_WITH_NULLS,
        "VALID_MINIMAL_RESUME": VALID_MINIMAL_RESUME,
        "INVALID_MISSING_PROFILE": INVALID_MISSING_PROFILE,
        "INVALID_MISSING_SUMMARY": INVALID_MISSING_SUMMARY,
        "INVALID_MISSING_SUMMARY_TEXT": INVALID_MISSING_SUMMARY_TEXT,
        "INVALID_MISSING_EXPERIENCE_FIELDS": INVALID_MISSING_EXPERIENCE_FIELDS,
        "INVALID_DATE_PRECISION": INVALID_DATE_PRECISION,
        "INVALID_EXTRA_TOP_LEVEL": INVALID_EXTRA_TOP_LEVEL,
        "INVALID_EXTRA_IN_EXPERIENCE": INVALID_EXTRA_IN_EXPERIENCE,
        "INVALID_WRONG_TYPE_YEARS": INVALID_WRONG_TYPE_YEARS,
        "INVALID_WRONG_TYPE_PROJECTS": INVALID_WRONG_TYPE_PROJECTS,
        "INVALID_TECH_KEYWORDS_TYPE": INVALID_TECH_KEYWORDS_TYPE,
        "INVALID_EMPTY_PROFILE": INVALID_EMPTY_PROFILE,
        "INVALID_NULL_PROFILE": INVALID_NULL_PROFILE,
        "INVALID_SKILL_STRUCTURE": INVALID_SKILL_STRUCTURE,
    }
    
    for name, fixture in fixtures.items():
        print(f"\n{'='*60}")
        print(f"Fixture: {name}")
        print(f"{'='*60}")
        print(json.dumps(fixture, indent=2))
