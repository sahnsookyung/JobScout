EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        # --- LOGISTICS & META DATA ---
        "thought_process": {
            "type": "string",
            "description": "Brief analysis of the tech stack, seniority level, and key constraints."
        },
        "job_summary": {
            "type": "string",
            "description": "A 1-sentence summary of what the role actually does."
        },
        "seniority_level": {
            "type": "string", 
            "enum": ["Intern", "Junior", "Mid-Level", "Senior", "Staff/Principal", "Lead/Manager", "Unspecified"],
            "description": "Inferred seniority level based on years of experience and title."
        },
        "remote_policy": {
            "type": "string",
            "enum": ["On-site", "Hybrid", "Remote (Local)", "Remote (Global)", "Unspecified"],
            "description": "The remote work policy. 'Remote (Local)' means remote but must be in specific country."
        },
        "visa_sponsorship_available": {
            "type": "boolean",
            "description": "True if the posting explicitly mentions visa sponsorship or relocation assistance."
        },
        
        # --- HARD REQUIREMENTS ---
        "min_years_experience": {
            "type": "integer", 
            "description": "Minimum years of experience required. Null if not specified."
        },
        "requires_degree": {
            "type": "boolean",
            "description": "True ONLY if a degree is a hard requirement, not just preferred."
        },
        "security_clearance": {
            "type": "boolean",
            "description": "True if a security clearance is explicitly required."
        },
        
        # --- COMPENSATION ---
        "salary_min": {
            "type": "number",
            "description": "Yearly minimum salary if mentioned. Null if not specified."
        },
        "salary_max": {
            "type": "number",
            "description": "Yearly maximum salary if mentioned. Null if not specified."
        },
        "currency": {
            "type": "string",
            "enum": ["USD", "JPY", "EUR", "GBP", "CAD", "AUD", "CHF", "CNY", "HKD", "SGD", "NZD", "SEK", "NOK", "DKK", "MXN", "BRL", "INR", "KRW", "TWD", "Unspecified"],
            "description": "Currency code (e.g. USD, JPY, EUR). Null if not specified."
        },

        # --- TECH AGGREGATION (For fast filtering) ---
        "tech_stack": {
            "type": "array",
            "description": "List of all primary technologies, languages, and frameworks mentioned.",
            "items": {"type": "string"}
        },

        # --- DETAILED LINE ITEMS ---
        "requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "req_type": {
                        "type": "string",
                        "enum": ["must_have", "nice_to_have", "responsibility", "benefit"]
                    },
                    "category": {
                        "type": "string", 
                        "enum": ["technical", "soft_skill", "domain_knowledge", "logistical"],
                        "description": "Classifies the requirement to improve vector matching."
                    },
                    "text": {
                        "type": "string",
                        "description": "The exact requirement text, cleaned up."
                    },
                    "related_skills": {
                        "type": "array", 
                        "items": {"type": "string"},
                        "description": "Specific tools/skills mentioned in this single requirement."
                    },
                    "proficiency": {
                         "type": "string",
                         "enum": ["basic", "proficient", "expert", "unspecified"],
                         "description": "The level of competency required for this specific item."
                    }
                },
                "required": ["req_type", "category", "text"],
                "additionalProperties": False
            }
        }
    },
    "required": ["thought_process", "requirements", "tech_stack"],
    "additionalProperties": False
}


"""
Comprehensive Resume Extraction Schema (resume.v1)

Structured extraction of resume data with full type information.
Supports date-based experience calculation and cross-validation.

JSON Schema Draft 2020-12
"""

RESUME_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://jobscout.ai/schemas/resume.v1.json",
    "type": "object",
    "additionalProperties": False,
    "required": ["schema_version", "profile"],
    
    "$defs": {
        "YYYY_MM_Date": {
            "type": ["string", "null"],
            "description": "Date in YYYY-MM format",
            "pattern": "^[0-9]{4}-[0-9]{2}$"
        },
        "NullableString": {
            "type": ["string", "null"]
        },
        "Link": {
            "type": "object",
            "additionalProperties": False,
            "required": ["url"],
            "properties": {
                "platform": {"$ref": "#/$defs/NullableString"},
                "label": {"$ref": "#/$defs/NullableString"},
                "url": {"type": "string"}
            }
        },
        "StringArray": {
            "type": "array",
            "items": {"type": "string"}
        },
        "ImpactItem": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "metric": {"$ref": "#/$defs/NullableString"},
                "value": {"type": ["number", "null"]},
                "unit": {"$ref": "#/$defs/NullableString"},
                "description": {"$ref": "#/$defs/NullableString"}
            }
        }
    },
    
    "properties": {
        "schema_version": {
            "type": "string",
            "enum": ["resume.v1"],
            "description": "Schema version identifier."
        },
        
        "document": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "source": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "filename": {"$ref": "#/$defs/NullableString"},
                        "url": {"$ref": "#/$defs/NullableString"},
                        "content_type": {
                            "type": "string",
                            "enum": ["application/pdf", "text/html", "text/plain", "other"]
                        },
                        "language": {"$ref": "#/$defs/NullableString"}
                    }
                },
                "sections": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["type", "items"],
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["summary", "experience", "education", "skills", "projects", "certifications", "awards", "publications", "volunteering", "languages", "links", "other"]
                            },
                            "title": {"$ref": "#/$defs/NullableString"},
                            "order": {"type": "integer"},
                            "raw_text": {"$ref": "#/$defs/NullableString"},
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "company": {"$ref": "#/$defs/NullableString"},
                                        "role": {"$ref": "#/$defs/NullableString"},
                                        "period": {"$ref": "#/$defs/NullableString"},
                                        "description": {"$ref": "#/$defs/NullableString"},
                                        "highlights": {"$ref": "#/$defs/StringArray"},
                                        "institution": {"$ref": "#/$defs/NullableString"},
                                        "degree": {"$ref": "#/$defs/NullableString"},
                                        "field_of_study": {"$ref": "#/$defs/NullableString"},
                                        "skills": {"$ref": "#/$defs/StringArray"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        
        "profile": {
            "type": "object",
            "additionalProperties": False,
            "required": ["experience"],
            "properties": {
                "person": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "full_name": {"$ref": "#/$defs/NullableString"},
                        "headline": {"$ref": "#/$defs/NullableString"},
                        "location": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "city": {"$ref": "#/$defs/NullableString"},
                                "region": {"$ref": "#/$defs/NullableString"},
                                "country": {"$ref": "#/$defs/NullableString"}
                            }
                        },
                        "emails": {"$ref": "#/$defs/StringArray"},
                        "phones": {"$ref": "#/$defs/StringArray"},
                        "links": {
                            "type": "array",
                            "items": {"$ref": "#/$defs/Link"}
                        }
                    }
                },
                
                "summary": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "text": {"$ref": "#/$defs/NullableString"},
                        "claimed_total_experience_years": {"type": ["number", "null"]},
                        "domain_focus": {"$ref": "#/$defs/StringArray"}
                    }
                },
                
                "experience": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["company", "title"],
                        "properties": {
                            "company": {"type": "string"},
                            "team": {"$ref": "#/$defs/NullableString"},
                            "title": {"type": "string"},
                            "employment_type": {
                                "type": ["string", "null"],
                                "enum": ["full_time", "part_time", "contract", "internship", "other", None]
                            },
                            "location": {"$ref": "#/$defs/NullableString"},
                            "start_date": {"$ref": "#/$defs/YYYY_MM_Date"},
                            "end_date": {"$ref": "#/$defs/YYYY_MM_Date"},
                            "is_current": {"type": ["boolean", "null"]},
                            "description": {"$ref": "#/$defs/NullableString"},
                            "highlights": {"$ref": "#/$defs/StringArray"},
                            "tech": {"$ref": "#/$defs/StringArray"},
                            "impact": {
                                "type": "array",
                                "items": {"$ref": "#/$defs/ImpactItem"}
                            }
                        }
                    }
                },
                
                "projects": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "name": {"$ref": "#/$defs/NullableString"},
                            "description": {"$ref": "#/$defs/NullableString"},
                            "highlights": {"$ref": "#/$defs/StringArray"},
                            "links": {
                                "type": "array",
                                "items": {"$ref": "#/$defs/Link"}
                            },
                            "tech": {"$ref": "#/$defs/StringArray"},
                            "start_date": {"$ref": "#/$defs/YYYY_MM_Date"},
                            "end_date": {"$ref": "#/$defs/YYYY_MM_Date"}
                        }
                    }
                },
                
                "education": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["institution"],
                        "properties": {
                            "institution": {"type": "string"},
                            "degree": {"$ref": "#/$defs/NullableString"},
                            "field_of_study": {"$ref": "#/$defs/NullableString"},
                            "start_year": {"type": ["integer", "null"]},
                            "end_year": {"type": ["integer", "null"]},
                            "highlights": {"$ref": "#/$defs/StringArray"}
                        }
                    }
                },
                
                "skills": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "groups": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["name", "skills"],
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "enum": ["languages", "backend", "frontend", "cloud", "devops", "data", "ml", "other"]
                                    },
                                    "skills": {"$ref": "#/$defs/StringArray"}
                                }
                            }
                        },
                        "all": {"$ref": "#/$defs/StringArray"}
                    }
                }
            }
        },
        
        "extraction": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "confidence": {"type": ["number", "null"]},
                "warnings": {"$ref": "#/$defs/StringArray"},
                "provenance": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "field": {"type": "string"},
                            "source_section_index": {"type": ["integer", "null"]},
                            "source_text": {"$ref": "#/$defs/NullableString"}
                        }
                    }
                }
            }
        }
    }
}
