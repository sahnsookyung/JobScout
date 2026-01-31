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
