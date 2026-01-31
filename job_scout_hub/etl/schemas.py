EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "min_years_experience": {
            "type": "integer", 
            "description": "Minimum years of experience required. Return null if not specified."
        },
        "requires_degree": {
            "type": "boolean",
            "description": "True if a specific degree (BS, MS, PhD) is explicitly required."
        },
        "security_clearance": {
            "type": "boolean",
            "description": "True if a security clearance is explicitly required."
        },
        "requirements": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "req_type": {
                        "type": "string",
                        "enum": ["required", "preferred", "responsibility", "benefit", "other"]
                    },
                    "text": {"type": "string"},
                    "skills": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["req_type", "text"],
                "additionalProperties": False
            }
        }
    },
    "required": ["requirements"],
    "additionalProperties": False
}
