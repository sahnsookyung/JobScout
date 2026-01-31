EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
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
