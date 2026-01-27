# Atomic job_requirement_units (what + JSON schema + prompt)
Atomic units are the smallest chunks you want to retrieve + cite as evidence, so “Requirements” is a section, while each bullet/subpoint under it is usually one atomic unit. Chunking into smaller coherent pieces tends to improve retrieval relevance versus embedding whole sections. 

## Stored JSON shape (normalized extraction output)
You can store this JSON in job_post_content.raw_payload or a separate job_post_normalized field, and also materialize each entry into job_requirement_unit.

{
  "schema_version": "1.0",
  "job_requirement_units": [
    {
      "unit_id": "string",
      "section": "requirements",
      "req_type": "required",
      "category": "hard_skill",
      "text": "Must have 5+ years of Ruby (2+ years hands-on).",
      "must_have": true,
      "confidence": 0.86,
      "skills": ["ruby"],
      "tools": [],
      "domains": [],
      "years_experience_min": 5,
      "seniority_signals": ["senior"],
      "location_constraints": {
        "remote_ok": true,
        "location_text": "Tokyo, Japan"
      },
      "evidence_notes": "Extracted from Requirements section bullet."
    }
  ]
}

## Extraction prompt template (JD → atomic units)
If your LLM supports schema/structured outputs, enforce JSON structure to avoid parsing failures.

```
Extract atomic requirement units from this job description.

Rules:
1) Output MUST be valid JSON only.
2) Each unit must represent one checkable requirement/responsibility/constraint/benefit.
3) Split compound bullets into multiple units when they contain multiple distinct requirements.
4) Classify req_type as: required | preferred | responsibility | constraint | benefit.
5) Populate skills/tools/domains with short normalized strings.
6) If years of experience are stated, set years_experience_min.
7) Set must_have=true for required + constraint; otherwise false.
8) confidence is a float 0..1.

Job description:
<<<
{JD_TEXT}
>>>
```