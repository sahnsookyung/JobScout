from __future__ import annotations

from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

import pytest

from core.resume_variants.generator import generate_resume_variant_content
from core.resume_variants.hashing import canonical_json_bytes


@pytest.mark.performance
def test_large_resume_and_requirement_set_stays_bounded() -> None:
    resume_data = {
        "profile": {
            "summary": {"text": "Senior backend engineer with distributed systems experience."},
            "skills": {"all": [{"name": f"Skill {index}"} for index in range(80)]},
            "experience": [
                {
                    "title": f"Engineer {index}",
                    "company": f"Company {index}",
                    "highlights": [f"Delivered service {index}-{bullet} with measurable impact." for bullet in range(6)],
                }
                for index in range(20)
            ],
        }
    }
    requirement_matches = [
        SimpleNamespace(
            id=uuid4(),
            job_requirement_unit_id=uuid4(),
            evidence_text=f"Delivered service {index} with measurable impact.",
            is_covered=index % 2 == 0,
            requirement=SimpleNamespace(text=f"Requirement {index}"),
        )
        for index in range(100)
    ]

    started = perf_counter()
    content, _evidence_map, warnings = generate_resume_variant_content(
        resume_data=resume_data,
        job=SimpleNamespace(title="Staff Backend Engineer", company="Acme"),
        match=SimpleNamespace(is_hidden=False),
        requirement_matches=requirement_matches,
        template_key="compact",
        tone="concise",
    )
    elapsed = perf_counter() - started

    assert elapsed < 1.0
    assert len(canonical_json_bytes(content)) < 128 * 1024
    assert len(content["targeted_evidence"]) == 10
    assert warnings
