"""Deterministic evidence-grounded resume variant generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.resume_evidence_selection import (
    build_job_relevance_terms,
    select_relevant_resume_evidence_units,
)

MAX_CONTENT_JSON_BYTES = 128 * 1024
GENERATOR_VERSION = "hybrid-v3"
EVIDENCE_POLICY_VERSION = "evidence-v3"
TEMPLATE_VERSION = "compact-v2"
RENDERER_VERSION = "renderer-v2"


@dataclass(frozen=True)
class SourcePointer:
    """Machine-verifiable pointer to source evidence used for a claim."""

    kind: str
    path: str | None = None
    index: int | None = None
    evidence_unit_id: str | None = None
    job_match_requirement_id: str | None = None
    job_requirement_unit_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "kind": self.kind,
                "path": self.path,
                "index": self.index,
                "evidence_unit_id": self.evidence_unit_id,
                "job_match_requirement_id": self.job_match_requirement_id,
                "job_requirement_unit_id": self.job_requirement_unit_id,
            }.items()
            if value is not None
        }


def _clean_text(value: Any, *, max_length: int = 600) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.replace("\x00", "").strip()
    if not cleaned:
        return None
    return cleaned[:max_length]


def _clean_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _source(kind: str, path: str, index: int | None = None) -> dict[str, Any]:
    return SourcePointer(kind=kind, path=path, index=index).as_dict()


def _claim(text: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    return {"text": text, "sources": sources}


def _profile(resume_data: dict[str, Any]) -> dict[str, Any]:
    profile = resume_data.get("profile")
    return profile if isinstance(profile, dict) else {}


def _summary_claims(profile: dict[str, Any]) -> list[dict[str, Any]]:
    summary = profile.get("summary")
    if not isinstance(summary, dict):
        return []
    text = _clean_text(summary.get("text"), max_length=500)
    if not text:
        return []
    return [_claim(text, [_source("structured_resume", "profile.summary.text")])]

def _contact_details(profile: dict[str, Any]) -> dict[str, Any]:
    contact = profile.get("contact")
    source = contact if isinstance(contact, dict) else profile
    links = source.get("links")
    cleaned_links = []
    if isinstance(links, list):
        cleaned_links = [
            cleaned
            for value in links[:8]
            if (cleaned := _clean_text(value, max_length=240))
        ]
    for key in ("linkedin_url", "portfolio_url"):
        value = _clean_text(source.get(key), max_length=240)
        if value and value not in cleaned_links:
            cleaned_links.append(value)
    return {
        "name": _clean_text(source.get("name"), max_length=160),
        "email": _clean_text(source.get("email"), max_length=240),
        "phone": _clean_text(source.get("phone"), max_length=80),
        "location": _clean_text(source.get("location"), max_length=160),
        "links": cleaned_links,
    }

def _date_text(value: Any) -> str | None:
    if not isinstance(value, dict):
        return _clean_text(value, max_length=80)
    original = _clean_text(value.get("text"), max_length=80)
    if original:
        return original
    year = value.get("year")
    month = value.get("month")
    if isinstance(year, int) and isinstance(month, int) and 1 <= month <= 12:
        return f"{year:04d}-{month:02d}"
    if isinstance(year, int):
        return str(year)
    return None


def _skill_claims(
    profile: dict[str, Any],
    *,
    relevance_terms: set[str] | None = None,
    limit: int = 24,
) -> list[dict[str, Any]]:
    skills = profile.get("skills")
    if not isinstance(skills, dict):
        return []
    items = skills.get("all")
    if not isinstance(items, list):
        return []

    ordered_items = [
        (index, item)
        for index, item in enumerate(items)
        if isinstance(item, dict)
    ]
    if relevance_terms:
        ordered_items.sort(
            key=lambda pair: (
                -_term_overlap(str(pair[1].get("name", "")), relevance_terms),
                pair[0],
            )
        )

    claims: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in ordered_items:
        name = _clean_text(item.get("name"), max_length=80)
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        claims.append(_claim(name, [_source("structured_resume", f"profile.skills.all[{index}].name", index)]))
        if len(claims) >= limit:
            break
    return claims


def _experience_claims(
    profile: dict[str, Any],
    *,
    relevance_terms: set[str] | None = None,
    limit: int = 12,
) -> list[dict[str, Any]]:
    experience = profile.get("experience")
    if not isinstance(experience, list):
        return []

    entries: list[dict[str, Any]] = []
    for index, item in enumerate(experience[:limit]):
        if not isinstance(item, dict):
            continue
        role = _clean_text(item.get("title"), max_length=120)
        company = _clean_text(item.get("company"), max_length=120)
        highlights = item.get("highlights")
        bullet_candidates: list[tuple[int, str, dict[str, Any]]] = []
        if isinstance(highlights, list):
            for bullet_index, highlight in enumerate(highlights[:8]):
                text = _clean_text(highlight, max_length=260)
                if text:
                    bullet_candidates.append(
                        (
                            bullet_index,
                            text,
                            _claim(
                                text,
                                [
                                    _source(
                                        "structured_resume",
                                        f"profile.experience[{index}].highlights[{bullet_index}]",
                                        bullet_index,
                                    )
                                ],
                            ),
                        )
                    )
        description = _clean_text(item.get("description"), max_length=260)
        if description and all(description.lower() != candidate[1].lower() for candidate in bullet_candidates):
            bullet_candidates.append(
                (
                    len(highlights) if isinstance(highlights, list) else 0,
                    description,
                    _claim(
                        description,
                        [_source("structured_resume", f"profile.experience[{index}].description", index)],
                    ),
                )
            )
        if relevance_terms:
            bullet_candidates.sort(
                key=lambda candidate: (-_term_overlap(candidate[1], relevance_terms), candidate[0])
            )
        bullets = [candidate[2] for candidate in bullet_candidates[:6]]
        if role or company or bullets:
            entries.append(
                {
                    "entry_id": f"experience-{index}",
                    "title": role,
                    "company": company,
                    "start_date": _date_text(item.get("start_date")),
                    "end_date": "Present" if item.get("is_current") else _date_text(item.get("end_date")),
                    "sources": [_source("structured_resume", f"profile.experience[{index}]", index)],
                    "bullets": bullets,
                }
            )
    return entries

def _project_entries(
    profile: dict[str, Any],
    *,
    relevance_terms: set[str] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    projects = profile.get("projects")
    items = projects.get("items") if isinstance(projects, dict) else None
    if not isinstance(items, list):
        return []
    entries: list[dict[str, Any]] = []
    for index, item in enumerate(items[:limit]):
        if not isinstance(item, dict):
            continue
        bullet_candidates: list[tuple[int, str, dict[str, Any]]] = []
        highlights = item.get("highlights")
        if isinstance(highlights, list):
            for bullet_index, highlight in enumerate(highlights[:6]):
                text = _clean_text(highlight, max_length=260)
                if text:
                    bullet_candidates.append(
                        (
                            bullet_index,
                            text,
                            _claim(
                                text,
                                [_source("structured_resume", f"profile.projects.items[{index}].highlights[{bullet_index}]", bullet_index)],
                            ),
                        )
                    )
        description = _clean_text(item.get("description"), max_length=260)
        if description and all(description.lower() != candidate[1].lower() for candidate in bullet_candidates):
            bullet_candidates.append(
                (
                    len(bullet_candidates),
                    description,
                    _claim(
                        description,
                        [_source("structured_resume", f"profile.projects.items[{index}].description", index)],
                    ),
                )
            )
        if relevance_terms:
            bullet_candidates.sort(
                key=lambda candidate: (-_term_overlap(candidate[1], relevance_terms), candidate[0])
            )
        technologies = item.get("technologies")
        cleaned_technologies = []
        if isinstance(technologies, list):
            cleaned_technologies = [
                cleaned
                for value in technologies[:16]
                if (cleaned := _clean_text(value, max_length=80))
            ]
        name = _clean_text(item.get("name"), max_length=160)
        if name or bullet_candidates:
            entries.append(
                {
                    "entry_id": f"project-{index}",
                    "name": name,
                    "date": _date_text(item.get("date")),
                    "url": _clean_text(item.get("url"), max_length=240),
                    "technologies": cleaned_technologies,
                    "sources": [_source("structured_resume", f"profile.projects.items[{index}]", index)],
                    "bullets": [candidate[2] for candidate in bullet_candidates[:5]],
                }
            )
    return entries

def _education_entries(profile: dict[str, Any], *, limit: int = 8) -> list[dict[str, Any]]:
    education = profile.get("education")
    if not isinstance(education, list):
        return []
    entries = []
    for index, item in enumerate(education[:limit]):
        if not isinstance(item, dict):
            continue
        description = _clean_text(item.get("description"), max_length=260)
        highlights = item.get("highlights")
        details = []
        if description:
            details.append(
                _claim(description, [_source("structured_resume", f"profile.education[{index}].description", index)])
            )
        if isinstance(highlights, list):
            for highlight_index, highlight in enumerate(highlights[:4]):
                text = _clean_text(highlight, max_length=220)
                if text:
                    details.append(
                        _claim(
                            text,
                            [_source("structured_resume", f"profile.education[{index}].highlights[{highlight_index}]", highlight_index)],
                        )
                    )
        institution = _clean_text(item.get("institution"), max_length=160)
        degree = _clean_text(item.get("degree"), max_length=160)
        if institution or degree or details:
            entries.append(
                {
                    "institution": institution,
                    "degree": degree,
                    "field_of_study": _clean_text(item.get("field_of_study"), max_length=160),
                    "graduation_year": item.get("graduation_year") if isinstance(item.get("graduation_year"), int) else None,
                    "sources": [_source("structured_resume", f"profile.education[{index}]", index)],
                    "details": details,
                }
            )
    return entries

def _certification_entries(profile: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    certifications = profile.get("certifications")
    if not isinstance(certifications, list):
        return []
    return [
        {
            "name": _clean_text(item.get("name"), max_length=160),
            "issuer": _clean_text(item.get("issuer"), max_length=160),
            "issued_year": item.get("issued_year") if isinstance(item.get("issued_year"), int) else None,
            "expires_year": item.get("expires_year") if isinstance(item.get("expires_year"), int) else None,
            "sources": [_source("structured_resume", f"profile.certifications[{index}]", index)],
        }
        for index, item in enumerate(certifications[:limit])
        if isinstance(item, dict) and _clean_text(item.get("name"), max_length=160)
    ]

def _language_entries(profile: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    languages = profile.get("languages")
    if not isinstance(languages, list):
        return []
    return [
        {
            "language": _clean_text(item.get("language"), max_length=100),
            "proficiency": _clean_text(item.get("proficiency"), max_length=100),
            "sources": [_source("structured_resume", f"profile.languages[{index}]", index)],
        }
        for index, item in enumerate(languages[:limit])
        if isinstance(item, dict) and _clean_text(item.get("language"), max_length=100)
    ]


def _evidence_unit_claims(
    evidence_units: list[Any],
    requirements: list[Any],
    *,
    job: Any,
    limit: int = 8,
) -> list[dict[str, Any]]:
    selected = select_relevant_resume_evidence_units(
        evidence_units,
        requirements,
        max_count=limit,
        job_texts=(getattr(job, "title", None), getattr(job, "description", None)),
    )
    claims: list[dict[str, Any]] = []
    seen: set[str] = set()
    for unit in selected[:limit]:
        text = _clean_text(getattr(unit, "source_text", None), max_length=280)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        claims.append(
            _claim(
                text,
                [
                    SourcePointer(
                        kind="resume_evidence_unit",
                        evidence_unit_id=str(getattr(unit, "evidence_unit_id", "")),
                    ).as_dict()
                ],
            )
        )
    return claims


def _requirement_claims(
    requirement_matches: list[Any],
    *,
    claim_limit: int = 10,
    gap_limit: int = 12,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    claims: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    warnings: list[str] = []
    for match in requirement_matches:
        requirement = getattr(match, "requirement", None)
        requirement_text = _clean_text(getattr(requirement, "text", None), max_length=180)
        evidence = _clean_text(getattr(match, "evidence_text", None), max_length=260)
        if getattr(match, "is_covered", False) and evidence and len(claims) < claim_limit:
            claims.append(
                _claim(
                    evidence,
                    [
                        SourcePointer(
                            kind="job_match_requirement",
                            job_match_requirement_id=str(getattr(match, "id")),
                            job_requirement_unit_id=str(getattr(match, "job_requirement_unit_id")),
                        ).as_dict()
                    ],
                )
            )
        elif requirement_text:
            if len(gaps) < gap_limit:
                gaps.append(
                    _claim(
                        requirement_text,
                        [
                            SourcePointer(
                                kind="job_requirement",
                                job_requirement_unit_id=str(getattr(match, "job_requirement_unit_id")),
                            ).as_dict()
                        ],
                    )
                )
            warnings.append(f"Unsupported requirement not claimed: {requirement_text}")
    return claims, gaps, warnings


def _requirement_units(requirement_matches: list[Any]) -> list[Any]:
    units: list[Any] = []
    for match in requirement_matches:
        requirement = getattr(match, "requirement", None)
        if requirement is not None:
            units.append(requirement)
    return units


def _term_overlap(text: str, terms: set[str]) -> int:
    lowered = text.lower()
    return sum(1 for term in terms if term in lowered)


def _merge_claims(*claim_groups: list[dict[str, Any]], limit: int = 12) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for claims in claim_groups:
        for claim in claims:
            text = _clean_text(claim.get("text"), max_length=280)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(claim)
            if len(merged) >= limit:
                return merged
    return merged


def _collect_claims(content: Any) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    if isinstance(content, dict):
        if "text" in content:
            claims.append(content)
        for value in content.values():
            claims.extend(_collect_claims(value))
    elif isinstance(content, list):
        for item in content:
            claims.extend(_collect_claims(item))
    return claims


def validate_claim_sources(content: dict[str, Any]) -> list[str]:
    """Return validation warnings for unsupported generated claims."""
    warnings = []
    for claim in _collect_claims(content):
        sources = claim.get("sources")
        if not isinstance(sources, list) or not sources:
            warnings.append(f"Generated claim lacks source pointers: {claim.get('text', '')[:80]}")
    return warnings

def resume_body_claims(content: dict[str, Any]) -> list[dict[str, Any]]:
    """Return claims that are rendered as candidate resume content."""
    claims: list[dict[str, Any]] = []
    claims.extend(_claims_for_key(content, "summary"))
    claims.extend(_claims_for_key(content, "skills"))
    for section, detail_key in (("experience", "bullets"), ("projects", "bullets"), ("education", "details")):
        entries = content.get(section)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                claims.extend(_claims_for_key(entry, detail_key))
    return claims

def _claims_for_key(content: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = content.get(key)
    if not isinstance(value, list):
        return []
    return [claim for claim in value if isinstance(claim, dict) and _clean_text(claim.get("text"))]

def validate_resume_content_quality(content: dict[str, Any]) -> list[str]:
    """Return actionable quality errors for drafts too sparse to be resumes."""
    claims = resume_body_claims(content)
    word_count = sum(len(str(claim.get("text", "")).split()) for claim in claims)
    substantive_sections = sum(
        bool(content.get(section))
        for section in ("experience", "projects", "education")
    )
    errors = []
    if substantive_sections == 0:
        errors.append("No experience, project, or education entries were extracted.")
    if len(claims) < 4 or word_count < 35:
        errors.append("The extracted resume does not contain enough detail to create a usable draft.")
    return errors

def build_evidence_map(content: dict[str, Any]) -> dict[str, Any]:
    claims = _collect_claims(content)
    return {
        "policy_version": EVIDENCE_POLICY_VERSION,
        "claim_count": len(claims),
        "source_types": sorted(
            {
                source.get("kind")
                for claim in claims
                for source in claim.get("sources", [])
                if isinstance(source, dict) and source.get("kind")
            }
        ),
    }


def generate_resume_variant_content(
    *,
    resume_data: dict[str, Any],
    job: Any,
    match: Any,
    requirement_matches: list[Any],
    template_key: str,
    tone: str,
    resume_evidence_units: list[Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Generate a deterministic, evidence-grounded resume variant."""
    profile = _profile(resume_data)
    requirements = _requirement_units(requirement_matches)
    relevance_terms = build_job_relevance_terms(
        requirements,
        job_texts=(getattr(job, "title", None), getattr(job, "description", None)),
    )
    evidence_unit_claims = _evidence_unit_claims(
        resume_evidence_units or [],
        requirements,
        job=job,
    )
    requirement_claims, gap_claims, warnings = _requirement_claims(requirement_matches)
    if getattr(match, "is_hidden", False):
        warnings.append("This match is hidden; generated draft is still available for review.")

    content = {
        "template_key": template_key,
        "tone": tone,
        "contact": _contact_details(profile),
        "job": {
            "title": _clean_text(getattr(job, "title", None), max_length=160),
            "company": _clean_text(getattr(job, "company", None), max_length=160),
        },
        "summary": _summary_claims(profile),
        "targeted_evidence": _merge_claims(evidence_unit_claims, requirement_claims),
        "skills": _skill_claims(profile, relevance_terms=relevance_terms),
        "experience": _experience_claims(profile, relevance_terms=relevance_terms),
        "projects": _project_entries(profile, relevance_terms=relevance_terms),
        "education": _education_entries(profile),
        "certifications": _certification_entries(profile),
        "languages": _language_entries(profile),
        "gaps": gap_claims,
        "source_quality": {
            "job_description_completeness": _clean_text(getattr(job, "description_completeness", None), max_length=60),
            "job_description_source": _clean_text(getattr(job, "description_source", None), max_length=80),
            "job_description_warning_code": _clean_text(getattr(job, "description_warning_code", None), max_length=80),
            "fit_score": _clean_number(getattr(match, "fit_score", None)),
            "required_coverage": _clean_number(getattr(match, "required_coverage", None)),
        },
    }
    warnings.extend(validate_claim_sources(content))

    evidence_map = build_evidence_map(content)
    return content, evidence_map, warnings
