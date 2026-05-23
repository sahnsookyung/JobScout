"""Deterministic evidence-grounded resume variant generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MAX_CONTENT_JSON_BYTES = 128 * 1024
GENERATOR_VERSION = "deterministic-v1"
EVIDENCE_POLICY_VERSION = "evidence-v1"
TEMPLATE_VERSION = "compact-v1"
RENDERER_VERSION = "renderer-v1"


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


def _skill_claims(profile: dict[str, Any], *, limit: int = 18) -> list[dict[str, Any]]:
    skills = profile.get("skills")
    if not isinstance(skills, dict):
        return []
    items = skills.get("all")
    if not isinstance(items, list):
        return []

    claims: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
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


def _experience_claims(profile: dict[str, Any], *, limit: int = 6) -> list[dict[str, Any]]:
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
        bullets = []
        if isinstance(highlights, list):
            for bullet_index, highlight in enumerate(highlights[:4]):
                text = _clean_text(highlight, max_length=260)
                if text:
                    bullets.append(
                        _claim(
                            text,
                            [
                                _source(
                                    "structured_resume",
                                    f"profile.experience[{index}].highlights[{bullet_index}]",
                                    bullet_index,
                                )
                            ],
                        )
                    )
        description = _clean_text(item.get("description"), max_length=260)
        if description and not bullets:
            bullets.append(
                _claim(
                    description,
                    [_source("structured_resume", f"profile.experience[{index}].description", index)],
                )
            )
        if role or company or bullets:
            entries.append(
                {
                    "title": role,
                    "company": company,
                    "sources": [_source("structured_resume", f"profile.experience[{index}]", index)],
                    "bullets": bullets,
                }
            )
    return entries


def _requirement_claims(requirement_matches: list[Any], *, limit: int = 10) -> tuple[list[dict[str, Any]], list[str]]:
    claims: list[dict[str, Any]] = []
    warnings: list[str] = []
    for match in requirement_matches:
        requirement = getattr(match, "requirement", None)
        requirement_text = _clean_text(getattr(requirement, "text", None), max_length=180)
        evidence = _clean_text(getattr(match, "evidence_text", None), max_length=260)
        if getattr(match, "is_covered", False) and evidence and len(claims) < limit:
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
            warnings.append(f"Unsupported requirement not claimed: {requirement_text}")
    return claims, warnings


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


def generate_resume_variant_content(
    *,
    resume_data: dict[str, Any],
    job: Any,
    match: Any,
    requirement_matches: list[Any],
    template_key: str,
    tone: str,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Generate a deterministic, evidence-grounded resume variant."""
    profile = _profile(resume_data)
    requirement_claims, warnings = _requirement_claims(requirement_matches)
    if getattr(match, "is_hidden", False):
        warnings.append("This match is hidden; generated draft is still available for review.")

    content = {
        "template_key": template_key,
        "tone": tone,
        "job": {
            "title": _clean_text(getattr(job, "title", None), max_length=160),
            "company": _clean_text(getattr(job, "company", None), max_length=160),
        },
        "summary": _summary_claims(profile),
        "targeted_evidence": requirement_claims,
        "skills": _skill_claims(profile),
        "experience": _experience_claims(profile),
    }
    warnings.extend(validate_claim_sources(content))

    evidence_map = {
        "policy_version": EVIDENCE_POLICY_VERSION,
        "claim_count": len(_collect_claims(content)),
        "source_types": sorted(
            {
                source.get("kind")
                for claim in _collect_claims(content)
                for source in claim.get("sources", [])
                if isinstance(source, dict) and source.get("kind")
            }
        ),
    }
    return content, evidence_map, warnings
