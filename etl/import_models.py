from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _fallback_external_id(payload: dict[str, Any]) -> str:
    for key in ("source_job_id", "job_id", "id", "job_url", "job_url_direct"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    title = str(payload.get("title") or "").strip() or "unknown-title"
    company = str(payload.get("company_name") or "").strip() or "unknown-company"
    location = str(payload.get("location") or "").strip() or "unknown-location"
    return f"{company}|{title}|{location}"


@dataclass(slots=True)
class ImportSourceDescriptor:
    provider: str
    site_name: str
    source_key: str
    external_job_id: str
    source_url: str | None = None
    apply_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def stable_job_url(self) -> str:
        if self.source_url:
            return self.source_url
        return f"external://{self.provider}/{self.source_key}/{self.external_job_id}"


@dataclass(slots=True)
class NormalizedJobRecord:
    title: str
    company_name: str
    location: Any
    description: str | None
    source: ImportSourceDescriptor
    tenant_id: Any | None = None
    is_remote: bool | None = None
    skills: list[Any] = field(default_factory=list)
    company_url: str | None = None
    posted_at: str | None = None
    employment_type: str | None = None
    compensation: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_scraper_payload(
        cls,
        job_data: dict[str, Any],
        site_name: str,
        *,
        tenant_id: Any | None = None,
    ) -> "NormalizedJobRecord":
        payload = dict(job_data)
        source = ImportSourceDescriptor(
            provider="jobspy",
            site_name=site_name,
            source_key=site_name,
            external_job_id=_fallback_external_id(payload),
            source_url=payload.get("job_url"),
            apply_url=payload.get("job_url_direct") or payload.get("job_url"),
            metadata={"ingest_mode": "scrape"},
        )
        return cls(
            title=str(payload.get("title") or ""),
            company_name=str(payload.get("company_name") or ""),
            location=payload.get("location"),
            description=payload.get("description"),
            source=source,
            tenant_id=tenant_id,
            is_remote=payload.get("is_remote"),
            skills=list(payload.get("skills") or []),
            company_url=payload.get("company_url"),
            posted_at=payload.get("date_posted"),
            employment_type=payload.get("employment_type"),
            compensation=dict(payload.get("compensation") or {}),
            metadata={},
            raw_payload=payload,
        )

    def as_job_data(self) -> dict[str, Any]:
        payload = dict(self.raw_payload)
        payload.update(
            {
                "title": self.title,
                "company_name": self.company_name,
                "location": self.location,
                "description": self.description,
                "is_remote": self.is_remote,
                "skills": list(self.skills),
                "company_url": self.company_url,
                "date_posted": self.posted_at,
                "employment_type": self.employment_type,
                "compensation": dict(self.compensation),
                "job_url": self.source.stable_job_url(),
                "job_url_direct": self.source.apply_url or self.source.source_url,
                "source_job_id": self.source.external_job_id,
                "source_provider": self.source.provider,
                "source_key": self.source.source_key,
                "source_metadata": {
                    **self.source.metadata,
                    **self.metadata,
                    "site_name": self.source.site_name,
                },
            }
        )
        return payload
