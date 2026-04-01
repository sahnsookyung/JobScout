from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

SUMMARY_CONTRACT_VERSION = 1


@dataclass(frozen=True)
class CanonicalJobSummary:
    text: str
    version: int
    content_hash: str


class CanonicalJobSummaryGenerator:
    """Build a stable embedding-oriented job summary from extracted metadata."""

    def __init__(self, contract_version: int = SUMMARY_CONTRACT_VERSION):
        self.contract_version = contract_version

    def generate(self, job: Any, metadata: Dict[str, Any]) -> CanonicalJobSummary:
        sections = [
            self._format_section("Role", self._build_role_line(job, metadata)),
            self._format_section(
                "Responsibilities",
                self._join_items(self._limit(self._responsibility_items(metadata), 4)),
            ),
            self._format_section(
                "Required",
                self._join_items(self._limit(self._requirements(metadata, {"must_have", "required"}), 6)),
            ),
            self._format_section(
                "Preferred",
                self._join_items(self._limit(self._requirements(metadata, {"nice_to_have", "preferred"}), 4)),
            ),
            self._format_section(
                "Work Arrangement",
                self._build_work_arrangement_line(job, metadata),
            ),
            self._format_section(
                "Compensation and Visa",
                self._build_compensation_and_visa_line(job, metadata),
            ),
            self._format_section(
                "Company and Team",
                self._join_items(self._limit(self._company_and_team_cues(job, metadata), 4)),
            ),
        ]

        text = "\n".join(section for section in sections if section)
        digest = hashlib.sha256(
            f"{self.contract_version}:{text}".encode("utf-8")
        ).hexdigest()[:32]
        return CanonicalJobSummary(
            text=text,
            version=self.contract_version,
            content_hash=digest,
        )

    @staticmethod
    def _format_section(label: str, value: str) -> str:
        if not value:
            return ""
        return f"{label}: {value}"

    @staticmethod
    def _join_items(items: Iterable[str]) -> str:
        normalized = [
            item.strip()
            for item in items
            if isinstance(item, str) and item.strip()
        ]
        return "; ".join(normalized)

    @staticmethod
    def _limit(items: Iterable[str], limit: int) -> List[str]:
        normalized = []
        seen: set[str] = set()
        for item in items:
            cleaned = item.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(cleaned)
            if len(normalized) >= limit:
                break
        return normalized

    @staticmethod
    def _build_role_line(job: Any, metadata: Dict[str, Any]) -> str:
        title = getattr(job, "title", None) or "Unknown role"
        company = getattr(job, "company", None) or "Unknown company"
        seniority = metadata.get("seniority_level") or getattr(job, "job_level", None)
        summary = metadata.get("job_summary")

        parts = [f"{title} at {company}"]
        if seniority:
            parts.append(f"seniority {seniority}")
        if summary:
            parts.append(summary.strip())
        return "; ".join(part for part in parts if part)

    @staticmethod
    def _responsibility_items(metadata: Dict[str, Any]) -> List[str]:
        return [
            item.get("text", "")
            for item in metadata.get("requirements", [])
            if item.get("req_type") == "responsibility"
        ]

    @staticmethod
    def _requirements(metadata: Dict[str, Any], allowed_types: set[str]) -> List[str]:
        return [
            item.get("text", "")
            for item in metadata.get("requirements", [])
            if item.get("req_type") in allowed_types
        ]

    @staticmethod
    def _build_work_arrangement_line(job: Any, metadata: Dict[str, Any]) -> str:
        parts: List[str] = []
        remote_policy = metadata.get("remote_policy")
        if remote_policy:
            parts.append(remote_policy)
        location = getattr(job, "location_text", None)
        if location:
            parts.append(f"location {location}")
        work_from_home_type = getattr(job, "work_from_home_type", None)
        if work_from_home_type:
            parts.append(f"work from home {work_from_home_type}")
        if getattr(job, "is_remote", None) is True and "remote" not in " ".join(parts).lower():
            parts.append("remote possible")
        return "; ".join(parts)

    @staticmethod
    def _build_compensation_and_visa_line(job: Any, metadata: Dict[str, Any]) -> str:
        parts: List[str] = []
        salary_min = CanonicalJobSummaryGenerator._coerce_numeric(
            metadata.get("salary_min", getattr(job, "salary_min", None))
        )
        salary_max = CanonicalJobSummaryGenerator._coerce_numeric(
            metadata.get("salary_max", getattr(job, "salary_max", None))
        )
        currency = CanonicalJobSummaryGenerator._coerce_text(
            metadata.get("currency", getattr(job, "currency", None))
        )
        if salary_min is not None or salary_max is not None:
            lower = str(int(salary_min)) if salary_min is not None else "?"
            upper = str(int(salary_max)) if salary_max is not None else "?"
            prefix = f"{currency} " if currency else ""
            parts.append(f"salary {prefix}{lower}-{upper}")

        visa = metadata.get("visa_sponsorship_available")
        if visa is True:
            parts.append("visa sponsorship available")
        elif visa is False:
            parts.append("visa sponsorship not indicated")
        return "; ".join(parts)

    @staticmethod
    def _coerce_numeric(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return None

    @staticmethod
    def _coerce_text(value: Any) -> str:
        return value if isinstance(value, str) else ""

    @staticmethod
    def _company_and_team_cues(job: Any, metadata: Dict[str, Any]) -> List[str]:
        cues: List[str] = []
        company_description = CanonicalJobSummaryGenerator._coerce_text(
            getattr(job, "company_description", None)
        )
        if company_description:
            cues.append(company_description)

        benefits = metadata.get("benefits", [])
        cues.extend(item.get("text", "") for item in benefits)

        tech_stack = metadata.get("tech_stack", [])
        if tech_stack:
            cues.append(f"tech stack {', '.join(str(item) for item in tech_stack[:8])}")
        return cues
