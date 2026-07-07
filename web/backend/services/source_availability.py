"""Source availability helpers shared by job and match responses."""

from typing import Any

COMPLIANT_REFRESH_SOURCE_SITES = {"greenhouse", "lever", "ashby"}
PROHIBITED_SCRAPER_SOURCE_SITES = {"tokyodev", "japandev", "jobspy", "workday"}


def source_site_tokens(source: Any) -> set[str]:
    if source is None:
        return set()
    values = [
        getattr(source, "site", None),
        getattr(source, "job_url", None),
        getattr(source, "job_url_direct", None),
    ]
    haystack = " ".join(str(value).lower() for value in values if value)
    tokens: set[str] = set()
    for site in COMPLIANT_REFRESH_SOURCE_SITES | PROHIBITED_SCRAPER_SOURCE_SITES:
        if site in haystack:
            tokens.add(site)
    if "greenhouse.io" in haystack or "boards.greenhouse" in haystack:
        tokens.add("greenhouse")
    if "lever.co" in haystack or "jobs.lever" in haystack:
        tokens.add("lever")
    if "ashbyhq.com" in haystack or "jobs.ashby" in haystack:
        tokens.add("ashby")
    return tokens


def source_refresh_kind(source: Any) -> str:
    tokens = source_site_tokens(source)
    if tokens & PROHIBITED_SCRAPER_SOURCE_SITES:
        return "prohibited"
    if tokens & COMPLIANT_REFRESH_SOURCE_SITES:
        return "compliant_ats"
    if source is None:
        return "none"
    return "unavailable"
