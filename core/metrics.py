"""Prometheus metric definitions — single source of truth.

Every metric used by any JobScout service is declared here. Emit sites
import these singletons (or the typed ``record_*`` helpers) and call
``.inc()`` / ``.observe()``. Names, labels, buckets, and the safe-value
enums all live here; do NOT redeclare a Counter at the emit site.

Metric contract: ``v1`` (2026-04-20). Bump the version in this docstring
when label sets or metric names change — that is a coordinated cross-repo
break with ``jobscout-cloud`` dashboards.

Multiprocess note: we currently run uvicorn single-process. If we ever
switch to ``--workers N`` set ``PROMETHEUS_MULTIPROC_DIR=/tmp/promstats``
and swap to ``MultiProcessCollector``. Until then, default ``REGISTRY``
is correct.
"""

from __future__ import annotations

from typing import Callable

from prometheus_client import Counter, Gauge, Histogram

NAMESPACE = "jobscout"

_ROUTE_VALUES = frozenset({"local_native", "local_heuristic", "remote", "threshold", "llm"})
_DEGRADED_REASONS = frozenset({
    "remote_unavailable",
    "local_unavailable",
    "provider_disabled",
    "degraded",
})
_TIER_VALUES = frozenset({"primary", "excluded"})
_EXCLUSION_REASONS = frozenset({
    "none",
    "below_min_fit",
    "beyond_top_k",
    "below_coverage_floor",
    "truncated",
})
_PREF_REASONS = frozenset({
    "applied",
    "disabled",
    "unconfigured",
    "runtime_error",
    "preference_judge_unavailable",
    "preference_reranker_unavailable",
    "preference_profile_unavailable",
})
_EMAIL_EVENTS = frozenset({
    "sent",
    "verified",
    "expired",
    "rate_limited",
    "invalid_address",
    "cleared",
})
_WORKER_SERVICES = frozenset({"extraction", "embeddings", "matcher"})
_WORKER_NAMES = frozenset({"consumer", "batch_consumer"})


def _safe(value: str, allowed: frozenset[str]) -> str:
    """Collapse arbitrary values to a closed set; bounds label cardinality."""
    return value if value in allowed else "other"


# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------

scorer_route_total = Counter(
    f"{NAMESPACE}_scorer_route_total",
    "Cross-encoder route taken when scoring a requirement batch.",
    labelnames=("route",),
)

scorer_degraded_reason_total = Counter(
    f"{NAMESPACE}_scorer_degraded_reason_total",
    "Soft-degrade trigger that produced a fallback verdict.",
    labelnames=("reason",),
)

evidence_rerank_latency_ms = Histogram(
    f"{NAMESPACE}_evidence_rerank_latency_ms",
    "Wall time spent in cross-encoder evidence rerank per job.",
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500),
)

selection_tier_items_total = Counter(
    f"{NAMESPACE}_selection_tier_items_total",
    "Match selection items emitted by tier and exclusion reason.",
    labelnames=("tier", "reason"),
)

preference_reranker_status_total = Counter(
    f"{NAMESPACE}_preference_reranker_status_total",
    "Outcome of preference reranking per pipeline run.",
    labelnames=("applied", "reason"),
)

email_verification_events_total = Counter(
    f"{NAMESPACE}_email_verification_events_total",
    "Lifecycle events for the user's notification email override.",
    labelnames=("event",),
)

worker_running = Gauge(
    f"{NAMESPACE}_worker_running",
    "Whether a background worker loop is currently running (1) or stopped (0).",
    labelnames=("service", "worker"),
)


# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------

_DEGRADE_SUBSTRINGS: tuple[tuple[str, str], ...] = (
    ("no remote provider", "remote_unavailable"),
    ("remote cross-encoder route", "remote_unavailable"),
    ("remote provider is disabled", "provider_disabled"),
    ("local provider is disabled", "provider_disabled"),
    ("local cross-encoder route", "local_unavailable"),
    ("no local provider", "local_unavailable"),
    ("no_provider_available", "degraded"),
)


def _classify_degrade_reason(value: str | BaseException | None) -> str:
    """Map a free-form degrade reason to the canonical enum.

    ``core/scorer/semantic_fit.py:_fallback_result`` records raw exception
    strings; this classifier folds known phrasings into the closed set so
    Prometheus label cardinality stays bounded.
    """

    if value is None:
        return "degraded"
    raw = str(value).lower()
    for needle, canonical in _DEGRADE_SUBSTRINGS:
        if needle in raw:
            return canonical
    return raw


# ---------------------------------------------------------------------------
# Typed emit helpers — prefer these over raw ``.labels(...)`` at emit sites.
# ---------------------------------------------------------------------------

def record_scorer_route(route: str) -> None:
    scorer_route_total.labels(route=_safe(route, _ROUTE_VALUES)).inc()


def record_scorer_degraded(reason: str | BaseException | None) -> None:
    canonical = _classify_degrade_reason(reason)
    scorer_degraded_reason_total.labels(
        reason=_safe(canonical, _DEGRADED_REASONS),
    ).inc()


def record_selection_tier_item(tier: str, reason: str | None = None) -> None:
    selection_tier_items_total.labels(
        tier=_safe(tier, _TIER_VALUES),
        reason=_safe(reason or "none", _EXCLUSION_REASONS),
    ).inc()


def record_preference_status(applied: bool, reason: str | None) -> None:
    # Strip the ``runtime_error:ExceptionName`` suffix that
    # candidate_preferences.py attaches so it collapses to the ``runtime_error``
    # bucket instead of ``other``.
    canonical = (reason or ("applied" if applied else "other")).split(":", 1)[0]
    preference_reranker_status_total.labels(
        applied="true" if applied else "false",
        reason=_safe(canonical, _PREF_REASONS),
    ).inc()


def record_email_event(event: str) -> None:
    email_verification_events_total.labels(event=_safe(event, _EMAIL_EVENTS)).inc()


def record_worker_running(service: str, worker: str, running: bool) -> None:
    worker_running.labels(
        service=_safe(service, _WORKER_SERVICES),
        worker=_safe(worker, _WORKER_NAMES),
    ).set(1 if running else 0)


def bind_worker_running(service: str, worker: str, callback: Callable[[], bool]) -> None:
    """Expose worker liveness as a scrape-time callback-backed gauge."""
    worker_running.labels(
        service=_safe(service, _WORKER_SERVICES),
        worker=_safe(worker, _WORKER_NAMES),
    ).set_function(lambda: 1 if callback() else 0)
