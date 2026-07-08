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
_MATCH_DEGRADED_REASONS = frozenset({
    "canonical_selection_unavailable",
    "match_query_unavailable",
    "llm_evaluation_lookup_unavailable",
    "policy_llm_enqueue_unavailable",
    "policy_unavailable",
    "unsupported_cursor_ranking_mode",
    "degraded",
})
_MATCH_QUERY_MODES = frozenset({"offset", "cursor"})
_MATCH_QUERY_VIEWS = frozenset({"summary", "compact"})
_EMAIL_EVENTS = frozenset({
    "sent",
    "verified",
    "expired",
    "rate_limited",
    "invalid_address",
    "cleared",
})
_WORKER_SERVICES = frozenset({"extraction", "embeddings", "matcher", "llm_evaluation"})
_WORKER_NAMES = frozenset({"consumer", "batch_consumer", "worker"})
_PIPELINE_STAGES = frozenset({
    "scrape",
    "extraction",
    "embedding",
    "matching",
    "repair",
    "resume_extraction",
    "resume_embedding",
})
_LLM_EVALUATION_QUEUE_REGISTRIES = frozenset({"queued", "started", "deferred", "scheduled", "failed"})
_LLM_EVALUATION_BACKLOG_STATUSES = frozenset({"pending", "running", "failed", "retryable_failed"})
_LLM_JUDGE_PROVIDERS = frozenset({"nvidia", "groq", "cerebras", "openai_compatible"})
_LLM_JUDGE_ERROR_CATEGORIES = frozenset(
    {
        "rate_limit",
        "timeout",
        "connection_error",
        "server_error",
        "circuit_open",
        "invalid_auth",
        "invalid_request",
        "schema_error",
        "unsupported_model",
        "input_too_large",
        "unknown",
    }
)
_LLM_JUDGE_SCHEDULER_EVENTS = frozenset({"scheduled", "reused", "succeeded", "failed"})
_LLM_JUDGE_CIRCUIT_EVENTS = frozenset({"opened", "skip", "closed", "manual_reset"})
_LLM_JUDGE_WAIT_OUTCOMES = frozenset({"waited", "retry_after", "unavailable"})
_LLM_QUEUE_OPERATOR_ACTIONS = frozenset({"pause", "resume", "retry"})
_LLM_PROVIDER_CANARY_STATUSES = frozenset({"succeeded", "failed", "rate_limited", "circuit_open"})
_DESCRIPTION_RECOVERY_PROVIDERS = frozenset(
    {"greenhouse", "lever", "ashby", "unsupported", "prohibited", "unmapped"}
)
_DESCRIPTION_RECOVERY_OUTCOMES = frozenset(
    {
        "queued",
        "processed",
        "description_found",
        "posting_not_found",
        "source_unsupported",
        "source_prohibited",
        "source_unmapped",
        "failed_retryable",
        "failed_terminal",
    }
)
_OCI_CRITICAL_LOG_EVENT_TYPES = frozenset({
    "description_recovery",
    "deploy_event",
    "provider_canary",
    "provider_circuit",
    "queue_operator_action",
    "readiness_check",
    "scheduler_job",
    "source_sync_failure",
    "worker_started",
    "worker_stopped",
})
_OCI_CRITICAL_LOG_OUTCOMES = frozenset({"disabled", "dropped", "error", "written"})
_OCI_CRITICAL_LOG_DROP_REASONS = frozenset({
    "cap_disabled",
    "cap_exceeded",
    "disabled",
    "write_error",
})
_OCI_CRITICAL_LOG_SERVICES = frozenset({
    "app",
    "cloud_ops",
    "embeddings",
    "extraction",
    "llm_evaluation_worker",
    "notification_worker",
    "orchestrator",
    "scorer_matcher",
})


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

match_query_degraded_reason_total = Counter(
    f"{NAMESPACE}_match_query_degraded_reason_total",
    "Soft-degrade trigger that affected the match query read path.",
    labelnames=("reason",),
)

match_query_rows_loaded = Histogram(
    f"{NAMESPACE}_match_query_rows_loaded",
    "Rows loaded by the match list read path per request.",
    labelnames=("mode", "view"),
    buckets=(0, 1, 5, 10, 25, 50, 100, 250, 500),
)

match_query_payload_bytes = Histogram(
    f"{NAMESPACE}_match_query_payload_bytes",
    "Serialized match list response size in bytes.",
    labelnames=("mode", "view"),
    buckets=(512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072),
)

llm_rerank_window_size = Histogram(
    f"{NAMESPACE}_llm_rerank_window_size",
    "Candidates included in the display-time LLM rerank window.",
    labelnames=("mode",),
    buckets=(0, 1, 3, 5, 10, 25, 50),
)

llm_rerank_policy_revision = Gauge(
    f"{NAMESPACE}_llm_rerank_policy_revision",
    "Last applied LLM judge policy revision observed by the match read path.",
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

jobs_imported = Counter(
    f"{NAMESPACE}_jobs_imported",
    "Jobs imported into the durable inventory.",
)

jobs_extraction_queued = Counter(
    f"{NAMESPACE}_jobs_extraction_queued",
    "Jobs queued for extraction.",
)

jobs_extracted = Counter(
    f"{NAMESPACE}_jobs_extracted",
    "Jobs that completed extraction.",
)

jobs_embedding_queued = Counter(
    f"{NAMESPACE}_jobs_embedding_queued",
    "Jobs queued for embedding.",
)

jobs_embedded = Counter(
    f"{NAMESPACE}_jobs_embedded",
    "Jobs that completed embedding.",
)

jobs_matched = Counter(
    f"{NAMESPACE}_jobs_matched",
    "Jobs that produced persisted matches.",
)

jobs_eligible_for_extraction = Gauge(
    f"{NAMESPACE}_jobs_eligible_for_extraction",
    "Current jobs eligible to queue for extraction.",
)

jobs_eligible_for_embedding = Gauge(
    f"{NAMESPACE}_jobs_eligible_for_embedding",
    "Current jobs eligible to queue for embedding.",
)

jobs_ready_for_matching = Gauge(
    f"{NAMESPACE}_jobs_ready_for_matching",
    "Current jobs extracted and embedded, ready for matching.",
)

jobs_inventory_total = Gauge(
    f"{NAMESPACE}_jobs_inventory",
    "Current job inventory count by lifecycle bucket.",
    labelnames=("bucket",),
)

jobs_stuck_by_stage = Gauge(
    f"{NAMESPACE}_jobs_stuck_by_stage",
    "Current jobs stuck or retryable by bounded pipeline stage.",
    labelnames=("stage",),
)

llm_evaluation_queue_jobs = Gauge(
    f"{NAMESPACE}_llm_evaluation_queue_jobs",
    "Current LLM evaluation RQ job count by bounded registry.",
    labelnames=("registry",),
)

llm_evaluation_backlog_rows = Gauge(
    f"{NAMESPACE}_llm_evaluation_backlog_rows",
    "Current durable LLM evaluation DB backlog count by bounded status.",
    labelnames=("status",),
)

llm_evaluation_oldest_age_seconds = Gauge(
    f"{NAMESPACE}_llm_evaluation_oldest_age_seconds",
    "Age in seconds of the oldest durable LLM evaluation backlog row by bounded status.",
    labelnames=("status",),
)

llm_judge_provider_fallbacks_total = Counter(
    f"{NAMESPACE}_llm_judge_provider_fallbacks_total",
    "Match-level LLM judge provider fallbacks after transient provider failures.",
    labelnames=("from_provider", "to_provider", "error_category"),
)

llm_judge_scheduler_jobs_total = Counter(
    f"{NAMESPACE}_llm_judge_scheduler_jobs_total",
    "Top-N LLM judge scheduler job events.",
    labelnames=("event",),
)

llm_judge_provider_circuit_events_total = Counter(
    f"{NAMESPACE}_llm_judge_provider_circuit_events_total",
    "LLM judge provider circuit breaker events.",
    labelnames=("provider", "event"),
)

llm_judge_provider_wait_seconds = Histogram(
    f"{NAMESPACE}_llm_judge_provider_wait_seconds",
    "LLM judge provider wait or retry-after seconds.",
    labelnames=("provider", "outcome"),
    buckets=(0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

llm_evaluation_queue_operator_actions_total = Counter(
    f"{NAMESPACE}_llm_evaluation_queue_operator_actions_total",
    "Manual LLM evaluation queue operator actions.",
    labelnames=("action",),
)

llm_judge_provider_canaries_total = Counter(
    f"{NAMESPACE}_llm_judge_provider_canaries_total",
    "Explicit LLM judge provider canary outcomes.",
    labelnames=("provider", "status", "error_category"),
)

oci_critical_log_events_total = Counter(
    f"{NAMESPACE}_oci_critical_log_events_total",
    "OCI critical-only JSONL logging outcomes.",
    labelnames=("event_type", "outcome"),
)

oci_critical_log_bytes_total = Counter(
    f"{NAMESPACE}_oci_critical_log_bytes_total",
    "Bytes written to the OCI critical-only JSONL log stream.",
    labelnames=("event_type",),
)

oci_critical_log_dropped_total = Counter(
    f"{NAMESPACE}_oci_critical_log_dropped_total",
    "OCI critical-only JSONL events dropped before write.",
    labelnames=("reason",),
)

oci_critical_log_budget_usage_ratio = Gauge(
    f"{NAMESPACE}_oci_critical_log_budget_usage_ratio",
    "Per-service fraction of the daily OCI critical-only log byte cap already used.",
    labelnames=("service",),
)

description_recovery_jobs_total = Counter(
    f"{NAMESPACE}_description_recovery_jobs_total",
    "Missing-description recovery job outcomes by bounded provider.",
    labelnames=("provider", "outcome"),
)

description_recovery_oldest_missing_age_seconds = Gauge(
    f"{NAMESPACE}_description_recovery_oldest_missing_age_seconds",
    "Age in seconds of the oldest active job still missing a description.",
)

description_recovery_provider_latency_seconds = Histogram(
    f"{NAMESPACE}_description_recovery_provider_latency_seconds",
    "Provider snapshot latency for missing-description recovery.",
    labelnames=("provider",),
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
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


def record_match_query_degraded(reason: str | None = None) -> None:
    match_query_degraded_reason_total.labels(
        reason=_safe(reason or "degraded", _MATCH_DEGRADED_REASONS),
    ).inc()

def record_match_query_rows_loaded(mode: str, view: str, count: int | float) -> None:
    match_query_rows_loaded.labels(
        mode=_safe(mode, _MATCH_QUERY_MODES),
        view=_safe(view, _MATCH_QUERY_VIEWS),
    ).observe(max(float(count or 0), 0.0))

def record_match_query_payload_bytes(mode: str, view: str, byte_count: int | float) -> None:
    match_query_payload_bytes.labels(
        mode=_safe(mode, _MATCH_QUERY_MODES),
        view=_safe(view, _MATCH_QUERY_VIEWS),
    ).observe(max(float(byte_count or 0), 0.0))

def record_llm_rerank_window_size(mode: str, count: int | float) -> None:
    llm_rerank_window_size.labels(
        mode=_safe(mode, _MATCH_QUERY_MODES),
    ).observe(max(float(count or 0), 0.0))

def set_llm_rerank_policy_revision(revision: int | float | None) -> None:
    if revision is None:
        return
    llm_rerank_policy_revision.set(max(float(revision or 0), 0.0))


def record_email_event(event: str) -> None:
    email_verification_events_total.labels(event=_safe(event, _EMAIL_EVENTS)).inc()


def record_worker_running(service: str, worker: str, running: bool) -> None:
    worker_running.labels(
        service=_safe(service, _WORKER_SERVICES),
        worker=_safe(worker, _WORKER_NAMES),
    ).set(1 if running else 0)
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event(
            "worker_started" if running else "worker_stopped",
            worker_service=service,
            worker=worker,
        )
    except Exception:
        pass


def record_llm_judge_provider_fallback(
    from_provider: str,
    to_provider: str,
    error_category: str,
) -> None:
    llm_judge_provider_fallbacks_total.labels(
        from_provider=_safe(from_provider, _LLM_JUDGE_PROVIDERS),
        to_provider=_safe(to_provider, _LLM_JUDGE_PROVIDERS),
        error_category=_safe(error_category, _LLM_JUDGE_ERROR_CATEGORIES),
    ).inc()


def record_llm_judge_scheduler_job(event: str) -> None:
    llm_judge_scheduler_jobs_total.labels(
        event=_safe(event, _LLM_JUDGE_SCHEDULER_EVENTS),
    ).inc()
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event("scheduler_job", scheduler_event=event)
    except Exception:
        pass


def record_llm_judge_provider_circuit_event(provider: str, event: str) -> None:
    llm_judge_provider_circuit_events_total.labels(
        provider=_safe(provider, _LLM_JUDGE_PROVIDERS),
        event=_safe(event, _LLM_JUDGE_CIRCUIT_EVENTS),
    ).inc()
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event(
            "provider_circuit",
            provider=_safe(provider, _LLM_JUDGE_PROVIDERS),
            circuit_event=event,
        )
    except Exception:
        pass


def observe_llm_judge_provider_wait_seconds(
    provider: str,
    outcome: str,
    seconds: float,
) -> None:
    llm_judge_provider_wait_seconds.labels(
        provider=_safe(provider, _LLM_JUDGE_PROVIDERS),
        outcome=_safe(outcome, _LLM_JUDGE_WAIT_OUTCOMES),
    ).observe(max(float(seconds or 0.0), 0.0))


def record_llm_evaluation_queue_operator_action(action: str) -> None:
    llm_evaluation_queue_operator_actions_total.labels(
        action=_safe(action, _LLM_QUEUE_OPERATOR_ACTIONS),
    ).inc()
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event("queue_operator_action", action=action)
    except Exception:
        pass


def record_llm_judge_provider_canary(
    provider: str,
    status: str,
    error_category: str | None = None,
) -> None:
    llm_judge_provider_canaries_total.labels(
        provider=_safe(provider, _LLM_JUDGE_PROVIDERS),
        status=_safe(status, _LLM_PROVIDER_CANARY_STATUSES),
        error_category=_safe(error_category or "unknown", _LLM_JUDGE_ERROR_CATEGORIES),
    ).inc()
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event(
            "provider_canary",
            provider=_safe(provider, _LLM_JUDGE_PROVIDERS),
            status=status,
            error_category=error_category or "unknown",
        )
    except Exception:
        pass

def record_description_recovery_job(provider: str, outcome: str, count: int | float = 1) -> None:
    _inc_counter(
        description_recovery_jobs_total.labels(
            provider=_safe(provider, _DESCRIPTION_RECOVERY_PROVIDERS),
            outcome=_safe(outcome, _DESCRIPTION_RECOVERY_OUTCOMES),
        ),
        count,
    )

def set_description_recovery_oldest_missing_age_seconds(seconds: int | float | None) -> None:
    description_recovery_oldest_missing_age_seconds.set(max(float(seconds or 0), 0.0))

def observe_description_recovery_provider_latency_seconds(provider: str, seconds: float) -> None:
    description_recovery_provider_latency_seconds.labels(
        provider=_safe(provider, _DESCRIPTION_RECOVERY_PROVIDERS),
    ).observe(max(float(seconds or 0.0), 0.0))


def record_oci_critical_log_event(event_type: str, outcome: str) -> None:
    oci_critical_log_events_total.labels(
        event_type=_safe(event_type, _OCI_CRITICAL_LOG_EVENT_TYPES),
        outcome=_safe(outcome, _OCI_CRITICAL_LOG_OUTCOMES),
    ).inc()


def observe_oci_critical_log_bytes(event_type: str, byte_count: int | float) -> None:
    _inc_counter(
        oci_critical_log_bytes_total.labels(
            event_type=_safe(event_type, _OCI_CRITICAL_LOG_EVENT_TYPES),
        ),
        byte_count,
    )


def record_oci_critical_log_drop(reason: str) -> None:
    oci_critical_log_dropped_total.labels(
        reason=_safe(reason, _OCI_CRITICAL_LOG_DROP_REASONS),
    ).inc()


def set_oci_critical_log_budget_usage_ratio(service: str, ratio: int | float) -> None:
    oci_critical_log_budget_usage_ratio.labels(
        service=_safe(service, _OCI_CRITICAL_LOG_SERVICES),
    ).set(max(min(float(ratio or 0), 1.0), 0.0))


def _inc_counter(counter: Counter, count: int | float = 1) -> None:
    value = max(float(count or 0), 0.0)
    if value:
        counter.inc(value)


def _stage(value: str) -> str:
    aliases = {
        "extract": "extraction",
        "embed": "embedding",
        "match": "matching",
        "extracting": "resume_extraction",
        "resume_etl": "resume_embedding",
    }
    return _safe(aliases.get(value, value), _PIPELINE_STAGES)


def record_jobs_imported(count: int | float = 1) -> None:
    _inc_counter(jobs_imported, count)


def record_jobs_extraction_queued(count: int | float = 1) -> None:
    _inc_counter(jobs_extraction_queued, count)


def record_jobs_extracted(count: int | float = 1) -> None:
    _inc_counter(jobs_extracted, count)


def record_jobs_embedding_queued(count: int | float = 1) -> None:
    _inc_counter(jobs_embedding_queued, count)


def record_jobs_embedded(count: int | float = 1) -> None:
    _inc_counter(jobs_embedded, count)


def record_jobs_matched(count: int | float = 1) -> None:
    _inc_counter(jobs_matched, count)


def set_jobs_stuck_by_stage(stage: str, count: int | float) -> None:
    jobs_stuck_by_stage.labels(stage=_stage(stage)).set(max(float(count or 0), 0.0))


def set_llm_evaluation_queue_depth(registry: str, count: int | float) -> None:
    llm_evaluation_queue_jobs.labels(
        registry=_safe(registry, _LLM_EVALUATION_QUEUE_REGISTRIES),
    ).set(max(float(count or 0), 0.0))

def set_llm_evaluation_backlog_metrics(stats: dict[str, object]) -> None:
    """Project durable LLM evaluation backlog stats into bounded gauges."""
    llm_evaluation_backlog_rows.labels(status="pending").set(
        float(stats.get("db_pending") or 0)
    )
    llm_evaluation_backlog_rows.labels(status="running").set(
        float(stats.get("db_running") or 0)
    )
    llm_evaluation_backlog_rows.labels(status="failed").set(
        float(stats.get("db_failed") or 0)
    )
    llm_evaluation_backlog_rows.labels(status="retryable_failed").set(
        float(stats.get("db_retryable_failed") or 0)
    )
    llm_evaluation_oldest_age_seconds.labels(status="pending").set(
        float(stats.get("oldest_pending_age_seconds") or 0)
    )
    llm_evaluation_oldest_age_seconds.labels(status="retryable_failed").set(
        float(stats.get("oldest_retryable_failed_age_seconds") or 0)
    )


def set_job_inventory_metrics(stats: dict[str, object]) -> None:
    """Project job inventory/eligibility stats into bounded gauges."""
    jobs_inventory_total.labels(bucket="total").set(float(stats.get("job_post_total") or 0))
    jobs_inventory_total.labels(bucket="active").set(float(stats.get("active_job_posts") or 0))
    jobs_inventory_total.labels(bucket="inactive").set(float(stats.get("inactive_job_posts") or 0))
    jobs_eligible_for_extraction.set(
        float(stats.get("pending_extraction_job_posts") or 0)
        + float(stats.get("retryable_extraction_job_posts") or 0)
    )
    jobs_eligible_for_embedding.set(
        float(stats.get("pending_embedding_job_posts") or 0)
        + float(stats.get("retryable_embedding_job_posts") or 0)
    )
    jobs_ready_for_matching.set(float(stats.get("ready_to_score_job_posts") or 0))
    set_jobs_stuck_by_stage(
        "extraction",
        float(stats.get("processing_extraction_job_posts") or 0)
        + float(stats.get("retryable_extraction_job_posts") or 0)
        + float(stats.get("failed_extraction_job_posts") or 0),
    )
    set_jobs_stuck_by_stage(
        "embedding",
        float(stats.get("processing_embedding_job_posts") or 0)
        + float(stats.get("retryable_embedding_job_posts") or 0)
        + float(stats.get("failed_embedding_job_posts") or 0),
    )


def bind_worker_running(service: str, worker: str, callback: Callable[[], bool]) -> None:
    """Expose worker liveness as a scrape-time callback-backed gauge."""
    worker_running.labels(
        service=_safe(service, _WORKER_SERVICES),
        worker=_safe(worker, _WORKER_NAMES),
    ).set_function(lambda: 1 if callback() else 0)


def bind_llm_evaluation_queue_depths(callback: Callable[[], dict[str, int]]) -> None:
    """Expose LLM evaluation RQ registry depths as scrape-time gauges."""
    for registry in _LLM_EVALUATION_QUEUE_REGISTRIES:
        llm_evaluation_queue_jobs.labels(registry=registry).set_function(
            lambda registry=registry: float(callback().get(registry, 0) or 0)
        )
