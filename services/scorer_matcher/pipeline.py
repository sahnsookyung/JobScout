"""Matching pipeline for the scorer-matcher service.

This module contains the core matching pipeline logic: loading a resume,
running vector matching and scoring, saving results, and dispatching
notifications.
"""

import time
import logging
import threading
from typing import List, Optional, Dict, Any, Callable

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from core.app_context import AppContext
from core.config_loader import PreferencesConfig
from core.llm_evaluation_queue import enqueue_llm_top_n_for_selection
from core.match_selection import (
    MatchSelectionItemSnapshot,
    MatchSelectionPolicySnapshot,
    select_matches,
)
from core.policy import get_result_policy_store
from core.matcher import (
    MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO,
    RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm,
)
from core.ranking import RankingContext, RankingMode, get_ranking_policy_store
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from core.scorer.semantic_fit import get_shared_local_cross_encoder_provider
from core.llm.interfaces import LLMProvider
from core.llm.schema_models import ResumeSchema
from database.models import SYSTEM_OWNER_ID
from etl.resume import ResumeProfiler
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow
from notification.orchestrator import resolve_notification_fit_floor, send_notifications
from services.scorer_matcher.candidate_preferences import (
    PreferenceStatus,
    PreferenceRerankResult,
    apply_candidate_preference_filters,
    apply_preference_semantic_reranking,
    load_candidate_preferences,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SaveMatchesBatchResult:
    saved_count: int
    failed_count: int
    active_job_ids: frozenset[str]
    job_match_ids_by_job_id: dict[str, str]


@dataclass(frozen=True)
class PreparedSelectionResult:
    match_dtos: List[MatchResultDTO]
    item_snapshots: List[MatchSelectionItemSnapshot]
    policy_snapshot: MatchSelectionPolicySnapshot
    owner_id: Optional[str]
    persist_match_dtos: List[MatchResultDTO] = field(default_factory=list)
    cached_job_match_ids_by_job_id: dict[str, str] = field(default_factory=dict)
    matching_page_size: int = 0
    matching_backlog_remaining: int = 0
    reusable_match_count: int = 0


def _cancelled_prepared_selection_result(
    *,
    owner_id: Optional[object],
    ranking_context: RankingContext,
    resume_resolution_reason: str,
    task_id: Optional[str],
) -> PreparedSelectionResult:
    """Return the declared pipeline envelope for an already-cancelled run."""
    policy_snapshot = MatchSelectionPolicySnapshot.from_ranking_context(
        ranking_context=ranking_context,
        fit_floor_used=0.0,
        required_coverage_floor_used=None,
        notification_fit_floor_used=0.0,
        top_k_used=0,
        candidate_pool_size=0,
        selected_count=0,
        alert_candidate_count=0,
        resume_resolution_reason=resume_resolution_reason,
        task_id=task_id,
    )
    return PreparedSelectionResult(
        match_dtos=[],
        item_snapshots=[],
        policy_snapshot=policy_snapshot,
        owner_id=(str(owner_id) if owner_id is not None else None),
    )


@dataclass
class MatchingPipelineResult:
    """Result of running the matching pipeline."""
    success: bool
    matches_count: int
    saved_count: int
    notified_count: int
    error: Optional[str] = None
    execution_time: float = 0.0
    cancelled: bool = False


def _resolve_ranking_context() -> RankingContext:
    """Resolve the ranking policy once so ranking and notifications share a snapshot."""
    ranking_config = get_ranking_policy_store().get_current_config()
    try:
        ranking_mode = RankingMode(ranking_config.active_default_mode)
    except ValueError:
        ranking_mode = RankingMode.BALANCED
    return RankingContext(mode=ranking_mode, config=ranking_config)


def _load_resume_from_db(resume_fingerprint: str) -> Optional[dict]:
    """Load resume extracted_data from database using fingerprint."""
    logger.info("Loading resume from database: %s...", resume_fingerprint[:16])
    try:
        with job_uow() as repo:
            structured_resume = repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)
            if not structured_resume:
                logger.error("No resume found in DB for fingerprint: %s...", resume_fingerprint[:16])
                return None
            if not structured_resume.extracted_data:
                logger.error("Resume found but no extracted_data for fingerprint: %s...", resume_fingerprint[:16])
                return None
            return structured_resume.extracted_data
    except Exception:
        logger.exception("Error loading resume from DB")
        return None


def _error_result(
    error: str,
    *,
    matches_count: int = 0,
    saved_count: int = 0,
    notified_count: int = 0,
    execution_time: float = 0.0,
    cancelled: bool = False,
) -> MatchingPipelineResult:
    """Build an error result with consistent defaults."""
    return MatchingPipelineResult(
        success=False,
        matches_count=matches_count,
        saved_count=saved_count,
        notified_count=notified_count,
        error=error,
        execution_time=execution_time,
        cancelled=cancelled,
    )


def _success_result(
    matches_count: int,
    saved_count: int,
    notified_count: int,
    execution_time: float,
) -> MatchingPipelineResult:
    """Build a successful pipeline result."""
    return MatchingPipelineResult(
        success=True,
        matches_count=matches_count,
        saved_count=saved_count,
        notified_count=notified_count,
        execution_time=execution_time,
    )


def _cancelled_result(
    error: str,
    *,
    matches_count: int = 0,
    saved_count: int = 0,
    notified_count: int = 0,
    execution_time: float = 0.0,
) -> MatchingPipelineResult:
    """Build a cancelled pipeline result."""
    return _error_result(
        error,
        matches_count=matches_count,
        saved_count=saved_count,
        notified_count=notified_count,
        execution_time=execution_time,
        cancelled=True,
    )


def _load_requested_resume(
    resume_fingerprint: str,
) -> tuple[Optional[Dict[str, Any]], bool, Optional[MatchingPipelineResult]]:
    """Load resume data for an explicitly requested fingerprint."""
    resume_data = _load_resume_from_db(resume_fingerprint)
    if not resume_data:
        return None, False, _error_result(
            "Resume not found in DB for fingerprint: %s..." % resume_fingerprint[:16],
        )

    logger.info("Loaded resume from database (fingerprint: %s...)", resume_fingerprint[:16])
    return resume_data, False, None


def _load_latest_ready_resume(
) -> tuple[Optional[str], Optional[Dict[str, Any]], bool, Optional[MatchingPipelineResult]]:
    """Load the latest ready resume data from storage."""
    latest_processing_state = None
    resume_data = None

    with job_uow() as repo:
        resume_fingerprint = repo.get_latest_ready_resume_fingerprint()
        latest_processing_state = repo.get_latest_resume_processing_state()

        if resume_fingerprint:
            structured_resume = repo.resume.get_structured_resume_by_fingerprint(
                resume_fingerprint
            )
            if not structured_resume or not structured_resume.extracted_data:
                return None, None, False, _error_result(
                    f"Ready resume {resume_fingerprint[:16]}... is missing structured data",
                )
            resume_data = structured_resume.extracted_data

    if resume_fingerprint:
        return resume_fingerprint, resume_data, False, None

    if latest_processing_state and latest_processing_state.processing_status in {
        "extracting",
        "extracted",
        "embedding",
    }:
        return (
            None,
            None,
            False,
            _error_result(
                "Latest resume upload is still processing "
                f"({latest_processing_state.processing_status}).",
            ),
        )

    return None, None, False, _error_result(
        "No ready resume found. Upload and process a resume first.",
    )


def _load_pipeline_resume(
    resume_fingerprint: Optional[str],
) -> tuple[Optional[Dict[str, Any]], Optional[str], bool, Optional[MatchingPipelineResult]]:
    """Load the resume data used by the matching pipeline."""
    if resume_fingerprint:
        resume_data, should_re_extract, error_result = _load_requested_resume(resume_fingerprint)
        return resume_data, resume_fingerprint, should_re_extract, error_result

    latest_fingerprint, resume_data, should_re_extract, error_result = _load_latest_ready_resume()
    return resume_data, latest_fingerprint, should_re_extract, error_result


def _result_after_matching(
    match_dtos: List[MatchResultDTO],
    stop_event: threading.Event,
) -> Optional[MatchingPipelineResult]:
    """Return the appropriate result when matching finished under cancellation."""
    if not stop_event.is_set():
        return None
    if not match_dtos:
        return _cancelled_result("Cancelled by user")
    return _cancelled_result(
        "Cancelled by user before saving results",
        matches_count=len(match_dtos),
    )


def _result_after_saving(
    match_dtos: List[MatchResultDTO],
    saved_count: int,
    stop_event: threading.Event,
    pipeline_start_time: float,
) -> Optional[MatchingPipelineResult]:
    """Return the appropriate result when the save step finished under cancellation."""
    if not stop_event.is_set():
        return None
    return _cancelled_result(
        "Cancelled after saving results",
        matches_count=len(match_dtos),
        saved_count=saved_count,
        execution_time=time.time() - pipeline_start_time,
    )


def _finish_pipeline_result(
    match_dtos: List[MatchResultDTO],
    saved_count: int,
    notified_count: int,
    stop_event: threading.Event,
    pipeline_start_time: float,
) -> MatchingPipelineResult:
    """Build the final pipeline result after completion logging."""
    execution_time = time.time() - pipeline_start_time
    logger.info("=" * 60)
    logger.info("MATCHING PIPELINE COMPLETED in %.2fs", execution_time)
    logger.info("=" * 60)

    if stop_event.is_set():
        return _cancelled_result(
            "Cancelled by user",
            matches_count=len(match_dtos),
            saved_count=saved_count,
            notified_count=notified_count,
            execution_time=execution_time,
        )

    return _success_result(
        matches_count=len(match_dtos),
        saved_count=saved_count,
        notified_count=notified_count,
        execution_time=execution_time,
    )


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_matching_pipeline(
    ctx: AppContext,
    stop_event: Optional[threading.Event] = None,
    status_callback: Optional[Callable[[str], None]] = None,
    resume_fingerprint: Optional[str] = None,
    owner_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> MatchingPipelineResult:
    """Run the matching pipeline as a self-contained operation.

    Queries the database for the latest stored resume, loads raw resume data,
    performs matching and scoring, saves results, and sends notifications.
    Can run standalone without requiring ETL to have run in the same process.

    Args:
        ctx: Application context with config, AI service, and other dependencies.
        stop_event: Optional threading event to signal early termination.
        status_callback: Optional callable invoked with a status string at each
            major pipeline stage (e.g. "loading_resume", "scoring", "notifying").
        resume_fingerprint: If provided, load the resume from the database using
            this fingerprint instead of the latest ready resume.
        owner_id: Optional authenticated owner identity for notification tracking.
        task_id: Optional matching task id for notification correlation.

    Returns:
        MatchingPipelineResult with success status and counts.
    """
    if stop_event is None:
        stop_event = threading.Event()

    logger.info("=" * 60)
    logger.info("STARTING MATCHING PIPELINE")
    logger.info("=" * 60)

    pipeline_start_time = time.time()
    requested_resume_fingerprint = resume_fingerprint

    matching_config = ctx.config.matching
    if not matching_config or not matching_config.enabled:
        logger.info("=== MATCHING PIPELINE: Skipped (disabled in config) ===")
        return MatchingPipelineResult(
            success=True, matches_count=0, saved_count=0,
            notified_count=0, error="Matching disabled in config",
        )

    try:
        resume_data, resume_fingerprint, should_re_extract, error_result = _load_pipeline_resume(
            resume_fingerprint,
        )
        if error_result:
            return error_result

        ranking_context = _resolve_ranking_context()

        # Step 2: Run matching and scoring
        prepared_selection = _run_matching_and_scoring(
            ctx, resume_data, resume_fingerprint, should_re_extract,
            matching_config, stop_event, status_callback, owner_id=owner_id,
            ranking_context=ranking_context,
            task_id=task_id,
            resume_resolution_reason=(
                "requested_resume_fingerprint"
                if requested_resume_fingerprint
                else "latest_ready_resume"
            ),
        )
        match_dtos = prepared_selection.match_dtos
        matching_result = _result_after_matching(match_dtos, stop_event)
        if matching_result:
            return matching_result

        # Step 4: Save matches
        if status_callback:
            status_callback("saving_results")
        save_batch_result, selection_run_id = _save_results_and_publish_selection(
            match_dtos=match_dtos,
            resume_fingerprint=resume_fingerprint,
            matching_config=matching_config,
            prepared_selection=prepared_selection,
            task_id=task_id,
        )
        saved_count = save_batch_result.saved_count

        save_result = _result_after_saving(
            match_dtos,
            saved_count,
            stop_event,
            pipeline_start_time,
        )
        if save_result:
            return save_result

        # Step 5: Send notifications
        notified_count = _send_run_notifications(
            ctx,
            failed_count=save_batch_result.failed_count,
            resume_fingerprint=resume_fingerprint,
            stop_event=stop_event,
            status_callback=status_callback,
            selection_run_id=selection_run_id,
            owner_id=prepared_selection.owner_id,
            task_id=task_id,
        )

        return _finish_pipeline_result(
            match_dtos,
            saved_count,
            notified_count,
            stop_event,
            pipeline_start_time,
        )

    except Exception as e:
        logger.exception("Error in matching pipeline")
        execution_time = time.time() - pipeline_start_time
        return MatchingPipelineResult(
            success=False, matches_count=0, saved_count=0, notified_count=0,
            error=str(e), execution_time=execution_time,
        )


def _save_results_and_publish_selection(
    *,
    match_dtos: List[MatchResultDTO],
    resume_fingerprint: str,
    matching_config,
    prepared_selection: PreparedSelectionResult,
    task_id: Optional[str],
) -> tuple[SaveMatchesBatchResult, Optional[str]]:
    """Persist the selected match set and publish its immutable run artifact."""
    persist_match_dtos = prepared_selection.persist_match_dtos
    if not persist_match_dtos and not prepared_selection.cached_job_match_ids_by_job_id:
        persist_match_dtos = match_dtos
    save_batch_result = _save_matches_batch(
        persist_match_dtos,
        resume_fingerprint,
        matching_config,
    )
    if save_batch_result.failed_count > 0:
        logger.warning(
            "Skipping active-match refresh for %s because %d match saves failed",
            resume_fingerprint[:16],
            save_batch_result.failed_count,
        )
        return save_batch_result, None

    job_match_ids_by_job_id = _job_match_ids_for_selection(
        prepared_selection,
        save_batch_result,
    )
    _reactivate_selection_matches(prepared_selection.cached_job_match_ids_by_job_id)
    _refresh_resume_match_set(
        resume_fingerprint,
        active_job_ids=_active_job_ids_for_selection(
            prepared_selection,
            fallback_active_job_ids=save_batch_result.active_job_ids,
        ),
    )
    selection_run_id = _publish_match_selection_run(
        owner_id=prepared_selection.owner_id,
        resume_fingerprint=resume_fingerprint,
        task_id=task_id,
        prepared_selection=prepared_selection,
        save_batch_result=save_batch_result,
        job_match_ids_by_job_id=job_match_ids_by_job_id,
    )
    _run_llm_judge_for_selection(
        selection_run_id=selection_run_id,
        owner_id=prepared_selection.owner_id,
    )
    return save_batch_result, selection_run_id


def _run_llm_judge_for_selection(
    *,
    selection_run_id: Optional[str],
    owner_id: Optional[str],
) -> dict[str, int]:
    """Schedule optional match-level LLM judging for the current selected top-N."""
    if not selection_run_id or not owner_id:
        return {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}

    try:
        llm_policy = get_result_policy_store().get_llm_judge_policy(owner_id)
        if (
            not getattr(llm_policy, "auto_enqueue_enabled", False)
            or not llm_policy.enabled
            or not llm_policy.available
        ):
            return {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}

        scheduled = enqueue_llm_top_n_for_selection(
            selection_run_id=selection_run_id,
            owner_id=owner_id,
            tenant_id=None,
            top_n=llm_policy.top_n,
            policy_revision=int(getattr(llm_policy, "revision", 0) or 0),
        )
        logger.info("LLM judge selection scheduling result: %s", scheduled)
        return {
            "attempted": 0,
            "reused": 0,
            "created": 0,
            "enqueued": 1 if scheduled.get("job_id") else 0,
            "failed": 0,
        }
    except Exception:
        logger.exception("Optional LLM judge failed after selection publication")
        return {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 1}


def _job_match_ids_for_selection(
    prepared_selection: PreparedSelectionResult,
    save_batch_result: SaveMatchesBatchResult,
) -> dict[str, str]:
    ids = dict(prepared_selection.cached_job_match_ids_by_job_id)
    ids.update(save_batch_result.job_match_ids_by_job_id)
    return ids


def _reactivate_selection_matches(job_match_ids_by_job_id: dict[str, str]) -> int:
    match_ids = list(job_match_ids_by_job_id.values())
    if not match_ids:
        return 0
    with job_uow() as repo:
        return repo.activate_matches_by_ids(match_ids)


def _active_job_ids_for_selection(
    prepared_selection: PreparedSelectionResult,
    *,
    fallback_active_job_ids: frozenset[str],
) -> frozenset[str]:
    """Return the selected primary jobs that should remain active after a rerun."""
    if not prepared_selection.item_snapshots:
        return fallback_active_job_ids
    return frozenset(
        str(item.job_id)
        for item in prepared_selection.item_snapshots
        if (getattr(item, "selection_tier", None) or "primary") == "primary"
    )


def _ensure_notification_service(ctx: AppContext) -> None:
    """Create the notification service lazily when notifications are enabled."""
    if ctx.notification_service is not None:
        return
    notification_config = getattr(ctx.config, "notifications", None)
    if not notification_config or not notification_config.enabled:
        return

    logger.info("Building notification service lazily for matching pipeline")
    ctx.notification_service = AppContext._build_notification_service(ctx.config)


def _send_run_notifications(
    ctx: AppContext,
    *,
    failed_count: int,
    resume_fingerprint: str,
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]],
    selection_run_id: Optional[str],
    owner_id: Optional[str],
    task_id: Optional[str],
) -> int:
    """Send notifications for the committed selection run, if available."""
    _ensure_notification_service(ctx)
    if ctx.notification_service is None or stop_event.is_set():
        return 0

    if status_callback:
        status_callback("notifying")
    return send_notifications(
        ctx,
        failed_count=failed_count,
        resume_fingerprint=resume_fingerprint,
        stop_event=stop_event,
        selection_run_id=selection_run_id,
        owner_id=owner_id,
        task_id=task_id,
    )

# ---------------------------------------------------------------------------
# Matching & scoring internals
# ---------------------------------------------------------------------------

def _load_structured_resume(repo, resume_fingerprint: str, should_re_extract: bool):
    """Load structured resume from database."""
    if not should_re_extract:
        return repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)
    return None


def _prepare_matcher_service(ctx, repo, matching_config):
    """Create and configure matcher service."""
    scorer_config = getattr(matching_config, "scorer", None)
    semantic_fit_config = getattr(scorer_config, "semantic_fit", None)
    recall_top_k = getattr(semantic_fit_config, "recall_top_k", 5)
    cross_encoder_provider = _resolve_evidence_rerank_provider(semantic_fit_config)
    return MatcherService(
        resume_profiler=ResumeProfiler(
            ai_service=ctx.ai_service,
            store=JobRepositoryAdapter(repo),
        ),
        config=matching_config.matcher,
        requirement_recall_top_k=recall_top_k,
        cross_encoder_provider=cross_encoder_provider,
    )


def _resolve_evidence_rerank_provider(semantic_fit_config):
    """Return the shared local cross-encoder provider when evidence rerank is enabled.

    Shares one loaded model with the scorer via `get_shared_local_cross_encoder_provider`
    so we don't pay ~2 GB of BGE reranker weights twice.
    """
    if not getattr(semantic_fit_config, "evidence_rerank_enabled", False):
        return None
    local_cfg = getattr(getattr(semantic_fit_config, "cross_encoder", None), "local", None)
    if local_cfg is None or not getattr(local_cfg, "enabled", False):
        return None
    return get_shared_local_cross_encoder_provider(
        model_name=local_cfg.model_name,
        cache_path=local_cfg.model_cache_path,
        runtime=local_cfg.runtime,
        max_batch_size=local_cfg.max_batch_size,
        trust_remote_code=local_cfg.trust_remote_code,
    )


def _get_pre_extracted_resume(structured_resume, should_re_extract: bool):
    """Return a validated ResumeSchema if a stored resume is available."""
    if should_re_extract:
        logger.info("Resume re-extraction needed — will extract fresh")
        return None

    if not structured_resume or not structured_resume.extracted_data:
        return None

    try:
        pre_extracted = ResumeSchema.model_validate(structured_resume.extracted_data)
        logger.info(
            "Using stored structured resume (fingerprint: %s)",
            getattr(structured_resume, "resume_fingerprint", "") or '',
        )
        return pre_extracted
    except Exception as e:
        raise ValueError(f"Failed to parse stored ready resume: {e}") from e


def _run_vector_matching(
    matcher,
    repo,
    resume_data,
    stop_event,
    pre_extracted_resume,
    resume_fingerprint,
    owner_id=None,
    exclude_reusable_resume_fingerprint: Optional[str] = None,
):
    """Run vector-based job matching."""
    logger.info("=== MATCHING STEP 1: Running MatcherService (Vector Retrieval) ===")
    preliminary_matches = matcher.match_resume_two_stage(
        repo=repo,
        resume_data=resume_data,
        stop_event=stop_event,
        pre_extracted_resume=pre_extracted_resume,
        resume_fingerprint=resume_fingerprint,
        owner_id=owner_id,
        exclude_reusable_resume_fingerprint=exclude_reusable_resume_fingerprint,
    )
    return preliminary_matches


def _run_scorer_service(scorer, preliminary_matches, matching_config, stop_event):
    """Run rule-based scoring over every preliminary match.

    We deliberately widen the scoring pass to include every preliminary match
    (min_fit=0, no coverage floor, top_k=all). Floor/top_k gates are applied
    by the canonical selection stage, which now persists below-floor items as
    `selection_tier='excluded'` instead of dropping them. This keeps the
    scoring pass exhaustive without leaking the widened policy into the
    canonical selection contract.
    """
    logger.info("=== MATCHING STEP 2: Running ScorerService (Fit-first scoring) ===")
    result_policy = _resolve_result_policy(matching_config)
    if result_policy and preliminary_matches:
        widened_top_k = max(
            int(getattr(result_policy, "top_k", len(preliminary_matches)) or len(preliminary_matches)),
            len(preliminary_matches),
        )
        if hasattr(result_policy, "model_copy"):
            result_policy = result_policy.model_copy(
                update={
                    "min_fit": 0.0,
                    "min_jd_required_coverage": None,
                    "top_k": widened_top_k,
                }
            )
        else:
            result_policy.min_fit = 0.0
            result_policy.min_jd_required_coverage = None
            result_policy.top_k = widened_top_k

    return scorer.score_matches(
        preliminary_matches=preliminary_matches,
        result_policy=result_policy,
        match_type="requirements_only",
        stop_event=stop_event,
    )


def _prepare_selection_result(
    scored_matches,
    *,
    ctx: AppContext,
    owner_id: Optional[str],
    ranking_context: RankingContext,
    matching_config,
    resume_resolution_reason: str,
    task_id: Optional[str],
):
    """Apply the canonical selection contract and return committed-run snapshots.

    Uses the ORIGINAL result policy (min_fit/top_k) — not the widened one from
    `_run_scorer_service`. Matches that fall below the floor or beyond top_k
    are tiered as 'excluded' rather than dropped, so stats and the API can
    reconcile with what the user configured.
    """
    result_policy = _resolve_result_policy(matching_config)
    fit_floor_used = float(getattr(result_policy, "min_fit", 0.0) or 0.0)
    required_coverage_floor_used = getattr(
        result_policy,
        "min_jd_required_coverage",
        None,
    )
    top_k_used = int(getattr(result_policy, "top_k", len(scored_matches)) or len(scored_matches))
    notification_fit_floor_used = float(resolve_notification_fit_floor(ctx, owner_id=owner_id))
    two_tier_enabled = bool(
        getattr(matching_config, "two_tier_selection_enabled", True)
    )
    return select_matches(
        scored_matches,
        ranking_context=ranking_context,
        fit_floor_used=fit_floor_used,
        required_coverage_floor_used=required_coverage_floor_used,
        top_k_used=max(top_k_used, 0),
        notification_fit_floor_used=notification_fit_floor_used,
        resume_resolution_reason=resume_resolution_reason,
        task_id=task_id,
        two_tier_enabled=two_tier_enabled,
    )


def _resolve_pipeline_ai_service(ctx: AppContext) -> Optional[LLMProvider]:
    ai_service = getattr(ctx, "ai_service", None)
    return ai_service if isinstance(ai_service, LLMProvider) else None


def _resolve_preferences_config(ctx: AppContext) -> PreferencesConfig:
    preferences = getattr(getattr(ctx, "config", None), "preferences", None)
    if preferences is not None:
        return preferences
    return PreferencesConfig()


def _resolve_result_policy(matching_config):
    """Resolve the active result policy from the shared store with config fallback."""
    fallback_policy = getattr(matching_config, "result_policy", None)
    try:
        return get_result_policy_store().get_current_policy()
    except Exception:
        logger.warning("Falling back to configured result policy", exc_info=True)
        return fallback_policy


def _log_resume_preparation(structured_resume, resume_fingerprint: str) -> None:
    """Log whether a stored resume will be reused or re-extracted."""
    if structured_resume:
        logger.info("Loaded resume from database (fingerprint: %s)", resume_fingerprint)
        logger.info("Resume experience: %s years", structured_resume.total_experience_years)
        return
    logger.info("Will re-extract resume (fingerprint: %s)", resume_fingerprint)


def _prepare_matching_run(
    ctx: AppContext,
    repo,
    matching_config,
    resume_fingerprint: str,
    should_re_extract: bool,
):
    """Load the structured resume and matcher service for matching."""
    structured_resume = _load_structured_resume(repo, resume_fingerprint, should_re_extract)
    if not structured_resume and not should_re_extract:
        raise ValueError(
            "Resume not found in database for fingerprint: %s. "
            "Make sure Resume ETL has been run." % resume_fingerprint
        )

    _log_resume_preparation(structured_resume, resume_fingerprint)
    matcher = _prepare_matcher_service(ctx, repo, matching_config)
    return structured_resume, matcher


def _run_preliminary_matching(
    matcher,
    repo,
    resume_data: dict,
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]],
    structured_resume,
    should_re_extract: bool,
    resume_fingerprint: str,
    owner_id=None,
    exclude_reusable_resume_fingerprint: Optional[str] = None,
):
    """Run vector matching and log its completion timing."""
    step_start = time.time()
    if status_callback:
        status_callback("vector_matching")

    pre_extracted_resume = _get_pre_extracted_resume(structured_resume, should_re_extract)
    preliminary_matches = _run_vector_matching(
        matcher,
        repo,
        resume_data,
        stop_event,
        pre_extracted_resume,
        resume_fingerprint,
        owner_id=owner_id,
        exclude_reusable_resume_fingerprint=exclude_reusable_resume_fingerprint,
    )

    step_elapsed = time.time() - step_start
    logger.info(
        "MATCHING Step 1 completed: Matched against %d jobs in %.2fs",
        len(preliminary_matches), step_elapsed,
    )
    return preliminary_matches


def _tier_breakdown(
    item_snapshots: List[MatchSelectionItemSnapshot],
) -> tuple[Dict[str, int], Dict[str, int]]:
    tier_counts: Dict[str, int] = {}
    excluded_reasons: Dict[str, int] = {}
    for item in item_snapshots:
        tier = getattr(item, "selection_tier", "primary") or "primary"
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        if tier != "primary":
            reason = getattr(item, "excluded_reason", None) or "unspecified"
            excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
    return tier_counts, excluded_reasons


def _count_reranked_requirements(match_dtos: List[MatchResultDTO]) -> int:
    return sum(
        1
        for dto in match_dtos
        for req in (getattr(dto, "requirement_matches", []) or [])
        if getattr(req, "evidence_score", None) is not None
    )


def _degraded_reason_breakdown(match_dtos: List[MatchResultDTO]) -> Dict[str, int]:
    degraded: Dict[str, int] = {}
    for dto in match_dtos:
        components = getattr(dto, "fit_components", None)
        if not isinstance(components, dict):
            continue
        raw = components.get("semantic_fit_fallback_reason")
        if raw:
            key = str(raw)
            degraded[key] = degraded.get(key, 0) + 1
    return degraded


def _truncated_excluded_count(
    policy_snapshot: Optional[MatchSelectionPolicySnapshot],
) -> int:
    if policy_snapshot is None:
        return 0
    snapshot = getattr(policy_snapshot, "ranking_config_snapshot", None) or {}
    return int(snapshot.get("excluded_truncated_count", 0) or 0)


def _log_pipeline_run_summary(
    *,
    match_dtos: List[MatchResultDTO],
    item_snapshots: List[MatchSelectionItemSnapshot],
    preference_status: PreferenceStatus,
    policy_snapshot: Optional[MatchSelectionPolicySnapshot],
) -> None:
    """Emit one structured-log line per pipeline run.

    Captures the observability surface §I asks for without taking a metrics
    dependency: tier counts, evidence rerank coverage, scorer degraded reason,
    and preference-reranker status. `extra` keys land in structured-log sinks
    (JSON formatter / Loki) as queryable fields.
    """
    tier_counts, excluded_reasons = _tier_breakdown(item_snapshots)
    evidence_rerank_count = _count_reranked_requirements(match_dtos)
    degraded_reasons = _degraded_reason_breakdown(match_dtos)
    truncated_excluded = _truncated_excluded_count(policy_snapshot)

    logger.info(
        "pipeline.run_summary selected=%d tier_counts=%s excluded_reasons=%s "
        "evidence_rerank_scored=%d degraded_reasons=%s preference_applied=%s "
        "preference_reason=%s excluded_truncated=%d",
        len(match_dtos),
        tier_counts,
        excluded_reasons,
        evidence_rerank_count,
        degraded_reasons,
        preference_status.applied,
        preference_status.reason,
        truncated_excluded,
        extra={
            "event": "pipeline.run_summary",
            "selected_count": len(match_dtos),
            "tier_counts": tier_counts,
            "excluded_reasons": excluded_reasons,
            "evidence_rerank_scored": evidence_rerank_count,
            "degraded_reasons": degraded_reasons,
            "preference_applied": preference_status.applied,
            "preference_reason": preference_status.reason,
            "excluded_truncated": truncated_excluded,
        },
    )


def _log_match_results(match_dtos: List[MatchResultDTO]) -> None:
    """Log the top match summary for observability."""
    if not match_dtos:
        return

    logger.info("Top 5 Matches:")
    for i, dto in enumerate(match_dtos[:5], 1):
        pref = dto.preference_score
        logger.info(
            "  %d. %s @ %s: fit=%.1f, pref=%s",
            i, dto.job.title, dto.job.company,
            dto.fit_score,
            f"{pref:.4f}" if pref is not None else "None",
        )


# pylint: disable=too-many-branches
def _run_matching_and_scoring(
    ctx: AppContext,
    resume_data: dict,
    resume_fingerprint: str,
    should_re_extract: bool,
    matching_config,
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]],
    ranking_context: RankingContext | None = None,
    owner_id: Optional[str] = None,
    task_id: Optional[str] = None,
    resume_resolution_reason: str = "latest_ready_resume",
) -> PreparedSelectionResult:
    """Run the matching and scoring pipeline within a UOW context."""
    if ranking_context is None:
        ranking_context = _resolve_ranking_context()

    if status_callback:
        status_callback("loading_resume")

    preparation_start = time.time()
    logger.info("=== RESUME ETL STEP 1: Prepare Resume & Compare Fingerprint ===")

    match_dtos: List[MatchResultDTO] = []
    item_snapshots: List[MatchSelectionItemSnapshot] = []
    policy_snapshot: Optional[MatchSelectionPolicySnapshot] = None

    with job_uow() as repo:
        structured_resume, matcher = _prepare_matching_run(
            ctx,
            repo,
            matching_config,
            resume_fingerprint,
            should_re_extract,
        )
        resolved_owner_id = getattr(structured_resume, "owner_id", None) or owner_id
        candidate_preferences = load_candidate_preferences(repo, resolved_owner_id)

        step_elapsed = time.time() - preparation_start
        logger.info("RESUME ETL Step 1 completed: Resume prepared in %.2fs", step_elapsed)

        if stop_event.is_set():
            return _cancelled_prepared_selection_result(
                owner_id=resolved_owner_id,
                ranking_context=ranking_context,
                resume_resolution_reason=resume_resolution_reason,
                task_id=task_id,
            )

        recalculate_existing = bool(getattr(matching_config, "recalculate_existing", False))
        reusable_match_dtos: List[MatchResultDTO] = []
        reusable_match_ids_by_job_id: dict[str, str] = {}
        matching_backlog_remaining = 0
        if not recalculate_existing:
            reusable_match_dtos = _load_reusable_match_dtos(
                repo,
                resume_fingerprint,
                tenant_id=None,
            )
            reusable_match_ids_by_job_id = {
                str(dto.job.id): str(dto.job_match_id)
                for dto in reusable_match_dtos
                if dto.job_match_id
            }
            matching_backlog_remaining = repo.count_pending_matching_jobs(
                resume_fingerprint,
                tenant_id=None,
            )

        preliminary_matches = _run_preliminary_matching(
            matcher,
            repo,
            resume_data,
            stop_event,
            status_callback,
            structured_resume,
            should_re_extract,
            resume_fingerprint,
            owner_id=resolved_owner_id,
            exclude_reusable_resume_fingerprint=(
                None if recalculate_existing else resume_fingerprint
            ),
        )
        preliminary_matches = apply_candidate_preference_filters(
            preliminary_matches,
            candidate_preferences,
        )

        if stop_event.is_set():
            return _cancelled_prepared_selection_result(
                owner_id=resolved_owner_id,
                ranking_context=ranking_context,
                resume_resolution_reason=resume_resolution_reason,
                task_id=task_id,
            )

        scoring_start = time.time()

        if status_callback:
            status_callback("scoring")
        scorer = ScoringService(
            repo=repo,
            config=matching_config.scorer,
            ai_service=_resolve_pipeline_ai_service(ctx),
        )
        scored_matches = _run_scorer_service(
            scorer, preliminary_matches, matching_config, stop_event,
        )
        rerank_start = time.time()
        rerank_result = apply_preference_semantic_reranking(
            scored_matches,
            candidate_preferences,
            config=_resolve_preferences_config(ctx),
        )
        if isinstance(rerank_result, PreferenceRerankResult):
            scored_matches = rerank_result.matches
            preference_status = rerank_result.status
        else:
            scored_matches = rerank_result
            preference_status = PreferenceStatus(applied=False, reason="unknown")
        logger.info(
            "Preference reranking completed in %.2fs for %d matches (applied=%s reason=%s)",
            time.time() - rerank_start,
            len(scored_matches),
            preference_status.applied,
            preference_status.reason,
        )
        scored_match_dtos = _convert_matches_to_dtos(
            scored_matches,
            preference_status=preference_status,
        )
        selection_candidates = reusable_match_dtos + scored_match_dtos
        selection_result = _prepare_selection_result(
            selection_candidates,
            ctx=ctx,
            owner_id=resolved_owner_id,
            ranking_context=ranking_context,
            matching_config=matching_config,
            resume_resolution_reason=resume_resolution_reason,
            task_id=task_id,
        )
        policy_snapshot = selection_result.policy_snapshot
        item_snapshots = selection_result.item_snapshots

        match_dtos = _selection_matches_to_dtos(
            selection_result.selected_matches,
            fallback_dtos=scored_match_dtos,
        )
        persist_matches = _matches_for_selection_persistence(
            scored_match_dtos,
            item_snapshots,
            selected_matches=selection_result.selected_matches,
        )
        persist_match_dtos = (
            persist_matches
            if persist_matches is not selection_result.selected_matches
            else scored_match_dtos
        )

    step_elapsed = time.time() - scoring_start
    logger.info("MATCHING Step 2 completed: Scored %d matches in %.2fs", len(match_dtos), step_elapsed)
    _log_match_results(match_dtos)
    _log_pipeline_run_summary(
        match_dtos=match_dtos,
        item_snapshots=item_snapshots,
        preference_status=preference_status,
        policy_snapshot=policy_snapshot,
    )
    if policy_snapshot is None:
        raise RuntimeError("Selection policy snapshot was not created")
    return PreparedSelectionResult(
        match_dtos=match_dtos,
        item_snapshots=item_snapshots,
        policy_snapshot=policy_snapshot,
        owner_id=(str(resolved_owner_id) if resolved_owner_id is not None else None),
        persist_match_dtos=persist_match_dtos,
        cached_job_match_ids_by_job_id=reusable_match_ids_by_job_id,
        matching_page_size=int(
            getattr(getattr(matching_config, "matcher", None), "batch_size", 0) or 0
        ),
        matching_backlog_remaining=matching_backlog_remaining,
        reusable_match_count=len(reusable_match_dtos),
    )


# ---------------------------------------------------------------------------
# DTO conversion
# ---------------------------------------------------------------------------

def _matches_for_selection_persistence(
    scored_matches,
    item_snapshots: List[MatchSelectionItemSnapshot],
    *,
    selected_matches,
):
    """Return every scored match needed by persisted selection snapshots."""
    if not item_snapshots:
        return selected_matches

    snapshot_job_ids = {str(item.job_id) for item in item_snapshots}
    return [
        match
        for match in scored_matches
        if str(match.job.id) in snapshot_job_ids
    ]


def _selection_matches_to_dtos(selected_matches, *, fallback_dtos: List[MatchResultDTO]) -> List[MatchResultDTO]:
    selected = list(selected_matches or [])
    if all(hasattr(match, "job") and hasattr(match, "fit_score") for match in selected):
        return selected
    selected_job_ids = {
        str(match.job.id)
        for match in selected
        if hasattr(match, "job") and hasattr(match.job, "id")
    }
    if selected_job_ids:
        return [
            dto
            for dto in fallback_dtos
            if str(dto.job.id) in selected_job_ids
        ]
    return fallback_dtos


def _number(value, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(value)


def _build_evidence_dto(evidence) -> Optional[JobEvidenceDTO]:
    if evidence is None:
        return None
    return JobEvidenceDTO(
        text=evidence.text,
        source_section=evidence.source_section,
        tags=evidence.tags,
    )


def _matched_req_to_dto(req) -> RequirementMatchDTO:
    return RequirementMatchDTO(
        requirement=JobRequirementDTO(
            id=str(req.requirement.id),
            req_type=req.requirement.req_type,
        ),
        evidence=_build_evidence_dto(req.evidence),
        similarity=req.similarity,
        is_covered=req.is_covered,
        evidence_score=getattr(req, "evidence_score", None),
    )


def _missing_req_to_dto(req) -> RequirementMatchDTO:
    return RequirementMatchDTO(
        requirement=JobRequirementDTO(
            id=str(req.requirement.id),
            req_type=req.requirement.req_type,
        ),
        similarity=req.similarity,
        is_covered=False,
        evidence_score=getattr(req, "evidence_score", None),
    )


def _persisted_requirement_to_dto(req) -> RequirementMatchDTO:
    evidence = None
    if getattr(req, "evidence_text", None):
        evidence = JobEvidenceDTO(
            text=req.evidence_text,
            source_section=req.evidence_section,
            tags=req.evidence_tags or {},
        )
    return RequirementMatchDTO(
        requirement=JobRequirementDTO(
            id=str(req.requirement.id),
            req_type=req.req_type,
        ),
        evidence=evidence,
        similarity=_number(req.similarity_score),
        is_covered=bool(req.is_covered),
        evidence_score=req.evidence_score,
    )


def _persisted_match_to_dto(match) -> MatchResultDTO:
    requirements = [
        _persisted_requirement_to_dto(req)
        for req in (getattr(match, "requirement_matches", []) or [])
    ]
    return MatchResultDTO(
        job=JobMatchDTO(
            id=str(match.job_post.id),
            title=match.job_post.title,
            company=match.job_post.company,
            location_text=match.job_post.location_text,
            is_remote=match.job_post.is_remote,
            content_hash=match.job_post.content_hash,
        ),
        fit_score=_number(match.fit_score),
        preference_score=(
            None if match.preference_score is None else float(match.preference_score)
        ),
        job_similarity=_number(match.job_similarity),
        jd_required_coverage=_number(match.required_coverage),
        jd_preferred_requirement_coverage=_number(match.preferred_requirement_coverage),
        requirement_matches=[req for req in requirements if req.is_covered],
        missing_requirements=[req for req in requirements if not req.is_covered],
        resume_fingerprint=match.resume_fingerprint,
        fit_components=match.fit_components or {},
        preference_components=match.preference_components or {},
        ranking_snapshot=match.ranking_snapshot or {},
        base_score=_number(match.base_score),
        penalties=_number(match.penalties),
        penalty_details=penalty_details_from_orm(
            match.penalty_details,
            total_penalties=_number(match.penalties),
        ),
        match_type=match.match_type or "requirements_only",
        job_match_id=str(match.id),
    )


def _load_reusable_match_dtos(repo, resume_fingerprint: str, tenant_id=None) -> List[MatchResultDTO]:
    return [
        _persisted_match_to_dto(match)
        for match in repo.get_reusable_matches_for_resume(
            resume_fingerprint,
            tenant_id=tenant_id,
        )
    ]


def _scoring_degraded_reason(match) -> Optional[str]:
    fit_components = getattr(match, "fit_components", {}) or {}
    if not isinstance(fit_components, dict):
        return None
    raw = fit_components.get("semantic_fit_fallback_reason")
    if not raw:
        return None
    text = str(raw).lower()
    if "remote" in text:
        return "remote_unavailable"
    if "local" in text:
        return "local_unavailable"
    if "disabled" in text:
        return "provider_disabled"
    return "degraded"


def _ranking_snapshot_from_match(match, preference_status: PreferenceStatus | None = None) -> dict:
    explanation = getattr(match, "ranking_explanation", None)
    snapshot = asdict(explanation) if explanation is not None else {}
    degraded_reason = _scoring_degraded_reason(match)
    if degraded_reason:
        snapshot["scoring"] = {"degraded_reason": degraded_reason}
    if preference_status is not None:
        snapshot["preference_status"] = preference_status.to_dict()
    return snapshot


def _convert_matches_to_dtos(
    scored_matches,
    *,
    preference_status: PreferenceStatus | None = None,
) -> List[MatchResultDTO]:
    """Convert ORM match objects to DTOs.

    Uses direct attribute access instead of getattr-with-defaults so that
    ORM schema drift (renames, removals) surfaces immediately as AttributeError
    rather than silently producing placeholder values.
    """
    match_dtos = []
    for match in scored_matches:
        dto = MatchResultDTO(
            job=JobMatchDTO(
                id=str(match.job.id),
                title=match.job.title,
                company=match.job.company,
                location_text=match.job.location_text,
                is_remote=match.job.is_remote,
                content_hash=match.job.content_hash,
            ),
            fit_score=match.fit_score or 0.0,
            preference_score=match.preference_score,
            job_similarity=match.job_similarity or 0.0,
            jd_required_coverage=match.jd_required_coverage,
            jd_preferred_requirement_coverage=match.jd_preferred_requirement_coverage,
            requirement_matches=[_matched_req_to_dto(r) for r in match.matched_requirements],
            missing_requirements=[_missing_req_to_dto(r) for r in match.missing_requirements],
            resume_fingerprint=match.resume_fingerprint,
            fit_components=match.fit_components,
            preference_components=getattr(match, "preference_components", {}) or {},
            ranking_snapshot=_ranking_snapshot_from_match(match, preference_status),
            base_score=match.base_score,
            penalties=match.penalties,
            penalty_details=penalty_details_from_orm(
                match.penalty_details,
                total_penalties=match.penalties,
            ),
            match_type=match.match_type,
            job_match_id=getattr(match, "job_match_id", None),
        )
        match_dtos.append(dto)
    return match_dtos


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _save_matches_batch(
    scored_match_dtos: List[MatchResultDTO],
    resume_fingerprint: str,
    matching_config,
) -> SaveMatchesBatchResult:
    """Save matches to database with per-match transactions."""
    saved_count = 0
    failed_count = 0
    active_job_ids: set[str] = set()
    job_match_ids_by_job_id: dict[str, str] = {}
    for dto in scored_match_dtos:
        try:
            with job_uow() as repo:
                existing = repo.get_existing_match(dto.job.id, resume_fingerprint)

                if existing and existing.status == 'active':
                    if existing.job_content_hash != dto.job.content_hash:
                        existing.status = 'stale'
                        existing.invalidated_reason = "Job content updated"
                        logger.info("Invalidated match for job %s due to content change", dto.job.id)
                        match_record = save_match_to_db(
                            scored_match=dto,
                            repo=repo,
                            is_stale_replacement=True,
                        )
                        saved_count += 1
                        active_job_ids.add(str(dto.job.id))
                        job_match_ids_by_job_id[str(dto.job.id)] = str(match_record.id)
                        continue

                    if not matching_config.recalculate_existing:
                        logger.debug("Refreshing existing active match snapshot for job %s", dto.job.id)
                        existing.job_similarity = dto.job_similarity
                        existing.fit_score = dto.fit_score
                        existing.preference_score = dto.preference_score
                        existing.fit_components = dto.fit_components
                        existing.preference_components = dto.preference_components
                        existing.ranking_snapshot = dto.ranking_snapshot
                        existing.base_score = dto.base_score
                        existing.penalties = dto.penalties
                        existing.penalty_details = dto.penalty_details
                        existing.required_coverage = dto.jd_required_coverage
                        existing.preferred_requirement_coverage = (
                            dto.jd_preferred_requirement_coverage
                        )
                        existing.total_requirements = (
                            len(dto.requirement_matches) + len(dto.missing_requirements)
                        )
                        existing.matched_requirements_count = len(dto.requirement_matches)
                        existing.match_type = dto.match_type
                        existing.job_content_hash = dto.job.content_hash
                        existing.calculated_at = datetime.now(timezone.utc)
                        existing.status = 'active'
                        repo.db.flush()
                        saved_count += 1
                        active_job_ids.add(str(dto.job.id))
                        job_match_ids_by_job_id[str(dto.job.id)] = str(existing.id)
                        continue

                match_record = save_match_to_db(
                    scored_match=dto,
                    repo=repo,
                    is_stale_replacement=False,
                )
                saved_count += 1
                active_job_ids.add(str(dto.job.id))
                job_match_ids_by_job_id[str(dto.job.id)] = str(match_record.id)
        except Exception:
            logger.exception("Failed saving match job_id=%s", dto.job.id)
            failed_count += 1

    return SaveMatchesBatchResult(
        saved_count=saved_count,
        failed_count=failed_count,
        active_job_ids=frozenset(active_job_ids),
        job_match_ids_by_job_id=job_match_ids_by_job_id,
    )


def _publish_match_selection_run(
    *,
    owner_id: Optional[str],
    resume_fingerprint: str,
    task_id: Optional[str],
    prepared_selection: PreparedSelectionResult,
    save_batch_result: SaveMatchesBatchResult,
    job_match_ids_by_job_id: Optional[dict[str, str]] = None,
) -> Optional[str]:
    """Publish the committed selection run that defines canonical membership."""
    with job_uow() as repo:
        selection_run = repo.match_selection.publish_selection_run(
            owner_id=owner_id or SYSTEM_OWNER_ID,
            resume_fingerprint=resume_fingerprint,
            policy_snapshot=prepared_selection.policy_snapshot,
            item_snapshots=prepared_selection.item_snapshots,
            job_match_ids_by_job_id=(
                job_match_ids_by_job_id
                if job_match_ids_by_job_id is not None
                else save_batch_result.job_match_ids_by_job_id
            ),
            task_id=task_id,
        )
        return str(selection_run.id)


def _refresh_resume_match_set(
    resume_fingerprint: str,
    *,
    active_job_ids: frozenset[str] = frozenset(),
) -> int:
    """Mark prior active matches stale after a successful refreshed save batch."""
    with job_uow() as repo:
        invalidated_count = repo.invalidate_matches_for_resume_except(
            resume_fingerprint,
            active_job_ids=active_job_ids,
            reason="Matching pipeline rerun refreshed active match set",
        )

    if invalidated_count:
        logger.info(
            "Marked %d prior matches stale after refreshing active results for %s",
            invalidated_count,
            resume_fingerprint[:16],
        )
    return invalidated_count
