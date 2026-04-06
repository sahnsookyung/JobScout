"""Matching pipeline for the scorer-matcher service.

This module contains the core matching pipeline logic: loading a resume,
running vector matching and scoring, saving results, and dispatching
notifications.
"""

import time
import logging
import threading
from typing import List, Optional, Dict, Any, Callable

from dataclasses import dataclass

from core.app_context import AppContext
from core.config_loader import PreferencesConfig
from core.policy import get_result_policy_store
from core.matcher import (
    MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO,
    RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm,
)
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from core.llm.interfaces import LLMProvider
from core.llm.schema_models import ResumeSchema
from etl.resume import ResumeProfiler
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow
from notification.orchestrator import send_notifications
from services.scorer_matcher.candidate_preferences import (
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
    except Exception as e:
        logger.error("Error loading resume from DB: %s", e)
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

        # Step 2: Run matching and scoring
        match_dtos = _run_matching_and_scoring(
            ctx, resume_data, resume_fingerprint, should_re_extract,
            matching_config, stop_event, status_callback, owner_id=owner_id,
        )
        matching_result = _result_after_matching(match_dtos, stop_event)
        if matching_result:
            return matching_result

        # Step 4: Save matches
        if status_callback:
            status_callback("saving_results")
        save_batch_result = _save_matches_batch(match_dtos, resume_fingerprint, matching_config)
        saved_count = save_batch_result.saved_count
        if save_batch_result.failed_count == 0:
            _refresh_resume_match_set(
                resume_fingerprint,
                active_job_ids=save_batch_result.active_job_ids,
            )
        else:
            logger.warning(
                "Skipping active-match refresh for %s because %d match saves failed",
                resume_fingerprint[:16],
                save_batch_result.failed_count,
            )

        save_result = _result_after_saving(
            match_dtos,
            saved_count,
            stop_event,
            pipeline_start_time,
        )
        if save_result:
            return save_result

        # Step 5: Send notifications
        notified_count = 0
        if ctx.notification_service and not stop_event.is_set():
            if status_callback:
                status_callback("notifying")
            notified_count = send_notifications(
                ctx,
                match_dtos,
                saved_count,
                resume_fingerprint,
                stop_event,
                owner_id=owner_id,
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
        logger.error("Error in matching pipeline: %s: %s", type(e).__name__, e, exc_info=True)
        execution_time = time.time() - pipeline_start_time
        return MatchingPipelineResult(
            success=False, matches_count=0, saved_count=0, notified_count=0,
            error=str(e), execution_time=execution_time,
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
    return MatcherService(
        resume_profiler=ResumeProfiler(
            ai_service=ctx.ai_service,
            store=JobRepositoryAdapter(repo),
        ),
        config=matching_config.matcher,
        requirement_recall_top_k=recall_top_k,
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
    )
    return preliminary_matches


def _run_scorer_service(scorer, preliminary_matches, matching_config, stop_event):
    """Run rule-based scoring."""
    logger.info("=== MATCHING STEP 2: Running ScorerService (Fit-first scoring) ===")
    result_policy = _resolve_result_policy(matching_config)
    if result_policy and preliminary_matches:
        widened_top_k = max(
            int(getattr(result_policy, "top_k", len(preliminary_matches)) or len(preliminary_matches)),
            len(preliminary_matches),
        )
        if hasattr(result_policy, "model_copy"):
            result_policy = result_policy.model_copy(update={"top_k": widened_top_k})
        elif hasattr(result_policy, "top_k"):
            try:
                result_policy.top_k = widened_top_k
            except Exception:
                # Mutation failed — clear the policy so scorer returns all matches;
                # _apply_final_result_policy will re-apply the original limit afterward.
                logger.warning("Could not widen result policy top_k prior to preference reranking; passing all matches to reranker", exc_info=True)
                result_policy = None

    return scorer.score_matches(
        preliminary_matches=preliminary_matches,
        result_policy=result_policy,
        match_type="requirements_only",
        stop_event=stop_event,
    )


def _apply_final_result_policy(scored_matches, matching_config):
    """Apply the final result truncation after semantic preference reranking."""
    result_policy = _resolve_result_policy(matching_config)
    if not result_policy:
        return scored_matches
    top_k = int(getattr(result_policy, "top_k", len(scored_matches)) or len(scored_matches))
    if top_k <= 0:
        return []
    return scored_matches[:top_k]


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
    )

    step_elapsed = time.time() - step_start
    logger.info(
        "MATCHING Step 1 completed: Matched against %d jobs in %.2fs",
        len(preliminary_matches), step_elapsed,
    )
    return preliminary_matches


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
    owner_id: Optional[str] = None,
) -> List[MatchResultDTO]:
    """Run the matching and scoring pipeline within a UOW context."""
    if status_callback:
        status_callback("loading_resume")

    preparation_start = time.time()
    logger.info("=== RESUME ETL STEP 1: Prepare Resume & Compare Fingerprint ===")

    match_dtos = []

    with job_uow() as repo:
        structured_resume, matcher = _prepare_matching_run(
            ctx,
            repo,
            matching_config,
            resume_fingerprint,
            should_re_extract,
        )
        candidate_preferences = load_candidate_preferences(repo, owner_id)

        step_elapsed = time.time() - preparation_start
        logger.info("RESUME ETL Step 1 completed: Resume prepared in %.2fs", step_elapsed)

        if stop_event.is_set():
            return []

        preliminary_matches = _run_preliminary_matching(
            matcher,
            repo,
            resume_data,
            stop_event,
            status_callback,
            structured_resume,
            should_re_extract,
            resume_fingerprint,
            owner_id=getattr(structured_resume, "owner_id", None) or owner_id,
        )
        preliminary_matches = apply_candidate_preference_filters(
            preliminary_matches,
            candidate_preferences,
        )

        if stop_event.is_set():
            return []

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
        scored_matches = apply_preference_semantic_reranking(
            scored_matches,
            candidate_preferences,
            config=_resolve_preferences_config(ctx),
        )
        logger.info(
            "Preference reranking completed in %.2fs for %d matches",
            time.time() - rerank_start,
            len(scored_matches),
        )
        scored_matches = _apply_final_result_policy(scored_matches, matching_config)

        match_dtos = _convert_matches_to_dtos(scored_matches)

    step_elapsed = time.time() - scoring_start
    logger.info("MATCHING Step 2 completed: Scored %d matches in %.2fs", len(match_dtos), step_elapsed)
    _log_match_results(match_dtos)
    return match_dtos


# ---------------------------------------------------------------------------
# DTO conversion
# ---------------------------------------------------------------------------

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
    )


def _missing_req_to_dto(req) -> RequirementMatchDTO:
    return RequirementMatchDTO(
        requirement=JobRequirementDTO(
            id=str(req.requirement.id),
            req_type=req.requirement.req_type,
        ),
        similarity=req.similarity,
        is_covered=False,
    )


def _convert_matches_to_dtos(scored_matches) -> List[MatchResultDTO]:
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
            jd_preferences_coverage=match.jd_preferences_coverage,
            requirement_matches=[_matched_req_to_dto(r) for r in match.matched_requirements],
            missing_requirements=[_missing_req_to_dto(r) for r in match.missing_requirements],
            resume_fingerprint=match.resume_fingerprint,
            fit_components=match.fit_components,
            base_score=match.base_score,
            penalties=match.penalties,
            penalty_details=penalty_details_from_orm(
                match.penalty_details,
                total_penalties=match.penalties,
            ),
            match_type=match.match_type,
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
    for dto in scored_match_dtos:
        try:
            with job_uow() as repo:
                existing = repo.get_existing_match(dto.job.id, resume_fingerprint)

                if existing and existing.status == 'active':
                    if existing.job_content_hash != dto.job.content_hash:
                        existing.status = 'stale'
                        existing.invalidated_reason = "Job content updated"
                        logger.info("Invalidated match for job %s due to content change", dto.job.id)
                        save_match_to_db(scored_match=dto, repo=repo, is_stale_replacement=True)
                        saved_count += 1
                        active_job_ids.add(str(dto.job.id))
                        continue

                    if not matching_config.recalculate_existing:
                        logger.debug("Skipping existing match for job %s", dto.job.id)
                        active_job_ids.add(str(dto.job.id))
                        continue

                save_match_to_db(scored_match=dto, repo=repo, is_stale_replacement=False)
                saved_count += 1
                active_job_ids.add(str(dto.job.id))
        except Exception:
            logger.exception("Failed saving match job_id=%s", dto.job.id)
            failed_count += 1

    return SaveMatchesBatchResult(
        saved_count=saved_count,
        failed_count=failed_count,
        active_job_ids=frozenset(active_job_ids),
    )


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
