"""Shared matching pipeline runner module.

This module contains the core matching pipeline logic used by the
scorer-matcher service and the web-triggered matching flow.
"""

import os
import time
import logging
import threading
from typing import List, Optional, Dict, Any, Callable, Tuple
from uuid import UUID

from dataclasses import dataclass

from core.app_context import AppContext
from core.policy import get_result_policy_store
from core.matcher import (
    MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO,
    RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm,
)
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from core.llm.schema_models import ResumeSchema
from etl.resume import ResumeProfiler, ResumeParser, load_resume_with_parser
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow
from notification.message_builder import NotificationMessageBuilder


logger = logging.getLogger(__name__)


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



def load_user_wants_data(wants_file_path: str) -> List[str]:
    """Load user wants from a file. Each line is a separate want."""
    logger.info("Loading user wants from %s", wants_file_path)
    try:
        with open(wants_file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.warning("User wants file not found: %s", wants_file_path)
        return []
    except Exception as e:
        logger.error("Error reading user wants file: %s", e)
        return []


def _load_resume_from_db(resume_fingerprint: str) -> Optional[dict]:
    """Load resume extracted_data from database using fingerprint."""
    logger.info("Loading resume from database: %s...", resume_fingerprint[:16])
    try:
        # FIX: removed redundant local re-import; module-level job_uow is used
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


def _get_configured_resume_file(ctx: AppContext) -> Optional[str]:
    """Resolve the configured resume file path, if one exists."""
    etl_config = getattr(ctx.config, "etl", None)
    resume_file = None
    if etl_config and getattr(etl_config, "resume", None):
        resume_file = etl_config.resume.resume_file
    elif etl_config and getattr(etl_config, "resume_file", None):
        resume_file = etl_config.resume_file

    if not resume_file:
        return None
    if not os.path.isabs(resume_file):
        return os.path.join(os.getcwd(), resume_file)
    return resume_file


def _load_configured_resume_fallback(
    ctx: AppContext,
) -> tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    """Configured resume fallback is no longer supported."""
    del ctx
    return None, None, "No ready resume found. Upload and process a resume first."


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


def _requested_resume_not_found_result(resume_fingerprint: str) -> MatchingPipelineResult:
    """Build the explicit-fingerprint not found result."""
    return _error_result(
        "Resume not found in DB for fingerprint: %s..." % resume_fingerprint[:16],
    )


def _load_requested_resume(
    resume_fingerprint: str,
) -> tuple[Optional[Dict[str, Any]], bool, Optional[MatchingPipelineResult]]:
    """Load resume data for an explicitly requested fingerprint."""
    resume_data = _load_resume_from_db(resume_fingerprint)
    if not resume_data:
        return None, False, _requested_resume_not_found_result(resume_fingerprint)

    logger.info("Loaded resume from database (fingerprint: %s...)", resume_fingerprint[:16])
    return resume_data, False, None


def _missing_structured_resume_result(resume_fingerprint: str) -> MatchingPipelineResult:
    """Build the missing structured resume result."""
    return _error_result(
        f"Ready resume {resume_fingerprint[:16]}... is missing structured data",
    )


def _processing_resume_result(processing_status: str) -> MatchingPipelineResult:
    """Build the processing-in-progress result."""
    return _error_result(
        "Latest resume upload is still processing "
        f"({processing_status}).",
    )


def _no_ready_resume_result(fallback_error: Optional[str]) -> MatchingPipelineResult:
    """Build the no-ready-resume result."""
    return _error_result(
        fallback_error or "No ready resume found. Upload and process a resume first.",
    )


def _load_latest_ready_resume(
    ctx: AppContext,
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
                return None, None, False, _missing_structured_resume_result(resume_fingerprint)
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
            _processing_resume_result(latest_processing_state.processing_status),
        )

    resume_fingerprint, resume_data, fallback_error = _load_configured_resume_fallback(ctx)
    if not resume_fingerprint:
        return None, None, False, _no_ready_resume_result(fallback_error)

    return resume_fingerprint, resume_data, False, None


def _load_pipeline_resume(
    ctx: AppContext,
    resume_fingerprint: Optional[str],
) -> tuple[Optional[Dict[str, Any]], Optional[str], bool, Optional[MatchingPipelineResult]]:
    """Load the resume data used by the matching pipeline."""
    if resume_fingerprint:
        resume_data, should_re_extract, error_result = _load_requested_resume(resume_fingerprint)
        return resume_data, resume_fingerprint, should_re_extract, error_result

    latest_fingerprint, resume_data, should_re_extract, error_result = _load_latest_ready_resume(ctx)
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
            this fingerprint instead of reading from the configured file path.
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
            ctx,
            resume_fingerprint,
        )
        if error_result:
            return error_result

        # Step 2: Load user wants embeddings
        user_want_embeddings = _load_user_wants_embeddings(matching_config, ctx.ai_service)

        # Step 3: Run matching and scoring
        match_dtos = _run_matching_and_scoring(
            ctx, resume_data, resume_fingerprint, should_re_extract,
            matching_config, user_want_embeddings, stop_event, status_callback,
        )
        matching_result = _result_after_matching(match_dtos, stop_event)
        if matching_result:
            return matching_result

        # Step 4: Save matches
        if status_callback:
            status_callback("saving_results")
        saved_count = _save_matches_batch(match_dtos, resume_fingerprint, matching_config)

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
            notified_count = _send_notifications(
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
# File / fingerprint helpers
# ---------------------------------------------------------------------------

def _load_resume_file(etl_config) -> Tuple[Optional[str], Optional[dict]]:
    """Load resume file from the configured path. Returns (filepath, data)."""
    if etl_config and getattr(etl_config, 'resume', None):
        resume_file = etl_config.resume.resume_file
    elif etl_config and getattr(etl_config, 'resume_file', None):
        resume_file = etl_config.resume_file
    else:
        resume_file = None

    if not resume_file:
        logger.info("No resume file configured in ETL config — skipping file-based matching")
        return None, None

    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    if not os.path.exists(resume_file):
        logger.error("Resume file not found: %s", resume_file)
        return None, None

    resume_data = load_resume_with_parser(resume_file)
    if not resume_data:
        logger.error("Failed to load resume data")
        return None, None

    return resume_file, resume_data


def _determine_resume_extraction(
    resume_file: str,
    etl_config,
) -> Tuple[str, bool]:
    """Determine if resume should be re-extracted based on fingerprint comparison.

    Returns:
        Tuple of (resume_fingerprint, should_re_extract).
    """
    from database.models import generate_file_fingerprint

    with open(resume_file, 'rb') as f:
        current_fingerprint = generate_file_fingerprint(f.read())
    logger.info("Current resume fingerprint: %s", current_fingerprint)

    with job_uow() as repo:
        stored_fingerprint = repo.resume.get_latest_stored_resume_fingerprint()

    force_re_extraction = (
        etl_config.resume.force_re_extraction
        if etl_config and etl_config.resume and etl_config.resume.force_re_extraction
        else False
    )

    if force_re_extraction or not stored_fingerprint or current_fingerprint != stored_fingerprint:
        should_re_extract = True
        resume_fingerprint = current_fingerprint
        if not stored_fingerprint:
            logger.info("No stored resume found — will extract")
        elif current_fingerprint != stored_fingerprint:
            logger.info(
                "Resume file changed (stored: %s, current: %s) — will re-extract",
                stored_fingerprint, current_fingerprint,
            )
        else:
            logger.info("Force re-extraction enabled in config — will re-extract")
    else:
        should_re_extract = False
        resume_fingerprint = stored_fingerprint
        logger.info("Resume unchanged (fingerprint: %s) — using stored data", current_fingerprint)

    return resume_fingerprint, should_re_extract


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------

def _load_user_wants_embeddings(matching_config, ai_service) -> List[List[float]]:
    """Load user wants from file and generate embeddings.

    Embeddings are generated in a single batched call when possible,
    falling back to individual calls with per-item error isolation.
    """
    if not matching_config.user_wants_file:
        return []

    wants_file = matching_config.user_wants_file
    if not os.path.isabs(wants_file):
        wants_file = os.path.join(os.getcwd(), wants_file)
    if not os.path.exists(wants_file):
        logger.warning("User wants file not found: %s", wants_file)
        return []

    user_wants = load_user_wants_data(wants_file)
    if not user_wants:
        return []

    logger.info("Loaded %d user wants from %s", len(user_wants), matching_config.user_wants_file)

    # FIX: prefer a single batched call; fall back to per-item with error isolation
    if hasattr(ai_service, 'generate_embeddings'):
        try:
            return ai_service.generate_embeddings(user_wants)
        except Exception as e:
            logger.warning("Batch embedding failed (%s), falling back to per-item", e)

    embeddings = []
    for want_text in user_wants:
        # FIX: isolate failures so one bad entry doesn't abort the whole pipeline
        try:
            embeddings.append(ai_service.generate_embedding(want_text))
        except Exception as e:
            logger.warning("Failed to embed want '%s': %s — skipping", want_text[:60], e)
    return embeddings


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
    return MatcherService(
        resume_profiler=ResumeProfiler(
            ai_service=ctx.ai_service,
            store=JobRepositoryAdapter(repo),
        ),
        config=matching_config.matcher,
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


def _run_vector_matching(matcher, repo, resume_data, stop_event, pre_extracted_resume, resume_fingerprint):
    """Run vector-based job matching."""
    logger.info("=== MATCHING STEP 1: Running MatcherService (Vector Retrieval) ===")
    preliminary_matches = matcher.match_resume_two_stage(
        repo=repo,
        resume_data=resume_data,
        stop_event=stop_event,
        pre_extracted_resume=pre_extracted_resume,
        resume_fingerprint=resume_fingerprint,
    )
    # FIX: removed duplicate "Step 1 completed" log (caller logs with timing)
    return preliminary_matches


def _run_scorer_service(scorer, preliminary_matches, matching_config, user_want_embeddings, job_facet_embeddings_map, stop_event):
    """Run rule-based scoring."""
    logger.info("=== MATCHING STEP 2: Running ScorerService (Rule-based Scoring) ===")
    result_policy = _resolve_result_policy(matching_config)

    common_kwargs = {
        "preliminary_matches": preliminary_matches,
        "result_policy": result_policy,
        "match_type": "requirements_only",
        "stop_event": stop_event,
    }

    if user_want_embeddings:
        logger.info("Using Fit/Want scoring with 'user wants' embeddings")
        scored_matches = scorer.score_matches(
            **common_kwargs,
            user_want_embeddings=user_want_embeddings,
            job_facet_embeddings_map=job_facet_embeddings_map,
        )
    else:
        logger.info("Using Fit-only scoring")
        scored_matches = scorer.score_matches(**common_kwargs)

    return scored_matches


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
):
    """Run vector matching and log its completion timing."""
    step_start = time.time()
    if status_callback:
        status_callback("vector_matching")

    pre_extracted_resume = _get_pre_extracted_resume(structured_resume, should_re_extract)
    preliminary_matches = _run_vector_matching(
        matcher, repo, resume_data, stop_event, pre_extracted_resume, resume_fingerprint,
    )

    step_elapsed = time.time() - step_start
    logger.info(
        "MATCHING Step 1 completed: Matched against %d jobs in %.2fs",
        len(preliminary_matches), step_elapsed,
    )
    return preliminary_matches


def _build_job_facet_embeddings_map(repo, preliminary_matches) -> Dict[str, Any]:
    """Load facet embeddings once per unique job id."""
    job_facet_embeddings_map: Dict[str, Any] = {}
    for preliminary in preliminary_matches:
        job_id = str(preliminary.job.id)
        if job_id in job_facet_embeddings_map:
            continue
        job_facet_embeddings_map[job_id] = repo.get_job_facet_embeddings(preliminary.job.id)
    return job_facet_embeddings_map


def _log_match_results(match_dtos: List[MatchResultDTO]) -> None:
    """Log the top match summary for observability."""
    if not match_dtos:
        return

    logger.info("Top 5 Matches:")
    for i, dto in enumerate(match_dtos[:5], 1):
        logger.info(
            "  %d. %s @ %s: overall=%.1f/100 (fit=%.1f, want=%.1f)",
            i, dto.job.title, dto.job.company,
            dto.overall_score, dto.fit_score, dto.want_score,
        )


# pylint: disable=too-many-branches
def _run_matching_and_scoring(
    ctx: AppContext,
    resume_data: dict,
    resume_fingerprint: str,
    should_re_extract: bool,
    matching_config,
    user_want_embeddings: List[List[float]],
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]],
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

        step_elapsed = time.time() - preparation_start
        logger.info("RESUME ETL Step 1 completed: Resume prepared in %.2fs", step_elapsed)

        if _should_terminate_early(stop_event, status_callback):
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
        )

        if _should_terminate_early(stop_event, status_callback):
            return []

        scoring_start = time.time()
        job_facet_embeddings_map = _build_job_facet_embeddings_map(repo, preliminary_matches)

        if status_callback:
            status_callback("scoring")
        scorer = ScoringService(repo=repo, config=matching_config.scorer)
        scored_matches = _run_scorer_service(
            scorer, preliminary_matches, matching_config,
            user_want_embeddings, job_facet_embeddings_map, stop_event,
        )

        match_dtos = _convert_matches_to_dtos(scored_matches)

    step_elapsed = time.time() - scoring_start
    logger.info("MATCHING Step 2 completed: Scored %d matches in %.2fs", len(match_dtos), step_elapsed)
    _log_match_results(match_dtos)
    return match_dtos


def _should_terminate_early(
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]],
) -> bool:
    """Check if we should terminate early based on stop event or status callback needs."""
    if stop_event.is_set():
        return True
    if status_callback:
        # This allows the callback to be called even if we're going to return early
        # The actual callback invocation happens in the calling function
        return False
    return False


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
            overall_score=match.overall_score or 0.0,
            fit_score=match.fit_score or 0.0,
            want_score=match.want_score or 0.0,
            job_similarity=match.job_similarity or 0.0,
            jd_required_coverage=match.jd_required_coverage,
            jd_preferences_coverage=match.jd_preferences_coverage,
            requirement_matches=[_matched_req_to_dto(r) for r in match.matched_requirements],
            missing_requirements=[_missing_req_to_dto(r) for r in match.missing_requirements],
            resume_fingerprint=match.resume_fingerprint,
            fit_components=match.fit_components,
            want_components=match.want_components,
            base_score=match.base_score,
            penalties=match.penalties,
            penalty_details=penalty_details_from_orm(
                match.penalty_details,
                total_penalties=match.penalties,
            ),
            fit_weight=match.fit_weight,
            want_weight=match.want_weight,
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
) -> int:
    """Save matches to database with per-match transactions."""
    saved_count = 0
    for dto in scored_match_dtos:
        try:
            with job_uow() as repo:
                existing = repo.get_existing_match(dto.job.id, resume_fingerprint)

                if existing and existing.status == 'active':
                    if existing.job_content_hash != dto.job.content_hash:
                        # FIX: clarified intent — mark existing stale, then insert
                        # a new record (is_stale_replacement=True signals insertion)
                        existing.status = 'stale'
                        existing.invalidated_reason = "Job content updated"
                        logger.info("Invalidated match for job %s due to content change", dto.job.id)
                        save_match_to_db(scored_match=dto, repo=repo, is_stale_replacement=True)
                        saved_count += 1
                        continue

                    if not matching_config.recalculate_existing:
                        logger.debug("Skipping existing match for job %s", dto.job.id)
                        continue

                save_match_to_db(scored_match=dto, repo=repo, is_stale_replacement=False)
                saved_count += 1
        except Exception:
            logger.exception("Failed saving match job_id=%s", dto.job.id)

    return saved_count


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

def _send_notifications(
    ctx: AppContext,
    scored_match_dtos: List[MatchResultDTO],
    saved_count: int,
    resume_fingerprint: str,
    stop_event: threading.Event,
    owner_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> int:
    """Send notifications for scored matches."""
    notification_config = ctx.config.notifications

    if not notification_config or not notification_config.enabled:
        logger.info("=== NOTIFICATION STEP: Skipped (disabled in config) ===")
        return 0

    if saved_count == 0:
        logger.info("=== NOTIFICATION STEP: Skipped (no matches to notify) ===")
        return 0

    step_start = time.time()
    logger.info("=== MATCHING STEP 3: Sending Notifications ===")

    try:
        user_id = owner_id or notification_config.user_id
        if not user_id:
            logger.warning(
                "Skipping notifications because no notification user identity is available"
            )
            return 0

        settings_snapshot = None
        enabled_channels = None
        try:
            resolved_user_id = UUID(str(user_id))
        except ValueError:
            resolved_user_id = None

        if resolved_user_id is not None:
            with job_uow() as repo:
                from database.models import User

                user = repo.db.get(User, resolved_user_id)
                if user is None:
                    logger.warning("Skipping notifications because user %s was not found", user_id)
                    return 0
                settings_snapshot = ctx.notification_service.get_user_notification_snapshot(user)
                enabled_channels = ctx.notification_service.get_enabled_channels_for_user(user)

            if not settings_snapshot.notifications_enabled:
                logger.info("Notifications disabled for user %s", user_id)
                return 0

            if not enabled_channels:
                logger.info("No enabled notification channels available for user %s", user_id)
                return 0
        else:
            enabled_channels = [
                name for name, cfg in notification_config.channels.items() if cfg.enabled
            ]
            if not enabled_channels:
                logger.warning("No notification channels configured")
                return 0

        high_score_matches = [
            dto for dto in scored_match_dtos
            if dto.overall_score is not None
            and dto.overall_score >= (
                settings_snapshot.min_score_threshold
                if settings_snapshot is not None
                else notification_config.min_score_threshold
            )
        ]

        notified_count = 0
        for dto in high_score_matches:
            if stop_event.is_set():
                break

            if not (
                settings_snapshot.notify_on_new_match
                if settings_snapshot is not None
                else notification_config.notify_on_new_match
            ):
                continue

            content = None
            match_id = None

            try:
                with job_uow() as repo:
                    match_record = repo.get_existing_match(
                        dto.job.id, resume_fingerprint, load_job_post=True,
                    )
                    if not match_record or not match_record.id:
                        logger.warning("No match record found for job %s, skipping", dto.job.id)
                        continue
                    if match_record.notified:
                        logger.debug("Match already notified for job %s, skipping", dto.job.id)
                        continue

                    match_id = match_record.id
                    job_post = match_record.job_post
                    if job_post:
                        content = NotificationMessageBuilder.build_notification_content(
                            job_post=job_post,
                            overall_score=float(dto.overall_score),
                            fit_score=dto.fit_score,
                            want_score=dto.want_score,
                            required_coverage=dto.jd_required_coverage,
                            apply_url=job_post.company_url_direct,
                        )

                    # FIX: persist notified=True in the same session — eliminates
                    # a redundant second UOW open/close per notification
                    if content:
                        ctx.notification_service.notify_new_match(
                            user_id=user_id,
                            match_id=str(match_id),
                            content=content,
                            channels=enabled_channels,
                            task_id=task_id,
                        )
                        notified_count += 1
                        match_record.notified = True

            except Exception:
                logger.exception("Failed to process notification for job_id=%s", dto.job.id)
                continue

        if (
            settings_snapshot.notify_on_batch_complete
            if settings_snapshot is not None
            else notification_config.notify_on_batch_complete
        ):
            try:
                ctx.notification_service.notify_batch_complete(
                    user_id=user_id,
                    total_matches=saved_count,
                    high_score_matches=len(high_score_matches),
                    channels=enabled_channels,
                    task_id=task_id,
                )
            except Exception as e:
                logger.error("Failed to send batch summary: %s", e)

        step_elapsed = time.time() - step_start
        logger.info(
            "MATCHING Step 3 completed: Sent %d notifications in %.2fs",
            notified_count, step_elapsed,
        )
        return notified_count

    except Exception as e:
        logger.error("Error in notification step: %s", e, exc_info=True)
        return 0
