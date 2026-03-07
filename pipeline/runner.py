"""Shared matching pipeline runner module.

This module contains the core matching pipeline logic that can be
used by both main.py and the web application.
"""

import os
import time
import json
import logging
import threading
from typing import List, Optional, Dict, Any, Callable, Tuple
from dataclasses import dataclass

from core.app_context import AppContext
from core.matcher import MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO, RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from core.llm.schema_models import ResumeSchema
from etl.resume import ResumeProfiler, ResumeParser
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


def _load_resume_with_parser(resume_file_path: str) -> Optional[dict]:
    """Load resume using ResumeParser for multi-format support."""
    logger.info(f"Loading resume from {resume_file_path}")
    try:
        parser = ResumeParser()
        parsed = parser.parse(resume_file_path)
        return parsed.data if parsed.data is not None else {"raw_text": parsed.text}
    except FileNotFoundError:
        logger.error(f"Resume file not found: {resume_file_path}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse resume: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading resume: {e}")
        return None


def load_user_wants_data(wants_file_path: str) -> List[str]:
    """Load user wants from a file. Each line is a separate want."""
    logger.info(f"Loading user wants from {wants_file_path}")
    try:
        with open(wants_file_path, 'r') as f:
            wants = [line.strip() for line in f if line.strip()]
            return wants
    except FileNotFoundError:
        logger.warning(f"User wants file not found: {wants_file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading user wants file: {e}")
        return []


def run_matching_pipeline(
    ctx: AppContext,
    stop_event: Optional[threading.Event] = None,
    status_callback: Optional[Callable[[str], None]] = None
) -> MatchingPipelineResult:
    """Run the matching pipeline as a self-contained operation.

    This function queries the database for the latest stored resume,
    loads the raw resume data from file, performs matching and scoring,
    then saves the results. It can run standalone without requiring
    ETL to have run in the same process.

    Args:
        ctx: Application context with config, AI service, and other dependencies
        stop_event: Optional threading event to signal early termination

    Returns:
        MatchingPipelineResult with success status and counts
    """
    if stop_event is None:
        stop_event = threading.Event()

    logger.info("=" * 60)
    logger.info("STARTING MATCHING PIPELINE")
    logger.info("=" * 60)

    matching_config = ctx.config.matching
    if not matching_config or not matching_config.enabled:
        logger.info("=== MATCHING PIPELINE: Skipped (disabled in config) ===")
        return MatchingPipelineResult(
            success=True,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error="Matching disabled in config"
        )

    try:
        # Step 1: Load and validate resume file
        resume_file, resume_data = _load_resume_file(ctx.config.etl)
        if not resume_file or not resume_data:
            return MatchingPipelineResult(
                success=False, matches_count=0, saved_count=0,
                notified_count=0, error="Failed to load resume"
            )

        # Step 2: Calculate fingerprint and determine if re-extraction is needed
        resume_fingerprint, should_re_extract = _determine_resume_extraction(
            resume_file, ctx.config.etl
        )

        # Step 3: Load user wants embeddings
        user_want_embeddings = _load_user_wants_embeddings(
            matching_config, ctx.ai_service
        )

        # Step 4: Run matching and scoring
        match_dtos = _run_matching_and_scoring(
            ctx, resume_data, resume_fingerprint, should_re_extract,
            matching_config, user_want_embeddings, stop_event, status_callback
        )
        if not match_dtos and stop_event.is_set():
            return MatchingPipelineResult(
                success=False, matches_count=0, saved_count=0,
                notified_count=0, error="Interrupted by system"
            )

        # Step 5: Save matches
        saved_count = _save_matches_batch(match_dtos, resume_fingerprint, matching_config)

        # Step 6: Send notifications
        notified_count = 0
        if ctx.notification_service and not stop_event.is_set():
            if status_callback:
                status_callback("notifying")
            notified_count = _send_notifications(
                ctx, match_dtos, saved_count, resume_data, resume_fingerprint, stop_event
            )

        execution_time = time.time() - _get_pipeline_start_time()
        logger.info("=" * 60)
        logger.info(f"MATCHING PIPELINE COMPLETED in {execution_time:.2f}s")
        logger.info("=" * 60)

        return MatchingPipelineResult(
            success=True,
            matches_count=len(match_dtos),
            saved_count=saved_count,
            notified_count=notified_count,
            execution_time=execution_time
        )

    except Exception as e:
        logger.exception("Error in matching pipeline")
        execution_time = time.time() - _get_pipeline_start_time()
        return MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error=str(e),
            execution_time=execution_time
        )


_pipeline_start_time: Optional[float] = None


def _get_pipeline_start_time() -> float:
    """Get the pipeline start time for execution time calculation."""
    global _pipeline_start_time
    if _pipeline_start_time is None:
        _pipeline_start_time = time.time()
    return _pipeline_start_time


def _reset_pipeline_start_time() -> None:
    """Reset the pipeline start time."""
    global _pipeline_start_time
    _pipeline_start_time = time.time()


def _load_resume_file(etl_config) -> Tuple[Optional[str], Optional[dict]]:
    """Load resume file from configured path and return (filepath, data)."""
    # Support both old path (etl.resume_file) and new path (etl.resume.resume_file)
    if etl_config and etl_config.resume:
        resume_file = etl_config.resume.resume_file
    elif etl_config and etl_config.resume_file:
        resume_file = etl_config.resume_file
    else:
        resume_file = None
    
    if not resume_file:
        logger.error("No resume file configured in ETL config")
        return None, None

    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    if not os.path.exists(resume_file):
        logger.error(f"Resume file not found: {resume_file}")
        return None, None

    resume_data = _load_resume_with_parser(resume_file)
    if not resume_data:
        logger.error("Failed to load resume data")
        return None, None

    return resume_file, resume_data


def _determine_resume_extraction(
    resume_file: str,
    etl_config
) -> Tuple[str, bool]:
    """
    Determine if resume should be re-extracted based on fingerprint comparison.
    
    Returns:
        Tuple of (resume_fingerprint, should_re_extract)
    """
    from database.models import generate_file_fingerprint
    from database.uow import job_uow

    with open(resume_file, 'rb') as f:
        current_fingerprint = generate_file_fingerprint(f.read())
    logger.info(f"Current resume fingerprint: {current_fingerprint[:16]}...")

    # Get stored fingerprint from DB
    with job_uow() as repo:
        stored_fingerprint = repo.resume.get_latest_stored_resume_fingerprint()

    # Check if force re-extraction is enabled
    force_re_extraction = (
        etl_config.resume.force_re_extraction
        if etl_config and etl_config.resume and etl_config.resume.force_re_extraction
        else False
    )

    # Determine if re-extraction is needed
    if force_re_extraction or not stored_fingerprint or current_fingerprint != stored_fingerprint:
        should_re_extract = True
        resume_fingerprint = current_fingerprint
        if not stored_fingerprint:
            logger.info("No stored resume found - will extract")
        elif current_fingerprint != stored_fingerprint:
            logger.info(f"Resume file changed (stored: {stored_fingerprint[:16]}..., current: {current_fingerprint[:16]}...) - will re-extract")
        else:
            logger.info("Force re-extraction enabled in config - will re-extract")
    else:
        should_re_extract = False
        resume_fingerprint = stored_fingerprint
        logger.info(f"Resume unchanged (fingerprint: {current_fingerprint[:16]}...) - using stored data")

    return resume_fingerprint, should_re_extract


def _load_user_wants_embeddings(
    matching_config,
    ai_service
) -> List[List[float]]:
    """Load user wants from file and generate embeddings."""
    user_want_embeddings = []
    
    if matching_config.user_wants_file:
        wants_file = matching_config.user_wants_file
        if not os.path.isabs(wants_file):
            wants_file = os.path.join(os.getcwd(), wants_file)
        if not os.path.exists(wants_file):
            logger.warning(f"User wants file not found: {wants_file}")
        else:
            user_wants = load_user_wants_data(wants_file)
            if user_wants:
                logger.info(f"Loaded {len(user_wants)} user wants from {matching_config.user_wants_file}")
                for want_text in user_wants:
                    embedding = ai_service.generate_embedding(want_text)
                    user_want_embeddings.append(embedding)
    
    return user_want_embeddings


def _run_matching_and_scoring(
    ctx: AppContext,
    resume_data: dict,
    resume_fingerprint: str,
    should_re_extract: bool,
    matching_config,
    user_want_embeddings: List[List[float]],
    stop_event: threading.Event,
    status_callback: Optional[Callable[[str], None]]
) -> List[MatchResultDTO]:
    """Run the matching and scoring pipeline within a UOW context."""
    from database.uow import job_uow

    if status_callback:
        status_callback("loading_resume")

    step_start = time.time()
    logger.info("=== RESUME ETL STEP 2: Prepare Resume & Compare Fingerprint ===")

    job_facet_embeddings_map = {}
    match_dtos = []

    with job_uow() as repo:
        # Only load structured resume from DB if NOT re-extracting
        structured_resume = None
        if not should_re_extract:
            structured_resume = repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)

        if not structured_resume:
            if should_re_extract:
                logger.info(f"Will re-extract resume (fingerprint: {resume_fingerprint[:16]}...)")
            else:
                error_msg = f"Resume not found in database for fingerprint: {resume_fingerprint[:16]}..."
                logger.error(error_msg)
                logger.error("Make sure Resume ETL has been run")
                return []

        if structured_resume:
            logger.info(f"Loaded resume from database (fingerprint: {resume_fingerprint[:16]}...)")
            logger.info(f"Resume experience: {structured_resume.total_experience_years} years")

        # Create matcher with store to persist resume embeddings
        matcher = MatcherService(
            resume_profiler=ResumeProfiler(
                ai_service=ctx.ai_service,
                store=JobRepositoryAdapter(repo)
            ),
            config=matching_config.matcher
        )

        step_elapsed = time.time() - step_start
        logger.info(f"RESUME ETL Step 2 completed: Resume prepared in {step_elapsed:.2f}s")

        if stop_event.is_set():
            return []

        if status_callback:
            status_callback("vector_matching")

        step_start = time.time()
        logger.info("=== MATCHING STEP 1: Running MatcherService (Vector Retrieval) ===")

        # Check if we have a stored structured resume to use (avoid re-extraction)
        pre_extracted_resume = None
        if not should_re_extract:
            if structured_resume and structured_resume.extracted_data:
                try:
                    pre_extracted_resume = ResumeSchema.model_validate(structured_resume.extracted_data)
                    logger.info(f"Using stored structured resume (fingerprint: {resume_fingerprint[:16]}...)")
                except Exception as e:
                    logger.warning(f"Failed to parse stored resume: {e}. Will re-extract.")
        else:
            logger.info("Resume re-extraction needed - will extract fresh")

        # Retrieve top jobs based on cosine distance with resume summary embedding
        preliminary_matches = matcher.match_resume_two_stage(
            repo=repo,
            resume_data=resume_data,
            stop_event=stop_event,
            pre_extracted_resume=pre_extracted_resume,
            resume_fingerprint=resume_fingerprint,
        )

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 1 completed: Matched against {len(preliminary_matches)} jobs in {step_elapsed:.2f}s")

        if stop_event.is_set():
            return []

        if status_callback:
            status_callback("scoring")

        step_start = time.time()
        logger.info("=== MATCHING STEP 2: Running ScorerService (Rule-based Scoring) ===")

        scorer = ScoringService(repo=repo, config=matching_config.scorer)

        if user_want_embeddings:
            logger.info("=== Using Fit/Want scoring with 'user wants' embeddings ===")
            for preliminary in preliminary_matches:
                job_id = str(preliminary.job.id)
                if job_id not in job_facet_embeddings_map:
                    job_facet_embeddings_map[job_id] = repo.get_job_facet_embeddings(preliminary.job.id)

            scored_matches = scorer.score_matches(
                preliminary_matches=preliminary_matches,
                result_policy=matching_config.result_policy,
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings_map=job_facet_embeddings_map,
                match_type="requirements_only",
                stop_event=stop_event,
            )
        else:
            logger.info("=== Using Fit-only scoring ===")
            scored_matches = scorer.score_matches(
                preliminary_matches=preliminary_matches,
                result_policy=matching_config.result_policy,
                match_type="requirements_only",
                stop_event=stop_event,
            )

        # Convert ORM objects to DTOs before exiting UOW context
        match_dtos = _convert_matches_to_dtos(scored_matches)

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 2 completed: Scored {len(match_dtos)} matches in {step_elapsed:.2f}s")

    if match_dtos:
        logger.info("Top 5 Matches:")
        for i, dto in enumerate(match_dtos[:5], 1):
            logger.info(f"  {i}. {dto.job.title} @ {dto.job.company}: overall={dto.overall_score:.1f}/100 (fit={dto.fit_score:.1f}, want={dto.want_score:.1f})")

    return match_dtos


def _convert_matches_to_dtos(scored_matches) -> List[MatchResultDTO]:
    """Convert ORM match objects to DTOs."""
    match_dtos = []
    for match in scored_matches:
        # Extract requirement matches
        requirement_matches_dtos = []
        for req in match.matched_requirements:
            evidence_dto = None
            if req.evidence:
                evidence_dto = JobEvidenceDTO(
                    text=getattr(req.evidence, 'text', ''),
                    source_section=getattr(req.evidence, 'source_section', None),
                    tags=getattr(req.evidence, 'tags', {}),
                )
            requirement_matches_dtos.append(RequirementMatchDTO(
                requirement=JobRequirementDTO(
                    id=str(req.requirement.id),
                    req_type=getattr(req.requirement, 'req_type', 'required'),
                ),
                evidence=evidence_dto,
                similarity=req.similarity,
                is_covered=req.is_covered,
            ))

        # Extract missing requirements
        missing_requirements_dtos = []
        for req in match.missing_requirements:
            missing_requirements_dtos.append(RequirementMatchDTO(
                requirement=JobRequirementDTO(
                    id=str(req.requirement.id),
                    req_type=getattr(req.requirement, 'req_type', 'required'),
                ),
                similarity=req.similarity,
                is_covered=False,
            ))

        dto = MatchResultDTO(
            job=JobMatchDTO(
                id=str(match.job.id),
                title=getattr(match.job, 'title', 'Unknown'),
                company=getattr(match.job, 'company', 'Unknown'),
                location_text=getattr(match.job, 'location_text', ''),
                is_remote=getattr(match.job, 'is_remote', False),
                content_hash=getattr(match.job, 'content_hash', ''),
            ),
            overall_score=match.overall_score if match.overall_score is not None else 0.0,
            fit_score=match.fit_score if match.fit_score is not None else 0.0,
            want_score=match.want_score if match.want_score is not None else 0.0,
            job_similarity=match.job_similarity if match.job_similarity is not None else 0.0,
            jd_required_coverage=match.jd_required_coverage,
            jd_preferences_coverage=match.jd_preferences_coverage,
            requirement_matches=requirement_matches_dtos,
            missing_requirements=missing_requirements_dtos,
            resume_fingerprint=match.resume_fingerprint,
            fit_components=getattr(match, 'fit_components', {}),
            want_components=getattr(match, 'want_components', {}),
            base_score=getattr(match, 'base_score', 0.0),
            penalties=getattr(match, 'penalties', 0.0),
            penalty_details=penalty_details_from_orm(
                getattr(match, 'penalty_details', []),
                total_penalties=getattr(match, 'penalties', 0.0)
            ),
            fit_weight=getattr(match, 'fit_weight', 0.7),
            want_weight=getattr(match, 'want_weight', 0.3),
            match_type=getattr(match, 'match_type', 'requirements_only'),
        )
        match_dtos.append(dto)
    return match_dtos


def _save_matches_batch(
    scored_match_dtos: List[MatchResultDTO],
    resume_fingerprint: str,
    matching_config
) -> int:
    """Save matches to database with per-match transactions."""
    saved_count = 0
    for dto in scored_match_dtos:
        try:
            with job_uow() as repo:
                existing = repo.get_existing_match(dto.job.id, resume_fingerprint)
                
                # Handle existing active matches
                if existing and existing.status == 'active':
                    # Job content changed → mark old as stale and create new match
                    # (Preserves history: stale shows WHY it was replaced)
                    if existing.job_content_hash != dto.job.content_hash:
                        existing.status = 'stale'
                        existing.invalidated_reason = "Job content updated"
                        logger.info(f"Invalidated match for job {dto.job.id} due to content change")
                        save_match_to_db(
                            scored_match=dto,
                            repo=repo,
                            is_stale_replacement=False,  # Updates existing record
                        )
                        saved_count += 1
                        continue
                    
                    # Content unchanged → respect recalculate_existing flag
                    elif not matching_config.recalculate_existing:
                        logger.debug(f"Skipping existing match for job {dto.job.id}")
                        continue

                # No existing match OR we need to update → save
                save_match_to_db(
                    scored_match=dto,
                    repo=repo,
                    is_stale_replacement=False,  # Updates existing or creates new
                )
                saved_count += 1
        except Exception:
            logger.exception("Failed saving match job_id=%s", dto.job.id)

    return saved_count


def _send_notifications(
    ctx: AppContext,
    scored_match_dtos: List[MatchResultDTO],
    saved_count: int,
    resume_data: dict,
    resume_fingerprint: str,
    stop_event: threading.Event
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
    logger.info("=== MATCHING STEP 4: Sending Notifications ===")

    try:
        user_id = notification_config.user_id or resume_data.get('email') or 'default_user'

        enabled_channels = [
            channel_name for channel_name, channel_config
            in notification_config.channels.items()
            if channel_config.enabled
        ]

        if not enabled_channels:
            logger.warning("No notification channels configured")
            return 0

        high_score_matches = [
            dto for dto in scored_match_dtos
            if dto.overall_score is not None and dto.overall_score >= notification_config.min_score_threshold
        ]

        notified_count = 0
        for dto in high_score_matches:
            if stop_event.is_set():
                break

            if notification_config.notify_on_new_match:
                job_dto = dto.job
                content = None
                match_id = None

                # Get match record and build notification content inside session
                try:
                    with job_uow() as repo:
                        match_record = repo.get_existing_match(
                            job_dto.id,
                            resume_fingerprint,
                            load_job_post=True
                        )

                        if not match_record or not match_record.id:
                            logger.warning(f"No match record found for job {job_dto.id}, skipping notification")
                            continue

                        if match_record.notified:
                            logger.debug(f"Match already notified for job {job_dto.id}, skipping")
                            continue

                        match_id = match_record.id
                        job_post = match_record.job_post

                        # Build content while session is alive - job_post is eager loaded
                        if job_post:
                            content = NotificationMessageBuilder.build_notification_content(
                                job_post=job_post,
                                overall_score=float(dto.overall_score),
                                fit_score=dto.fit_score,
                                want_score=dto.want_score,
                                required_coverage=dto.jd_required_coverage,
                                apply_url=job_post.company_url_direct
                            )
                except Exception:
                    logger.exception("Failed to get match record for job_id=%s", job_dto.id)
                    continue

                # Session closed - safe to call notification with serializable content
                if content:
                    try:
                        ctx.notification_service.notify_new_match(
                            user_id=user_id,
                            match_id=str(match_id),
                            content=content,
                            channels=enabled_channels,
                        )
                        notified_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send notification for match {match_id}: {e}")
                        continue

                    # Persist notified flag
                    try:
                        with job_uow() as repo:
                            match_record = repo.get_existing_match(job_dto.id, resume_fingerprint)
                            if match_record:
                                match_record.notified = True
                    except Exception as e:
                        logger.error(f"Failed to persist notified flag for match {match_id}: {e}")

        if notification_config.notify_on_batch_complete:
            try:
                ctx.notification_service.notify_batch_complete(
                    user_id=user_id,
                    total_matches=saved_count,
                    high_score_matches=len(high_score_matches),
                    channels=enabled_channels
                )
            except Exception as e:
                logger.error(f"Failed to send batch summary: {e}")

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 4 completed: Sent {notified_count} notifications in {step_elapsed:.2f}s")
        return notified_count
    except Exception as e:
        logger.error(f"Error in notification step: {e}", exc_info=True)
        return 0
