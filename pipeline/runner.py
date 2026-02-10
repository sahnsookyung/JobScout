"""Shared matching pipeline runner module.

This module contains the core matching pipeline logic that can be
used by both main.py and the web application.
"""

import os
import time
import json
import logging
import threading
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass

from core.app_context import AppContext
from core.matcher import MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO, RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from etl.resume import ResumeProfiler
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow


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


def load_resume_data(resume_file_path: str) -> Optional[dict]:
    """Load resume data from JSON file."""
    logger.info(f"Loading resume from {resume_file_path}")
    try:
        with open(resume_file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Resume file not found: {resume_file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in resume file: {e}")
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
    
    pipeline_start = time.time()
    
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
        if status_callback:
            status_callback("loading_resume")
        
        step_start = time.time()
        logger.info("=== MATCHING STEP 6: Loading Resume & Preparing Evidence ===")

        # Step 1: Verify resume file exists
        resume_file = ctx.config.etl.resume_file if (ctx.config.etl and ctx.config.etl.resume_file) else None
        if not resume_file:
            error_msg = "No resume file configured in ETL config"
            logger.error(error_msg)
            return MatchingPipelineResult(
                success=False,
                matches_count=0,
                saved_count=0,
                notified_count=0,
                error=error_msg
            )
        
        if not os.path.isabs(resume_file):
            resume_file = os.path.join(os.getcwd(), resume_file)
        
        if not os.path.exists(resume_file):
            error_msg = f"Resume file not found: {resume_file}"
            logger.error(error_msg)
            return MatchingPipelineResult(
                success=False,
                matches_count=0,
                saved_count=0,
                notified_count=0,
                error=error_msg
            )

        # Step 2: Load raw resume data
        resume_data = load_resume_data(resume_file)
        if not resume_data:
            error_msg = "Failed to load resume data"
            logger.error(error_msg)
            return MatchingPipelineResult(
                success=False,
                matches_count=0,
                saved_count=0,
                notified_count=0,
                error=error_msg
            )

        # Step 3: Query for latest stored resume fingerprint
        with job_uow() as repo:
            resume_fingerprint = repo.resume.get_latest_stored_resume_fingerprint()
        
        if not resume_fingerprint:
            error_msg = "No resume found in database. Run ETL first."
            logger.error(error_msg)
            return MatchingPipelineResult(
                success=False,
                matches_count=0,
                saved_count=0,
                notified_count=0,
                error=error_msg
            )

        # Load user wants BEFORE entering UOW (AI service calls are slow, don't hold DB transaction)
        user_wants = []
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
                        embedding = ctx.ai_service.generate_embedding(want_text)
                        user_want_embeddings.append(embedding)

        # Step 4: Run matching and scoring within UOW, then convert to DTOs
        job_facet_embeddings_map = {}
        match_dtos = []
        
        with job_uow() as repo:
            structured_resume = repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)

            if not structured_resume:
                error_msg = f"Resume not found in database for fingerprint: {resume_fingerprint[:16]}..."
                logger.error(error_msg)
                logger.error("Make sure ETL Step 5 (resume processing) completed successfully")
                return MatchingPipelineResult(
                    success=False,
                    matches_count=0,
                    saved_count=0,
                    notified_count=0,
                    error=error_msg
                )

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
            logger.info(f"MATCHING Step 6 completed: Resume loaded in {step_elapsed:.2f}s")

            if stop_event.is_set():
                return MatchingPipelineResult(
                    success=False,
                    matches_count=0,
                    saved_count=0,
                    notified_count=0,
                    error="Interrupted by system"
                )

            if status_callback:
                status_callback("vector_matching")

            step_start = time.time()
            logger.info("=== MATCHING STEP 7: Running MatcherService (Vector Retrieval) ===")

            # Retrieve top jobs based on cosine distance with resume summary embedding
            preliminary_matches = matcher.match_resume_two_stage(
                repo=repo,
                resume_data=resume_data,
                stop_event=stop_event,
            )

            step_elapsed = time.time() - step_start
            logger.info(f"MATCHING Step 7 completed: Matched against {len(preliminary_matches)} jobs in {step_elapsed:.2f}s")

            if stop_event.is_set():
                return MatchingPipelineResult(
                    success=False,
                    matches_count=0,
                    saved_count=0,
                    notified_count=0,
                    error="Interrupted by system"
                )

            if status_callback:
                status_callback("scoring")

            step_start = time.time()
            logger.info("=== MATCHING STEP 8: Running ScorerService (Rule-based Scoring) ===")

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
                        total_penalties=getattr(match, 'penalties', 0.0),
                        preferences_boost=getattr(match, 'preferences_boost', 0.0)
                    ),
                    preferences_boost=getattr(match, 'preferences_boost', 0.0),
                    fit_weight=getattr(match, 'fit_weight', 0.7),
                    want_weight=getattr(match, 'want_weight', 0.3),
                    match_type=getattr(match, 'match_type', 'requirements_only'),
                )
                match_dtos.append(dto)

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 8 completed: Scored {len(match_dtos)} matches in {step_elapsed:.2f}s")

        if match_dtos:
            logger.info("Top 5 Matches:")
            for i, dto in enumerate(match_dtos[:5], 1):
                logger.info(f"  {i}. {dto.job.title} @ {dto.job.company}: overall={dto.overall_score:.1f}/100 (fit={dto.fit_score:.1f}, want={dto.want_score:.1f})")

        if stop_event.is_set():
            return MatchingPipelineResult(
                success=False,
                matches_count=len(match_dtos),
                saved_count=0,
                notified_count=0,
                error="Interrupted by system"
            )

        # Step 9: Save matches with per-match transactions
        if status_callback:
            status_callback("saving_results")

        step_start = time.time()
        logger.info("=== MATCHING STEP 9: Saving Matches to Database ===")
        saved_count = _save_matches_batch(match_dtos, resume_fingerprint, matching_config)
        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 9 completed: Saved {saved_count} matches in {step_elapsed:.2f}s")

        if stop_event.is_set():
            return MatchingPipelineResult(
                success=True,
                matches_count=len(match_dtos),
                saved_count=saved_count,
                notified_count=0,
                error="Interrupted by system before notifications"
            )

        # Step 10: Send notifications (optional, only if notification service exists)
        notified_count = 0
        if ctx.notification_service:
            if status_callback:
                status_callback("notifying")
            notified_count = _send_notifications(
                ctx, match_dtos, saved_count, resume_data, resume_fingerprint, stop_event
            )

        execution_time = time.time() - pipeline_start
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
        execution_time = time.time() - pipeline_start
        return MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error=str(e),
            execution_time=execution_time
        )


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
                            is_stale_replacement=True,  # Creates NEW record
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
    logger.info("=== MATCHING STEP 10: Sending Notifications ===")

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

                # Get match record first
                try:
                    with job_uow() as repo:
                        match_record = repo.get_existing_match(
                            job_dto.id,
                            resume_fingerprint
                        )

                        if not match_record or not match_record.id:
                            logger.warning(f"No match record found for job {job_dto.id}, skipping notification")
                            continue

                        if match_record.notified:
                            logger.debug(f"Match already notified for job {job_dto.id}, skipping")
                            continue

                        match_id = match_record.id
                except Exception:
                    logger.exception("Failed to get match record for job_id=%s", job_dto.id)
                    continue

                # Send notification
                try:
                    ctx.notification_service.notify_new_match(
                        user_id=user_id,
                        match_id=str(match_id),
                        job_title=job_dto.title,
                        company=job_dto.company,
                        score=float(dto.overall_score),
                        location=job_dto.location_text,
                        is_remote=job_dto.is_remote,
                        channels=enabled_channels
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
        logger.info(f"MATCHING Step 10 completed: Sent {notified_count} notifications in {step_elapsed:.2f}s")
        return notified_count
    except Exception as e:
        logger.error(f"Error in notification step: {e}", exc_info=True)
        return 0
