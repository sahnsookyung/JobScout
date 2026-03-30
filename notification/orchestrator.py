"""Notification orchestration for the matching pipeline.

Decides when and what to notify based on match results and user settings.
Delegates actual delivery to NotificationService.
"""

import logging
import time
import threading
from typing import Any, List, Optional
from uuid import UUID

from database.models import User
from database.uow import job_uow
from notification.message_builder import NotificationMessageBuilder
from notification.models import NotificationDeliveryPlan

logger = logging.getLogger(__name__)


def _notification_setting_value(
    notification_config,
    settings_snapshot: Any,
    attribute: str,
):
    """Read a notification setting from user settings when present, else config."""
    if settings_snapshot is not None:
        return getattr(settings_snapshot, attribute)
    return getattr(notification_config, attribute)


def _resolve_notification_plan(
    ctx,
    owner_id: Optional[str],
) -> Optional[NotificationDeliveryPlan]:
    """Resolve the notification recipient identity and enabled channels."""
    notification_config = ctx.config.notifications
    user_id = owner_id or notification_config.user_id
    if not user_id:
        logger.warning("Skipping notifications because no notification user identity is available")
        return None

    resolved_user_id: Optional[UUID]
    try:
        resolved_user_id = UUID(str(user_id))
    except ValueError:
        resolved_user_id = None

    if resolved_user_id is None:
        enabled_channels = [
            name for name, cfg in notification_config.channels.items() if cfg.enabled
        ]
        if not enabled_channels:
            logger.warning("No notification channels configured")
            return None
        return NotificationDeliveryPlan(user_id=str(user_id), enabled_channels=enabled_channels)

    with job_uow() as repo:
        user = repo.db.get(User, resolved_user_id)
        if user is None:
            logger.warning("Skipping notifications because user %s was not found", user_id)
            return None

        settings_snapshot = ctx.notification_service.get_user_notification_snapshot(user)
        enabled_channels = ctx.notification_service.get_enabled_channels_for_user(user)

    if not settings_snapshot.notifications_enabled:
        logger.info("Notifications disabled for user %s", user_id)
        return None

    if not enabled_channels:
        logger.info("No enabled notification channels available for user %s", user_id)
        return None

    return NotificationDeliveryPlan(
        user_id=str(user_id),
        enabled_channels=enabled_channels,
        settings_snapshot=settings_snapshot,
    )


def _high_score_matches_for_plan(
    scored_match_dtos: List,
    notification_config,
    delivery_plan: NotificationDeliveryPlan,
) -> List:
    """Filter matches down to those that should trigger delivery."""
    threshold = _notification_setting_value(
        notification_config,
        delivery_plan.settings_snapshot,
        "min_score_threshold",
    )
    return [
        dto for dto in scored_match_dtos
        if dto.overall_score is not None and dto.overall_score >= threshold
    ]


def _send_match_notification(
    ctx,
    dto,
    *,
    resume_fingerprint: str,
    delivery_plan: NotificationDeliveryPlan,
    task_id: Optional[str],
) -> bool:
    """Send a single match notification when a saved match record is available."""
    content = None
    match_id = None

    with job_uow() as repo:
        match_record = repo.get_existing_match(
            dto.job.id, resume_fingerprint, load_job_post=True,
        )
        if not match_record or not match_record.id:
            logger.warning("No match record found for job %s, skipping", dto.job.id)
            return False
        if match_record.notified:
            logger.debug("Match already notified for job %s, skipping", dto.job.id)
            return False

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

        if not content:
            return False

        ctx.notification_service.notify_new_match(
            user_id=delivery_plan.user_id,
            match_id=str(match_id),
            content=content,
            channels=delivery_plan.enabled_channels,
            task_id=task_id,
        )
        match_record.notified = True
        return True


def _send_batch_complete_notification(
    ctx,
    *,
    delivery_plan: NotificationDeliveryPlan,
    saved_count: int,
    high_score_matches: List,
    task_id: Optional[str],
) -> None:
    """Send the batch summary notification."""
    ctx.notification_service.notify_batch_complete(
        user_id=delivery_plan.user_id,
        total_matches=saved_count,
        high_score_matches=len(high_score_matches),
        channels=delivery_plan.enabled_channels,
        task_id=task_id,
    )


def _notify_per_match(
    ctx,
    high_score_matches: List,
    notification_config,
    delivery_plan: NotificationDeliveryPlan,
    resume_fingerprint: str,
    task_id: Optional[str],
    stop_event: threading.Event,
) -> int:
    """Send one notification per high-score match. Returns count sent."""
    notify_on_new_match = _notification_setting_value(
        notification_config, delivery_plan.settings_snapshot, "notify_on_new_match",
    )
    notified_count = 0
    for dto in high_score_matches:
        if stop_event.is_set():
            break
        if not notify_on_new_match:
            continue
        try:
            if _send_match_notification(
                ctx, dto,
                resume_fingerprint=resume_fingerprint,
                delivery_plan=delivery_plan,
                task_id=task_id,
            ):
                notified_count += 1
        except Exception:
            logger.exception("Failed to process notification for job_id=%s", dto.job.id)
    return notified_count


def send_notifications(
    ctx,
    scored_match_dtos: List,
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
        delivery_plan = _resolve_notification_plan(ctx, owner_id)
        if delivery_plan is None:
            return 0

        high_score_matches = _high_score_matches_for_plan(
            scored_match_dtos, notification_config, delivery_plan,
        )

        notified_count = _notify_per_match(
            ctx, high_score_matches, notification_config, delivery_plan,
            resume_fingerprint, task_id, stop_event,
        )

        if _notification_setting_value(
            notification_config, delivery_plan.settings_snapshot, "notify_on_batch_complete",
        ):
            try:
                _send_batch_complete_notification(
                    ctx,
                    delivery_plan=delivery_plan,
                    saved_count=saved_count,
                    high_score_matches=high_score_matches,
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
