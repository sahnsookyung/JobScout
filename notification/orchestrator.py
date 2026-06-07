"""Notification orchestration for the matching pipeline.

Decides when and what to notify based on match results and user settings.
Delegates actual delivery to NotificationService.
"""

import logging
import time
import threading
from dataclasses import dataclass
from typing import Any, List, Optional
from uuid import UUID

from database.models import User
from database.uow import job_uow
from notification.message_builder import NotificationMessageBuilder
from notification.models import NotificationDeliveryPlan

logger = logging.getLogger(__name__)


@dataclass
class NotificationCandidate:
    """Plain ranking candidate detached from SQLAlchemy session lifecycle."""

    id: str
    fit_score: float | None
    preference_score: float | None
    job_similarity: float | None
    alert_eligible: bool | None = None
    selection_tier: str = "primary"
    excluded_reason: str | None = None
    ranking_explanation: Any = None


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


def _alert_eligible_matches_for_plan(
    persisted_matches: List,
    notification_config,
    delivery_plan: NotificationDeliveryPlan,
) -> List:
    """Filter persisted matches down to those eligible for alerts."""
    if all(getattr(match, "alert_eligible", None) is not None for match in persisted_matches):
        return [match for match in persisted_matches if bool(match.alert_eligible)]

    min_fit_for_alerts = _notification_setting_value(
        notification_config,
        delivery_plan.settings_snapshot,
        "min_fit_for_alerts",
    )
    return [
        match for match in persisted_matches
        if match.fit_score is not None and float(match.fit_score) >= min_fit_for_alerts
    ]


def _load_persisted_notification_matches(
    selection_run_id: str,
    *,
    tier: Optional[str] = "primary",
) -> List:
    """Load canonical persisted notification candidates for this run."""
    with job_uow() as repo:
        items = repo.match_selection.get_items_for_run(
            selection_run_id, tier=tier
        )
        return [
            NotificationCandidate(
                id=str(item.job_match_id),
                fit_score=float(item.fit_score_at_selection),
                preference_score=(
                    None
                    if item.preference_score_at_selection is None
                    else float(item.preference_score_at_selection)
                ),
                job_similarity=float(item.job_similarity_at_selection),
                alert_eligible=bool(item.alert_eligible),
                selection_tier=str(getattr(item, "selection_tier", "primary") or "primary"),
                excluded_reason=getattr(item, "excluded_reason", None),
            )
            for item in items
        ]


def resolve_notification_fit_floor(
    ctx,
    *,
    owner_id: Optional[str] = None,
) -> float:
    """Resolve the notification fit floor that should be captured in selection snapshots."""
    notification_config = getattr(getattr(ctx, "config", None), "notifications", None)
    if notification_config is None:
        return 0.0
    if getattr(ctx, "notification_service", None) is None:
        return float(getattr(notification_config, "min_fit_for_alerts", 0.0) or 0.0)

    delivery_plan = _resolve_notification_plan(ctx, owner_id)
    if delivery_plan is None:
        return float(getattr(notification_config, "min_fit_for_alerts", 0.0) or 0.0)

    return float(
        _notification_setting_value(
            notification_config,
            delivery_plan.settings_snapshot,
            "min_fit_for_alerts",
        )
    )


def _send_match_notification(
    ctx,
    match_id: str,
    *,
    delivery_plan: NotificationDeliveryPlan,
    task_id: Optional[str],
) -> bool:
    """Send a single notification from a persisted active match row."""
    content = None

    with job_uow() as repo:
        match_record = repo.match.get_match_by_id(match_id)
        if not match_record or not match_record.id:
            logger.warning("No match record found for match %s, skipping", match_id)
            return False
        if match_record.status != "active" or match_record.is_hidden:
            logger.debug("Match %s is no longer active/visible, skipping", match_id)
            return False
        if match_record.notified:
            logger.debug("Match already notified for %s, skipping", match_id)
            return False

        job_post = match_record.job_post
        if job_post:
            content = NotificationMessageBuilder.build_from_orm(
                job_post,
                match_record,
                apply_url=job_post.company_url_direct,
            )

        if not content:
            return False

        results = ctx.notification_service.notify_new_match(
            user_id=delivery_plan.user_id,
            match_id=str(match_id),
            content=content,
            channels=delivery_plan.enabled_channels,
            task_id=task_id,
        )
        if not any(results.values()):
            logger.warning(
                "No notification channels accepted delivery for match %s; leaving it eligible for retry",
                match_id,
            )
            return False
        match_record.notified = True
        return True


def _send_batch_complete_notification(
    ctx,
    *,
    delivery_plan: NotificationDeliveryPlan,
    saved_count: int,
    alert_eligible_matches: List,
    min_fit_for_alerts: int,
    task_id: Optional[str],
) -> bool:
    """Send the batch summary notification."""
    results = ctx.notification_service.notify_batch_complete(
        user_id=delivery_plan.user_id,
        total_matches=saved_count,
        alert_eligible_matches=len(alert_eligible_matches),
        min_fit_for_alerts=min_fit_for_alerts,
        channels=delivery_plan.enabled_channels,
        task_id=task_id,
    )
    return any(results.values())


def _notify_per_match(
    ctx,
    alert_eligible_matches: List,
    notification_config,
    delivery_plan: NotificationDeliveryPlan,
    task_id: Optional[str],
    stop_event: threading.Event,
) -> int:
    """Send one notification per alert-eligible persisted match. Returns count sent."""
    notify_on_new_match = _notification_setting_value(
        notification_config, delivery_plan.settings_snapshot, "notify_on_new_match",
    )
    notified_count = 0
    for match in alert_eligible_matches:
        if stop_event.is_set():
            break
        if not notify_on_new_match:
            continue
        try:
            if _send_match_notification(
                ctx,
                str(match.id),
                delivery_plan=delivery_plan,
                task_id=task_id,
            ):
                notified_count += 1
        except Exception:
            logger.exception("Failed to process notification for match_id=%s", match.id)
    return notified_count


def send_notifications(
    ctx,
    *,
    failed_count: int,
    resume_fingerprint: str,
    stop_event: threading.Event,
    selection_run_id: Optional[str] = None,
    owner_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> int:
    """Send notifications for the persisted active match set for this run."""
    notification_config = ctx.config.notifications

    if not notification_config or not notification_config.enabled:
        logger.info("=== NOTIFICATION STEP: Skipped (disabled in config) ===")
        return 0

    if failed_count > 0:
        logger.warning(
            "=== NOTIFICATION STEP: Skipped (%d match saves failed; suppressing alerts for this run) ===",
            failed_count,
        )
        return 0

    step_start = time.time()
    logger.info("=== MATCHING STEP 3: Sending Notifications ===")

    try:
        delivery_plan = _resolve_notification_plan(ctx, owner_id)
        if delivery_plan is None:
            return 0

        if not selection_run_id:
            logger.warning(
                "=== NOTIFICATION STEP: Skipped (no committed selection run for resume %s) ===",
                resume_fingerprint[:16],
            )
            return 0

        persisted_matches = _load_persisted_notification_matches(selection_run_id, tier="all")
        if not persisted_matches:
            logger.info(
                "=== NOTIFICATION STEP: Skipped (no persisted selection items for resume %s) ===",
                resume_fingerprint[:16],
            )
            return 0
        primary_matches = [
            match for match in persisted_matches
            if getattr(match, "selection_tier", "primary") == "primary"
        ]
        alert_eligible_matches = _alert_eligible_matches_for_plan(
            primary_matches,
            notification_config,
            delivery_plan,
        )
        min_fit_for_alerts = int(
            _notification_setting_value(
                notification_config,
                delivery_plan.settings_snapshot,
                "min_fit_for_alerts",
            )
        )

        per_match_count = _notify_per_match(
            ctx,
            alert_eligible_matches,
            notification_config,
            delivery_plan,
            task_id,
            stop_event,
        )
        batch_count = 0

        if _notification_setting_value(
            notification_config, delivery_plan.settings_snapshot, "notify_on_batch_complete",
        ):
            try:
                if _send_batch_complete_notification(
                    ctx,
                    delivery_plan=delivery_plan,
                    saved_count=len(persisted_matches),
                    alert_eligible_matches=alert_eligible_matches,
                    min_fit_for_alerts=min_fit_for_alerts,
                    task_id=task_id,
                ):
                    batch_count = 1
            except Exception:
                logger.exception("Failed to send batch summary")

        notified_count = per_match_count + batch_count
        step_elapsed = time.time() - step_start
        logger.info(
            "MATCHING Step 3 completed: Sent %d notifications in %.2fs",
            notified_count, step_elapsed,
        )
        return notified_count

    except Exception:
        logger.exception("Error in notification step")
        return 0
