from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable, Optional

from sqlalchemy import select, update
from sqlalchemy.orm import joinedload

from core.match_selection.contracts import (
    MatchSelectionItemSnapshot,
    MatchSelectionPolicySnapshot,
)
from database.models import JobMatch, MatchSelectionItem, MatchSelectionRun
from database.repositories.base import BaseRepository


class MatchSelectionRepository(BaseRepository):
    def get_current_run_for_resume(
        self,
        resume_fingerprint: str,
    ) -> Optional[MatchSelectionRun]:
        stmt = (
            select(MatchSelectionRun)
            .where(
                MatchSelectionRun.resume_fingerprint == resume_fingerprint,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
            )
            .order_by(MatchSelectionRun.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_latest_current_run_for_owner(
        self,
        owner_id: Any,
    ) -> Optional[MatchSelectionRun]:
        stmt = (
            select(MatchSelectionRun)
            .where(
                MatchSelectionRun.owner_id == owner_id,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
            )
            .order_by(MatchSelectionRun.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_committed_run_for_task(
        self,
        *,
        resume_fingerprint: str,
        task_id: str,
    ) -> Optional[MatchSelectionRun]:
        stmt = (
            select(MatchSelectionRun)
            .where(
                MatchSelectionRun.resume_fingerprint == resume_fingerprint,
                MatchSelectionRun.task_id == task_id,
                MatchSelectionRun.lifecycle_status == "committed",
            )
            .order_by(MatchSelectionRun.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_items_for_run(
        self,
        selection_run_id: Any,
    ) -> list[MatchSelectionItem]:
        stmt = (
            select(MatchSelectionItem)
            .where(MatchSelectionItem.selection_run_id == selection_run_id)
            .options(
                joinedload(MatchSelectionItem.job_match).joinedload(JobMatch.job_post)
            )
            .order_by(MatchSelectionItem.rank_position.asc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def publish_selection_run(
        self,
        *,
        owner_id: Any,
        resume_fingerprint: str,
        policy_snapshot: MatchSelectionPolicySnapshot,
        item_snapshots: Iterable[MatchSelectionItemSnapshot],
        job_match_ids_by_job_id: dict[str, str],
        task_id: Optional[str] = None,
    ) -> MatchSelectionRun:
        if task_id:
            existing_for_task = self.get_committed_run_for_task(
                resume_fingerprint=resume_fingerprint,
                task_id=task_id,
            )
            if existing_for_task is not None:
                return existing_for_task

        run = MatchSelectionRun(
            owner_id=owner_id,
            resume_fingerprint=resume_fingerprint,
            task_id=task_id,
            lifecycle_status="pending",
            is_current=False,
            policy_snapshot_json=asdict(policy_snapshot),
            ranking_mode_used=policy_snapshot.ranking_mode_used,
            ranking_config_version=policy_snapshot.ranking_config_version,
            stable_tie_break_key=policy_snapshot.stable_tie_break_key,
            fit_floor_used=policy_snapshot.fit_floor_used,
            notification_fit_floor_used=policy_snapshot.notification_fit_floor_used,
            top_k_used=policy_snapshot.top_k_used,
            candidate_pool_size=policy_snapshot.candidate_pool_size,
            selected_count=policy_snapshot.selected_count,
            alert_candidate_count=policy_snapshot.alert_candidate_count,
            resume_resolution_reason=policy_snapshot.resume_resolution_reason,
        )
        self.db.add(run)
        self.db.flush()

        for item in item_snapshots:
            job_match_id = job_match_ids_by_job_id.get(item.job_id)
            if not job_match_id:
                raise ValueError(f"Missing saved job_match_id for job {item.job_id}")
            self.db.add(
                MatchSelectionItem(
                    selection_run_id=run.id,
                    job_match_id=job_match_id,
                    rank_position=item.rank_position,
                    fit_score_at_selection=item.fit_score_at_selection,
                    preference_score_at_selection=item.preference_score_at_selection,
                    job_similarity_at_selection=item.job_similarity_at_selection,
                    required_coverage_at_selection=item.required_coverage_at_selection,
                    alert_eligible=item.alert_eligible,
                    dominant_reason_code=item.dominant_reason_code,
                    explanation_label=item.explanation_label,
                    ranking_snapshot=item.ranking_snapshot,
                )
            )

        self.db.execute(
            update(MatchSelectionRun)
            .where(
                MatchSelectionRun.resume_fingerprint == resume_fingerprint,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
                MatchSelectionRun.id != run.id,
            )
            .values(is_current=False, lifecycle_status="superseded")
        )
        run.lifecycle_status = "committed"
        run.is_current = True
        self.db.flush()
        return run
