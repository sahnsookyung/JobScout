"""Contracts for committed match-selection runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from core.ranking import RankingContext


@dataclass(frozen=True)
class MatchSelectionPolicySnapshot:
    policy_snapshot_version: str
    ranking_mode_used: str
    ranking_config_version: str
    stable_tie_break_key: str
    fit_floor_used: float
    required_coverage_floor_used: Optional[float]
    notification_fit_floor_used: float
    top_k_used: int
    candidate_pool_size: int
    selected_count: int
    alert_candidate_count: int
    resume_resolution_reason: str
    task_id: Optional[str] = None
    ranking_config_snapshot: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_ranking_context(
        cls,
        *,
        ranking_context: RankingContext,
        fit_floor_used: float,
        required_coverage_floor_used: Optional[float],
        notification_fit_floor_used: float,
        top_k_used: int,
        candidate_pool_size: int,
        selected_count: int,
        alert_candidate_count: int,
        resume_resolution_reason: str,
        task_id: Optional[str] = None,
    ) -> "MatchSelectionPolicySnapshot":
        config = ranking_context.config
        return cls(
            policy_snapshot_version="2026-04-09.v1",
            ranking_mode_used=ranking_context.mode.value,
            ranking_config_version=config.config_version,
            stable_tie_break_key=config.stable_tie_break_key,
            fit_floor_used=fit_floor_used,
            required_coverage_floor_used=required_coverage_floor_used,
            notification_fit_floor_used=notification_fit_floor_used,
            top_k_used=top_k_used,
            candidate_pool_size=candidate_pool_size,
            selected_count=selected_count,
            alert_candidate_count=alert_candidate_count,
            resume_resolution_reason=resume_resolution_reason,
            task_id=task_id,
            ranking_config_snapshot={
                "active_default_mode": config.active_default_mode,
                "balanced_w_pref": config.balanced_w_pref,
                "balanced_w_fit": config.balanced_w_fit,
                "stable_tie_break_key": config.stable_tie_break_key,
                "config_version": config.config_version,
            },
        )


@dataclass(frozen=True)
class MatchSelectionItemSnapshot:
    job_id: str
    rank_position: int
    fit_score_at_selection: float
    preference_score_at_selection: Optional[float]
    job_similarity_at_selection: float
    required_coverage_at_selection: float
    alert_eligible: bool
    dominant_reason_code: Optional[str]
    explanation_label: Optional[str]
    ranking_snapshot: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MatchSelectionResult:
    selected_matches: list[Any]
    item_snapshots: list[MatchSelectionItemSnapshot]
    policy_snapshot: MatchSelectionPolicySnapshot
