#!/usr/bin/env python3
"""Offline fit-semantics evaluation harness.

This harness is intentionally Python-only because it exercises the backend
retrieval and scoring code directly:
- semantic requirement/evidence judgments
- hybrid retrieval fusion behavior
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from core.config_loader import MatcherConfig, load_config
from core.llm.fake_service import FakeLLMService
from core.matcher.models import (
    JobMatchPreliminary,
    RequirementEvidenceCandidate,
    RequirementMatchResult,
)
from core.matcher.service import MatcherService
from core.scorer.semantic_fit import (
    CrossEncoderSemanticFitScorer,
    LLMSemanticFitScorer,
    LocalCrossEncoderProvider,
    ThresholdSemanticFitScorer,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "evaluations" / "fit_semantics_cases.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate fit-semantics scoring and retrieval behavior.")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_FIXTURE_PATH,
        help="Path to the fit evaluation fixture JSON.",
    )
    parser.add_argument(
        "--mode",
        choices=["cross_encoder", "llm", "threshold"],
        default="cross_encoder",
        help="Scoring mode to evaluate.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "config.yaml",
        help="Config file used to build matcher/scorer settings.",
    )
    parser.add_argument(
        "--allow-failures",
        action="store_true",
        help="Exit zero even when evaluation cases fail; useful for exploratory reporting.",
    )
    return parser.parse_args()


def _make_requirement(req_id: str, text: str, req_type: str) -> Any:
    return SimpleNamespace(id=req_id, text=text, req_type=req_type, weight=1.0)


def _make_evidence(text: str, section: str) -> Any:
    return SimpleNamespace(text=text, source_section=section)


def _pair_preliminary(case: dict[str, Any]) -> JobMatchPreliminary:
    job = SimpleNamespace(
        id=f"job-{case['name']}",
        title=case["job_title"],
        company=case["job_company"],
        canonical_job_summary=case["job_summary"],
        description=case["job_summary"],
    )
    evidence = _make_evidence(case["evidence_text"], case["evidence_section"])
    requirement_match = RequirementMatchResult(
        requirement=_make_requirement(
            req_id=f"req-{case['name']}",
            text=case["requirement_text"],
            req_type=case["req_type"],
        ),
        evidence=evidence,
        similarity=float(case["original_similarity"]),
        is_covered=float(case["original_similarity"]) >= 0.5,
        evidence_candidates=[
            RequirementEvidenceCandidate(
                evidence=evidence,
                similarity=float(case["original_similarity"]),
                rank=1,
            )
        ],
    )
    matched = [requirement_match] if requirement_match.is_covered else []
    missing = [] if requirement_match.is_covered else [requirement_match]
    return JobMatchPreliminary(
        job=job,
        job_similarity=0.75,
        requirement_matches=matched,
        missing_requirements=missing,
        resume_fingerprint=f"fixture-{case['name']}",
        retrieval_score=0.75,
    )


def _build_scorer(mode: str, config) -> Any:
    if mode == "threshold":
        return ThresholdSemanticFitScorer()
    if mode == "llm":
        return LLMSemanticFitScorer(FakeLLMService())
    local_provider = LocalCrossEncoderProvider(
        model_name=config.matching.scorer.semantic_fit.cross_encoder.local.model_name,
        cache_path=config.matching.scorer.semantic_fit.cross_encoder.local.model_cache_path,
        runtime=config.matching.scorer.semantic_fit.cross_encoder.local.runtime,
        max_batch_size=config.matching.scorer.semantic_fit.cross_encoder.local.max_batch_size,
        trust_remote_code=config.matching.scorer.semantic_fit.cross_encoder.local.trust_remote_code,
    )
    return CrossEncoderSemanticFitScorer(
        local_provider=local_provider,
        remote_provider=None,
        fallback_scorer=ThresholdSemanticFitScorer(),
    )


def _evaluate_pair_cases(cases: list[dict[str, Any]], *, scorer: Any, scorer_config: Any) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    correct = 0
    for case in cases:
        preliminary = _pair_preliminary(case)
        score_result = scorer.score(
            preliminary,
            fit_penalties=0.0,
            config=scorer_config,
        )
        verdict = score_result.fit_explanation["requirement_verdicts"][0]["verdict"]
        passed = verdict == case["expected_verdict"]
        correct += int(passed)
        results.append(
            {
                "name": case["name"],
                "expected_verdict": case["expected_verdict"],
                "actual_verdict": verdict,
                "passed": passed,
                "effective_fit_mode": score_result.fit_components.get("effective_fit_mode"),
                "provider_route": score_result.fit_components.get("provider_route"),
            }
        )
    return {
        "passed": correct,
        "total": len(cases),
        "accuracy": (correct / len(cases)) if cases else 0.0,
        "results": results,
    }


def _evaluate_retrieval_cases(cases: list[dict[str, Any]], *, matcher_config: MatcherConfig) -> dict[str, Any]:
    matcher = MatcherService(resume_profiler=SimpleNamespace(), config=matcher_config)
    results: list[dict[str, Any]] = []
    correct = 0
    for case in cases:
        jobs = {
            item["job_id"]: SimpleNamespace(id=item["job_id"])
            for item in case["dense_candidates"] + case["lexical_candidates"]
        }
        dense_pairs = [
            (jobs[item["job_id"]], float(item["dense_score"]))
            for item in case["dense_candidates"]
        ]
        lexical_pairs = [
            (
                jobs[item["job_id"]],
                float(item["lexical_score"]),
                float(item["dense_similarity"]),
            )
            for item in case["lexical_candidates"]
        ]
        fused = matcher._fuse_candidates(dense_pairs, lexical_pairs)
        actual_top_job_id = str(fused[0].job.id) if fused else None
        passed = actual_top_job_id == case["expected_top_job_id"]
        correct += int(passed)
        results.append(
            {
                "name": case["name"],
                "expected_top_job_id": case["expected_top_job_id"],
                "actual_top_job_id": actual_top_job_id,
                "passed": passed,
            }
        )
    return {
        "passed": correct,
        "total": len(cases),
        "accuracy": (correct / len(cases)) if cases else 0.0,
        "results": results,
    }


def main() -> int:
    args = _parse_args()
    config = load_config(args.config)
    with args.fixture.open("r", encoding="utf-8") as handle:
        fixture = json.load(handle)

    scorer = _build_scorer(args.mode, config)
    pair_summary = _evaluate_pair_cases(
        fixture.get("pair_cases", []),
        scorer=scorer,
        scorer_config=config.matching.scorer,
    )
    retrieval_summary = _evaluate_retrieval_cases(
        fixture.get("retrieval_cases", []),
        matcher_config=config.matching.matcher,
    )

    report = {
        "mode": args.mode,
        "fixture": str(args.fixture),
        "pair_summary": pair_summary,
        "retrieval_summary": retrieval_summary,
        "all_passed": pair_summary["passed"] == pair_summary["total"]
        and retrieval_summary["passed"] == retrieval_summary["total"],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["all_passed"] and not args.allow_failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
