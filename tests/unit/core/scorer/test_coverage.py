"""Unit tests for scorer coverage diagnostics."""

from types import SimpleNamespace

import pytest

from core.scorer.coverage import calculate_requirement_coverage


def _match(req_type: str, weight: float, similarity: float):
    return SimpleNamespace(
        requirement=SimpleNamespace(req_type=req_type, weight=weight),
        similarity=similarity,
    )


def _missing(req_type: str, weight: float):
    return SimpleNamespace(req_type=req_type, weight=weight)


def test_calculate_requirement_coverage_isolated_by_req_type():
    stats = calculate_requirement_coverage(
        matched_requirements=[
            _match("required", 1.0, 0.9),
            _match("preferred", 2.0, 0.8),
        ],
        missing_requirements=[
            _missing("required", 1.0),
            _missing("preferred", 2.0),
        ],
        req_type="preferred",
        threshold=0.6,
        clamp_similarity=True,
    )

    assert stats["total_weight"] == pytest.approx(4.0)
    assert stats["coverage"] == pytest.approx(0.4)
    assert stats["missing_count"] == pytest.approx(1.0)
    assert stats["missing_ratio"] == pytest.approx(0.5)


def test_calculate_requirement_coverage_zero_when_no_matching_type():
    stats = calculate_requirement_coverage(
        matched_requirements=[_match("required", 1.0, 0.9)],
        missing_requirements=[],
        req_type="preferred",
        threshold=0.6,
        clamp_similarity=True,
    )

    assert stats["total_weight"] == pytest.approx(0.0)
    assert stats["coverage"] == pytest.approx(0.0)
