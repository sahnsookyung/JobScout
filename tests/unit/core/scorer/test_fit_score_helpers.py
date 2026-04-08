"""Unit tests for fit_score helper functions (uncovered branches)."""

import pytest
from types import SimpleNamespace

from core.scorer.fit_score import (
    _warn_correct,
    _cfg_float,
    _cfg_bool,
    _as_requirement,
    _as_match,
    calculate_fit_score,
)
from core.config_loader import ScorerConfig


# ---------------------------------------------------------------------------
# _warn_correct
# ---------------------------------------------------------------------------

class TestWarnCorrect:
    def test_logs_warning_when_values_differ(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="core.scorer.fit_score"):
            _warn_correct("my_param", 1.5, 1.0)
        assert "my_param" in caplog.text

    def test_no_log_when_values_same(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="core.scorer.fit_score"):
            _warn_correct("my_param", 0.5, 0.5)
        assert caplog.text == ""


# ---------------------------------------------------------------------------
# _cfg_float — exception path
# ---------------------------------------------------------------------------

class TestCfgFloat:
    def test_returns_default_when_conversion_fails(self):
        config = SimpleNamespace(bad_val="not_a_float")
        result = _cfg_float(config, "bad_val", 0.7)
        assert result == pytest.approx(0.7)

    def test_returns_float_when_valid(self):
        config = SimpleNamespace(good_val="0.3")
        result = _cfg_float(config, "good_val", 0.7)
        assert result == pytest.approx(0.3)

    def test_uses_default_when_attr_missing(self):
        config = SimpleNamespace()
        result = _cfg_float(config, "missing_attr", 0.5)
        assert result == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# _cfg_bool — int/float and string branches
# ---------------------------------------------------------------------------

class TestCfgBool:
    def test_int_truthy(self):
        config = SimpleNamespace(flag=1)
        assert _cfg_bool(config, "flag", False) is True

    def test_int_falsy(self):
        config = SimpleNamespace(flag=0)
        assert _cfg_bool(config, "flag", True) is False

    def test_float_truthy(self):
        config = SimpleNamespace(flag=1.5)
        assert _cfg_bool(config, "flag", False) is True

    def test_float_falsy(self):
        config = SimpleNamespace(flag=0.0)
        assert _cfg_bool(config, "flag", True) is False

    def test_string_true(self):
        for val in ("true", "1", "yes", "y", "on", "TRUE", "Yes"):
            config = SimpleNamespace(flag=val)
            assert _cfg_bool(config, "flag", False) is True, f"Expected True for {val!r}"

    def test_string_false(self):
        for val in ("false", "0", "no", "n", "off", "FALSE", "No"):
            config = SimpleNamespace(flag=val)
            assert _cfg_bool(config, "flag", True) is False, f"Expected False for {val!r}"

    def test_invalid_string_returns_default(self):
        config = SimpleNamespace(flag="maybe")
        assert _cfg_bool(config, "flag", True) is True

    def test_bool_true_passthrough(self):
        config = SimpleNamespace(flag=True)
        assert _cfg_bool(config, "flag", False) is True

    def test_bool_false_passthrough(self):
        config = SimpleNamespace(flag=False)
        assert _cfg_bool(config, "flag", True) is False


# ---------------------------------------------------------------------------
# _as_requirement — unscored req_type and invalid weight
# ---------------------------------------------------------------------------

class TestAsRequirement:
    def test_unscored_req_type_returns_weight_zero(self):
        req = SimpleNamespace(req_type="responsibility", weight=1.0)
        result = _as_requirement(req)
        assert result.req_type == "responsibility"
        assert result.weight == pytest.approx(0.0)

    def test_constraint_req_type_returns_weight_zero(self):
        req = SimpleNamespace(req_type="constraint", weight=2.0)
        result = _as_requirement(req)
        assert result.weight == pytest.approx(0.0)

    def test_invalid_weight_defaults_to_one(self):
        req = SimpleNamespace(req_type="required", weight="not_a_number")
        result = _as_requirement(req)
        assert result.weight == pytest.approx(1.0)

    def test_negative_weight_clamped_to_zero(self):
        req = SimpleNamespace(req_type="required", weight=-2.0)
        result = _as_requirement(req)
        assert result.weight == pytest.approx(0.0)

    def test_match_like_object_uses_requirement_field(self):
        inner = SimpleNamespace(req_type="preferred", weight=0.5)
        match_obj = SimpleNamespace(requirement=inner, similarity=0.8)
        result = _as_requirement(match_obj)
        assert result.req_type == "preferred"
        assert result.weight == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# _as_match — no similarity attribute and invalid similarity
# ---------------------------------------------------------------------------

class TestAsMatch:
    def test_object_without_similarity_uses_default(self):
        req = SimpleNamespace(req_type="required", weight=1.0)  # no .similarity
        result = _as_match(req, default_similarity=0.5)
        assert result.similarity == pytest.approx(0.5)

    def test_invalid_similarity_defaults_to_zero(self):
        obj = SimpleNamespace(req_type="required", weight=1.0, similarity="invalid")
        result = _as_match(obj, default_similarity=0.0)
        assert result.similarity == pytest.approx(0.0)

    def test_valid_similarity_used(self):
        obj = SimpleNamespace(req_type="required", weight=1.0, similarity=0.75)
        result = _as_match(obj)
        assert result.similarity == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# calculate_fit_score — edge cases
# ---------------------------------------------------------------------------

def _default_config(**kwargs):
    return ScorerConfig(**kwargs)


class TestCalculateFitScoreEdgeCases:
    def test_invalid_job_similarity_defaults_to_zero(self):
        config = _default_config()
        score, components = calculate_fit_score(
            job_similarity="not_a_number",
            matched_requirements=[],
            missing_requirements=[],
            fit_penalties=0.0,
            config=config,
        )
        assert components["job_similarity"] == pytest.approx(0.0)
        assert score >= 0.0

    def test_degenerate_config_denom_zero_returns_zero_core(self):
        # w_req=0, w_sim=0 → denom=0 → core=0
        # ScorerConfig doesn't have job_similarity_weight so use SimpleNamespace
        config = SimpleNamespace(
            weight_required=0.0,
            job_similarity_weight=0.0,
            req_similarity_threshold=0.6,
            similarity_clamp=True,
            missing_required_penalty_max=0.0,
            per_missing_required_penalty=0.0,
            missing_required_penalty_cap=0.0,
            enable_explicit_missing_required_penalty=False,
        )
        score, _ = calculate_fit_score(
            job_similarity=1.0,
            matched_requirements=[],
            missing_requirements=[],
            fit_penalties=0.0,
            config=config,
        )
        # denom=0 → core=0, no preferred bonus → score=0
        assert score == pytest.approx(0.0)

    def test_missing_required_penalty_disabled(self):
        config = _default_config(
            enable_explicit_missing_required_penalty=False,
            missing_required_penalty_max=50.0,
            per_missing_required_penalty=10.0,
        )
        # Build a missing_required match
        missing = SimpleNamespace(req_type="required", weight=1.0)
        score_disabled, _ = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=[],
            missing_requirements=[missing],
            fit_penalties=0.0,
            config=config,
        )
        config_enabled = _default_config(
            enable_explicit_missing_required_penalty=True,
            missing_required_penalty_max=50.0,
            per_missing_required_penalty=10.0,
        )
        score_enabled, _ = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=[],
            missing_requirements=[missing],
            fit_penalties=0.0,
            config=config_enabled,
        )
        # Disabled penalty should give same or higher score than enabled
        assert score_disabled >= score_enabled

    def test_invalid_fit_penalties_defaults_to_zero(self):
        config = _default_config()
        score_invalid, _ = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=[],
            missing_requirements=[],
            fit_penalties="bad_value",
            config=config,
        )
        score_zero, _ = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=[],
            missing_requirements=[],
            fit_penalties=0.0,
            config=config,
        )
        # Invalid fit_penalties treated as 0.0 → same score
        assert score_invalid == pytest.approx(score_zero)

    def test_required_coverage_counts_missing_required_weight(self):
        config = _default_config(enable_explicit_missing_required_penalty=False)
        matched = [
            SimpleNamespace(
                requirement=SimpleNamespace(req_type="required", weight=1.0),
                similarity=0.9,
            )
        ]
        missing = [SimpleNamespace(req_type="required", weight=1.0)]

        _, components = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=matched,
            missing_requirements=missing,
            fit_penalties=0.0,
            config=config,
        )

        assert components["total_required_weight"] == pytest.approx(2.0)
        assert components["required_coverage"] == pytest.approx(0.45)

    def test_preferred_matches_do_not_increase_fit_score(self):
        config = _default_config(enable_explicit_missing_required_penalty=False)
        required_only = [
            SimpleNamespace(
                requirement=SimpleNamespace(req_type="required", weight=1.0),
                similarity=0.9,
            )
        ]
        with_preferred = required_only + [
            SimpleNamespace(
                requirement=SimpleNamespace(req_type="preferred", weight=1.0),
                similarity=0.95,
            )
        ]

        score_required_only, _ = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=required_only,
            missing_requirements=[],
            fit_penalties=0.0,
            config=config,
        )
        score_with_preferred, components = calculate_fit_score(
            job_similarity=0.8,
            matched_requirements=with_preferred,
            missing_requirements=[],
            fit_penalties=0.0,
            config=config,
        )

        assert score_with_preferred == pytest.approx(score_required_only)
        assert components["preferred_requirement_coverage"] == pytest.approx(0.95)
