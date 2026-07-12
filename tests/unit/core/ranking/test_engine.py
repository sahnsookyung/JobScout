"""Unit tests for the ranking engine.

Tests cover:
  - Behavioral correctness for all three ranking modes
  - NULL-awareness: preference_score=None sorts correctly and is preserved
  - Invariants: higher scores never worsen rank within a mode
  - Stable tie-break by id
  - Aggregate logging thresholds
  - Edge cases: empty list, all-NULL, all-equal scores
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pytest

from core.ranking.engine import RankingContext, RankingMode, rank_matches
from core.ranking.policy import RankingConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class FakeMatch:
    """Minimal match object accepted by rank_matches()."""
    id: str
    fit_score: Optional[float] = 0.0
    preference_score: Optional[float] = None
    job_similarity: Optional[float] = 0.0
    ranking_explanation: object = field(default=None, init=False)


def _cfg(**kwargs) -> RankingConfig:
    base = {"balanced_w_pref": 0.6, "balanced_w_fit": 0.4}
    base.update(kwargs)
    return RankingConfig(**base)


def _ctx(mode: RankingMode, **cfg_kwargs) -> RankingContext:
    return RankingContext(mode=mode, config=_cfg(**cfg_kwargs))


# ---------------------------------------------------------------------------
# Empty list
# ---------------------------------------------------------------------------

def test_rank_empty_returns_empty():
    result = rank_matches([], _ctx(RankingMode.BALANCED))
    assert result == []


# ---------------------------------------------------------------------------
# preference_first mode
# ---------------------------------------------------------------------------

class TestPreferenceFirstMode:
    def _run(self, matches):
        return rank_matches(matches, _ctx(RankingMode.PREFERENCE_FIRST))

    def test_higher_pref_sorts_before_lower_pref(self):
        low = FakeMatch("low", fit_score=80, preference_score=30)
        high = FakeMatch("high", fit_score=80, preference_score=90)
        result = self._run([low, high])
        assert result[0].id == "high"
        assert result[1].id == "low"

    def test_null_pref_sorts_after_nonzero_pref(self):
        scored = FakeMatch("scored", fit_score=90, preference_score=10)
        no_pref = FakeMatch("no_pref", fit_score=90, preference_score=None)
        result = self._run([no_pref, scored])
        assert result[0].id == "scored"
        assert result[1].id == "no_pref"

    def test_null_pref_sorts_after_zero_pref(self):
        zero = FakeMatch("zero", fit_score=90, preference_score=0)
        null = FakeMatch("null", fit_score=90, preference_score=None)
        result = self._run([null, zero])
        assert result[0].id == "zero"
        assert result[1].id == "null"

    def test_higher_fit_breaks_equal_pref_tie(self):
        low_fit = FakeMatch("low_fit", fit_score=60, preference_score=80)
        high_fit = FakeMatch("high_fit", fit_score=80, preference_score=80)
        result = self._run([low_fit, high_fit])
        assert result[0].id == "high_fit"

    def test_invariant_increasing_pref_never_worsens_rank(self):
        """Increasing preference_score for a match must not move it down."""
        a = FakeMatch("a", fit_score=70, preference_score=30)
        b = FakeMatch("b", fit_score=70, preference_score=70)
        result = self._run([a, b])
        assert result[0].id == "b"

        # Now give a a higher preference_score than b
        a.preference_score = 90
        result2 = self._run([a, b])
        assert result2[0].id == "a"

    def test_similarity_cannot_reverse_large_pref_gap(self):
        high_sim_low_pref = FakeMatch("h_sim", fit_score=80, preference_score=10, job_similarity=0.99)
        low_sim_high_pref = FakeMatch("l_sim", fit_score=80, preference_score=90, job_similarity=0.01)
        result = self._run([high_sim_low_pref, low_sim_high_pref])
        assert result[0].id == "l_sim"

    def test_attaches_explanation(self):
        m = FakeMatch("m", fit_score=75, preference_score=50)
        rank_matches([m], _ctx(RankingMode.PREFERENCE_FIRST))
        assert m.ranking_explanation is not None
        assert m.ranking_explanation.ranking_mode_used == "preference_first"

    def test_explanation_code_preference_first_when_pref_available(self):
        m = FakeMatch("m", preference_score=50)
        rank_matches([m], _ctx(RankingMode.PREFERENCE_FIRST))
        assert m.ranking_explanation.dominant_reason_code == "preference_first"

    def test_explanation_code_preference_unavailable_when_pref_null(self):
        m = FakeMatch("m", preference_score=None)
        rank_matches([m], _ctx(RankingMode.PREFERENCE_FIRST))
        assert m.ranking_explanation.dominant_reason_code == "preference_unavailable"
        assert "preference_score" in m.ranking_explanation.missing_scores


# ---------------------------------------------------------------------------
# fit_first mode
# ---------------------------------------------------------------------------

class TestFitFirstMode:
    def _run(self, matches):
        return rank_matches(matches, _ctx(RankingMode.FIT_FIRST))

    def test_higher_fit_sorts_first(self):
        low = FakeMatch("low", fit_score=60, preference_score=90)
        high = FakeMatch("high", fit_score=85, preference_score=10)
        result = self._run([low, high])
        assert result[0].id == "high"

    def test_null_fit_treated_as_zero(self):
        no_fit = FakeMatch("no_fit", fit_score=None, preference_score=90)
        scored = FakeMatch("scored", fit_score=10, preference_score=0)
        result = self._run([no_fit, scored])
        assert result[0].id == "scored"

    def test_pref_breaks_equal_fit_tie(self):
        low_pref = FakeMatch("low_pref", fit_score=80, preference_score=20)
        high_pref = FakeMatch("high_pref", fit_score=80, preference_score=80)
        result = self._run([low_pref, high_pref])
        assert result[0].id == "high_pref"

    def test_null_pref_sorts_after_nonzero_pref_when_fit_equal(self):
        scored_pref = FakeMatch("scored", fit_score=80, preference_score=50)
        no_pref = FakeMatch("no_pref", fit_score=80, preference_score=None)
        result = self._run([no_pref, scored_pref])
        assert result[0].id == "scored"

    def test_invariant_increasing_fit_never_worsens_rank(self):
        a = FakeMatch("a", fit_score=50, preference_score=90)
        b = FakeMatch("b", fit_score=80, preference_score=10)
        result = self._run([a, b])
        assert result[0].id == "b"

        # Give a better fit than b
        a.fit_score = 90
        result2 = self._run([a, b])
        assert result2[0].id == "a"

    def test_similarity_cannot_reverse_large_fit_gap(self):
        high_sim_low_fit = FakeMatch("h_sim", fit_score=50, preference_score=50, job_similarity=0.99)
        low_sim_high_fit = FakeMatch("l_sim", fit_score=90, preference_score=50, job_similarity=0.01)
        result = self._run([high_sim_low_fit, low_sim_high_fit])
        assert result[0].id == "l_sim"

    def test_explanation_code_is_fit_first(self):
        m = FakeMatch("m", fit_score=70, preference_score=50)
        rank_matches([m], _ctx(RankingMode.FIT_FIRST))
        assert m.ranking_explanation.dominant_reason_code == "fit_first"


# ---------------------------------------------------------------------------
# balanced mode
# ---------------------------------------------------------------------------

class TestBalancedMode:
    def _ctx(self, w_pref=0.6, w_fit=0.4):
        return _ctx(RankingMode.BALANCED, balanced_w_pref=w_pref, balanced_w_fit=w_fit)

    def _run(self, matches, w_pref=0.6, w_fit=0.4):
        return rank_matches(matches, self._ctx(w_pref=w_pref, w_fit=w_fit))

    def test_blend_formula_determines_order(self):
        # primary_a = 0.6*0.9 + 0.4*(50/100) = 0.54 + 0.20 = 0.74
        # primary_b = 0.6*0.4 + 0.4*(90/100) = 0.24 + 0.36 = 0.60
        a = FakeMatch("a", fit_score=50, preference_score=90)
        b = FakeMatch("b", fit_score=90, preference_score=40)
        result = self._run([a, b])
        assert result[0].id == "a"

    def test_null_pref_treated_as_zero_in_blend(self):
        # With pref=None → 0.0: primary = 0.6*0 + 0.4*(80/100) = 0.32
        # b: primary = 0.6*0.4 + 0.4*(80/100) = 0.24 + 0.32 = 0.56
        null_pref = FakeMatch("null", fit_score=80, preference_score=None)
        has_pref = FakeMatch("has_pref", fit_score=80, preference_score=40)
        result = self._run([null_pref, has_pref])
        assert result[0].id == "has_pref"

    def test_null_pref_recorded_in_missing_scores(self):
        m = FakeMatch("m", fit_score=80, preference_score=None)
        self._run([m])
        assert "preference_score" in m.ranking_explanation.missing_scores

    def test_null_pref_preserved_in_explanation(self):
        """NULL is substituted as 0.0 in the blend but preserved in explanation."""
        m = FakeMatch("m", fit_score=80, preference_score=None)
        self._run([m])
        assert m.ranking_explanation.preference_score is None

    def test_balanced_primary_score_in_explanation(self):
        m = FakeMatch("m", fit_score=80, preference_score=50)
        # primary = 0.6*0.5 + 0.4*0.8 = 0.30 + 0.32 = 0.62
        self._run([m])
        assert m.ranking_explanation.balanced_primary_score == pytest.approx(0.62, abs=1e-6)

    def test_balanced_primary_score_null_when_pref_null(self):
        m = FakeMatch("m", fit_score=80, preference_score=None)
        # primary = 0.6*0.0 + 0.4*0.8 = 0.32 (still computed; NULL substituted as 0)
        self._run([m])
        # balanced_primary_score is set even when pref is NULL (uses 0.0 in blend)
        assert m.ranking_explanation.balanced_primary_score == pytest.approx(0.32, abs=1e-6)

    def test_explanation_code_balanced_blend(self):
        m = FakeMatch("m", fit_score=70, preference_score=60)
        self._run([m])
        assert m.ranking_explanation.dominant_reason_code == "balanced_blend"

    def test_invariant_increasing_pref_never_worsens_balanced_rank(self):
        a = FakeMatch("a", fit_score=70, preference_score=20)
        b = FakeMatch("b", fit_score=70, preference_score=80)
        result = self._run([a, b])
        assert result[0].id == "b"

        a.preference_score = 95
        result2 = self._run([a, b])
        assert result2[0].id == "a"

    def test_invariant_increasing_fit_never_worsens_balanced_rank(self):
        a = FakeMatch("a", fit_score=40, preference_score=50)
        b = FakeMatch("b", fit_score=80, preference_score=50)
        result = self._run([a, b])
        assert result[0].id == "b"

        a.fit_score = 95
        result2 = self._run([a, b])
        assert result2[0].id == "a"

    def test_weights_50_50(self):
        # primary_a = 0.5*0.8 + 0.5*(60/100) = 0.40 + 0.30 = 0.70
        # primary_b = 0.5*0.6 + 0.5*(80/100) = 0.30 + 0.40 = 0.70 → tie → stable by id
        a = FakeMatch("a", fit_score=60, preference_score=80)
        b = FakeMatch("b", fit_score=80, preference_score=60)
        result = self._run([a, b], w_pref=0.5, w_fit=0.5)
        # Tied primary; stable tie-break by id string: "a" < "b"
        assert result[0].id == "a"
        assert result[1].id == "b"


# ---------------------------------------------------------------------------
# Stable tie-break
# ---------------------------------------------------------------------------

class TestStableTieBreak:
    def test_equal_scores_use_id_as_tiebreak(self):
        matches = [
            FakeMatch("zzz", fit_score=80, preference_score=50, job_similarity=0.5),
            FakeMatch("aaa", fit_score=80, preference_score=50, job_similarity=0.5),
            FakeMatch("mmm", fit_score=80, preference_score=50, job_similarity=0.5),
        ]
        result = rank_matches(matches, _ctx(RankingMode.BALANCED))
        assert [m.id for m in result] == ["aaa", "mmm", "zzz"]

    def test_stable_across_modes(self):
        m1 = FakeMatch("b_id", fit_score=50, preference_score=None)
        m2 = FakeMatch("a_id", fit_score=50, preference_score=None)
        for mode in list(RankingMode):
            result = rank_matches([m1, m2], _ctx(mode))
            assert result[0].id == "a_id", f"Mode {mode}: expected a_id first"


# ---------------------------------------------------------------------------
# Score normalisation
# ---------------------------------------------------------------------------

class TestScoreNormalisation:
    def test_null_job_similarity_treated_as_zero(self):
        m = FakeMatch("m", fit_score=70, preference_score=50, job_similarity=None)
        rank_matches([m], _ctx(RankingMode.BALANCED))
        assert m.ranking_explanation.similarity_score == pytest.approx(0.0)

    def test_fit_score_clamped_to_0_1(self):
        m = FakeMatch("m", fit_score=200)  # > 100, should clamp to 1.0
        rank_matches([m], _ctx(RankingMode.BALANCED))
        expl = m.ranking_explanation
        assert expl.fit_score == pytest.approx(1.0)

    def test_fit_score_zero_when_negative(self):
        m = FakeMatch("m", fit_score=-10)
        rank_matches([m], _ctx(RankingMode.BALANCED))
        assert m.ranking_explanation.fit_score == pytest.approx(0.0)

    def test_preference_score_clamped_to_0_100(self):
        m = FakeMatch("m", preference_score=150)
        rank_matches([m], _ctx(RankingMode.BALANCED))
        assert m.ranking_explanation.preference_score == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Aggregate logging
# ---------------------------------------------------------------------------

class TestAggregateLogging:
    def test_debug_when_null_ratio_below_50_percent(self, caplog):
        matches = [
            FakeMatch("a", preference_score=50),
            FakeMatch("b", preference_score=50),
            FakeMatch("c", preference_score=None),  # 1/3 ≈ 33% → DEBUG
        ]
        with caplog.at_level(logging.DEBUG, logger="core.ranking.engine"):
            rank_matches(matches, _ctx(RankingMode.BALANCED))

        assert any("1/3" in r.message for r in caplog.records)
        assert all(r.levelname != "WARNING" for r in caplog.records if "NULL preference_score" in r.message)

    def test_warning_when_null_ratio_above_50_percent(self, caplog):
        matches = [
            FakeMatch("a", preference_score=None),
            FakeMatch("b", preference_score=None),
            FakeMatch("c", preference_score=50),  # 2/3 ≈ 67% → WARNING
        ]
        with caplog.at_level(logging.WARNING, logger="core.ranking.engine"):
            rank_matches(matches, _ctx(RankingMode.BALANCED))

        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warning_records) >= 1

    def test_no_log_when_all_pref_present(self, caplog):
        matches = [FakeMatch("a", preference_score=50), FakeMatch("b", preference_score=80)]
        with caplog.at_level(logging.DEBUG, logger="core.ranking.engine"):
            rank_matches(matches, _ctx(RankingMode.BALANCED))

        assert not any("NULL preference_score" in r.message for r in caplog.records)

    def test_no_log_for_empty_list(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="core.ranking.engine"):
            rank_matches([], _ctx(RankingMode.BALANCED))
        assert caplog.records == []


# ---------------------------------------------------------------------------
# All-NULL preference scores
# ---------------------------------------------------------------------------

class TestAllNullPreferenceScores:
    def test_all_null_preference_first_orders_by_fit(self):
        matches = [
            FakeMatch("low", fit_score=60, preference_score=None),
            FakeMatch("high", fit_score=90, preference_score=None),
        ]
        result = rank_matches(matches, _ctx(RankingMode.PREFERENCE_FIRST))
        # All NULL → (is_null=True, -(0.0), -fit, -sim, stable)
        # high fit should still rank first as secondary dimension
        assert result[0].id == "high"

    def test_all_null_preference_balanced_orders_by_fit_only(self):
        matches = [
            FakeMatch("low", fit_score=40, preference_score=None),
            FakeMatch("high", fit_score=80, preference_score=None),
        ]
        # primary = 0.6*0 + 0.4*fit
        result = rank_matches(matches, _ctx(RankingMode.BALANCED))
        assert result[0].id == "high"


# ---------------------------------------------------------------------------
# RankingConfig weight validation
# ---------------------------------------------------------------------------

class TestRankingConfigValidation:
    def test_invalid_weights_raise_at_init(self):
        with pytest.raises(ValueError, match="must equal 1.0"):
            RankingConfig(balanced_w_pref=0.7, balanced_w_fit=0.4)

    def test_valid_weights_do_not_raise(self):
        cfg = RankingConfig(balanced_w_pref=0.3, balanced_w_fit=0.7)
        assert cfg.balanced_w_pref == pytest.approx(0.3)

    def test_effective_top_k_respects_max(self):
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4, max_top_k=50, default_top_k=25)
        assert cfg.effective_top_k(100) == 50
        assert cfg.effective_top_k(None) == 25
        assert cfg.effective_top_k(10) == 10
