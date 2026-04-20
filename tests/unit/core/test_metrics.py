"""Unit tests for ``core.metrics``.

Verify metric shapes, the `_safe` cardinality bound, and the degrade-reason
classifier. The autouse ``_reset_prometheus_metrics`` fixture in
``tests/conftest.py`` zeros children between tests.
"""

from __future__ import annotations

from prometheus_client import REGISTRY

from core import metrics as m


class TestMetricDeclarations:
    def test_all_six_metrics_registered(self):
        """Registry exposes each declared metric by name (even at zero).

        ``collect()`` returns family names *without* the ``_total`` suffix
        for counters and *without* bucket-variant suffixes for histograms.
        Assert against the bare family names.
        """
        families = {
            "jobscout_scorer_route",
            "jobscout_scorer_degraded_reason",
            "jobscout_evidence_rerank_latency_ms",
            "jobscout_selection_tier_items",
            "jobscout_preference_reranker_status",
            "jobscout_email_verification_events",
        }
        exposed = set()
        for collector in REGISTRY._collector_to_names:
            for fam in collector.collect():
                exposed.add(fam.name)
        missing = families - exposed
        assert not missing, f"metrics missing from registry: {missing}"


class TestSafeCollapse:
    def test_known_values_pass_through(self):
        assert m._safe("remote", m._ROUTE_VALUES) == "remote"
        assert m._safe("primary", m._TIER_VALUES) == "primary"

    def test_unknown_values_collapse_to_other(self):
        assert m._safe("mystery_route", m._ROUTE_VALUES) == "other"
        assert m._safe("", m._EMAIL_EVENTS) == "other"


class TestClassifyDegradeReason:
    def test_none_maps_to_degraded(self):
        assert m._classify_degrade_reason(None) == "degraded"

    def test_remote_phrasings(self):
        assert m._classify_degrade_reason("No remote provider available") == "remote_unavailable"
        assert (
            m._classify_degrade_reason(
                "Remote cross-encoder route requested but no remote provider is configured"
            )
            == "remote_unavailable"
        )

    def test_local_phrasings(self):
        assert (
            m._classify_degrade_reason(
                "Local cross-encoder route requested but local provider is disabled"
            )
            == "provider_disabled"
        )
        assert m._classify_degrade_reason("no local provider configured") == "local_unavailable"

    def test_no_provider_available_sentinel(self):
        assert m._classify_degrade_reason("no_provider_available") == "degraded"

    def test_exception_instances_stringified(self):
        err = RuntimeError("Remote cross-encoder route requested but no remote provider")
        assert m._classify_degrade_reason(err) == "remote_unavailable"

    def test_unknown_phrase_passes_through_for_safe_to_bucket(self):
        # _classify returns the raw string unchanged; _safe then collapses it.
        unclassified = m._classify_degrade_reason("something else entirely")
        assert m._safe(unclassified, m._DEGRADED_REASONS) == "other"


class TestRecordHelpers:
    def _sample(self, name: str, labels: dict[str, str]) -> float:
        return REGISTRY.get_sample_value(name, labels) or 0.0

    def test_record_scorer_route(self):
        before = self._sample("jobscout_scorer_route_total", {"route": "remote"})
        m.record_scorer_route("remote")
        after = self._sample("jobscout_scorer_route_total", {"route": "remote"})
        assert after - before == 1

    def test_record_scorer_route_unknown_collapses(self):
        m.record_scorer_route("mystery")
        assert self._sample("jobscout_scorer_route_total", {"route": "other"}) == 1

    def test_record_scorer_degraded_classifies(self):
        m.record_scorer_degraded(RuntimeError("local provider is disabled"))
        assert (
            self._sample(
                "jobscout_scorer_degraded_reason_total",
                {"reason": "provider_disabled"},
            )
            == 1
        )

    def test_record_selection_tier_defaults_reason(self):
        m.record_selection_tier_item("primary")
        assert (
            self._sample(
                "jobscout_selection_tier_items_total",
                {"tier": "primary", "reason": "none"},
            )
            == 1
        )

    def test_record_selection_tier_preserves_reason(self):
        m.record_selection_tier_item("excluded", "truncated")
        assert (
            self._sample(
                "jobscout_selection_tier_items_total",
                {"tier": "excluded", "reason": "truncated"},
            )
            == 1
        )

    def test_record_preference_status_strips_exception_suffix(self):
        # candidate_preferences.py emits e.g. "runtime_error:TimeoutError";
        # the helper must strip the suffix so it lands in the runtime_error bucket.
        m.record_preference_status(False, "runtime_error:TimeoutError")
        assert (
            self._sample(
                "jobscout_preference_reranker_status_total",
                {"applied": "false", "reason": "runtime_error"},
            )
            == 1
        )

    def test_record_preference_status_applied_true(self):
        m.record_preference_status(True, None)
        assert (
            self._sample(
                "jobscout_preference_reranker_status_total",
                {"applied": "true", "reason": "applied"},
            )
            == 1
        )

    def test_record_email_event(self):
        m.record_email_event("sent")
        assert (
            self._sample("jobscout_email_verification_events_total", {"event": "sent"})
            == 1
        )

    def test_record_email_event_unknown_collapses(self):
        m.record_email_event("double_opt_in")
        assert (
            self._sample("jobscout_email_verification_events_total", {"event": "other"})
            == 1
        )

    def test_evidence_rerank_histogram_observes(self):
        with m.evidence_rerank_latency_ms.time():
            pass
        # Histogram emits a _count series; it must be >= 1 after one observation.
        count = self._sample("jobscout_evidence_rerank_latency_ms_count", {})
        assert count is None or count >= 1
