"""Unit tests for core/utils.py"""

import pytest
from core.utils import cosine_similarity_from_distance, JobFingerprinter


# ---------------------------------------------------------------------------
# cosine_similarity_from_distance
# ---------------------------------------------------------------------------

class TestCosineSimilarityFromDistance:
    def test_zero_distance_gives_similarity_one(self):
        assert cosine_similarity_from_distance(0.0) == pytest.approx(1.0)

    def test_distance_one_gives_similarity_zero(self):
        assert cosine_similarity_from_distance(1.0) == pytest.approx(0.0)

    def test_midpoint_distance(self):
        assert cosine_similarity_from_distance(0.5) == pytest.approx(0.5)

    def test_distance_produces_expected_similarity(self):
        assert cosine_similarity_from_distance(0.2) == pytest.approx(0.8)
        assert cosine_similarity_from_distance(0.8) == pytest.approx(0.2)

    def test_distance_greater_than_one_clipped_to_zero(self):
        # similarity = 1 - 1.5 = -0.5 → clipped to 0.0
        result = cosine_similarity_from_distance(1.5)
        assert result == pytest.approx(0.0)

    def test_distance_two_clipped_to_zero(self):
        result = cosine_similarity_from_distance(2.0)
        assert result == pytest.approx(0.0)

    def test_negative_distance_clipped_to_one(self):
        # similarity = 1 - (-0.5) = 1.5 → clipped to 1.0
        result = cosine_similarity_from_distance(-0.5)
        assert result == pytest.approx(1.0)

    def test_float_input(self):
        assert cosine_similarity_from_distance(0.3) == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# JobFingerprinter.calculate
# ---------------------------------------------------------------------------

class TestJobFingerprinterCalculate:
    def test_same_inputs_same_hash(self):
        h1 = JobFingerprinter.calculate("Google", "Engineer", "NYC")
        h2 = JobFingerprinter.calculate("Google", "Engineer", "NYC")
        assert h1 == h2

    def test_different_inputs_different_hash(self):
        h1 = JobFingerprinter.calculate("Google", "Engineer", "NYC")
        h2 = JobFingerprinter.calculate("Meta", "Engineer", "NYC")
        assert h1 != h2

    def test_case_insensitive(self):
        h1 = JobFingerprinter.calculate("Google", "Engineer", "NYC")
        h2 = JobFingerprinter.calculate("GOOGLE", "ENGINEER", "NYC")
        assert h1 == h2

    def test_strips_whitespace(self):
        h1 = JobFingerprinter.calculate("Google", "Engineer", "NYC")
        h2 = JobFingerprinter.calculate("  Google  ", "  Engineer  ", "  NYC  ")
        assert h1 == h2

    def test_returns_sha256_hex_string(self):
        result = JobFingerprinter.calculate("A", "B", "C")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)


# ---------------------------------------------------------------------------
# JobFingerprinter.normalize_location
# ---------------------------------------------------------------------------

class TestJobFingerprinterNormalizeLocation:
    def test_string_location(self):
        assert JobFingerprinter.normalize_location("New York") == "New York"

    def test_dict_with_city(self):
        assert JobFingerprinter.normalize_location({"city": "Tokyo"}) == "Tokyo"

    def test_dict_with_country_fallback(self):
        assert JobFingerprinter.normalize_location({"country": "Japan"}) == "Japan"

    def test_dict_with_empty_city_falls_back_to_country(self):
        result = JobFingerprinter.normalize_location({"city": None, "country": "Canada"})
        assert result == "Canada"

    def test_dict_with_city_as_list(self):
        # Covers lines 48-50: city is a list, use first element
        result = JobFingerprinter.normalize_location({"city": ["japan", "jp"]})
        assert result == "japan"

    def test_dict_with_no_city_or_country(self):
        assert JobFingerprinter.normalize_location({}) == "Unknown"

    def test_none_input(self):
        result = JobFingerprinter.normalize_location(None)
        assert result == "Unknown"

    def test_list_input(self):
        # Not a dict or string → returns "Unknown"
        result = JobFingerprinter.normalize_location(["Remote"])
        assert result == "Unknown"

    def test_integer_input(self):
        # Not a dict or string → falls through to str(location_text) where location_text="Unknown"
        result = JobFingerprinter.normalize_location(42)
        assert result == "Unknown"
