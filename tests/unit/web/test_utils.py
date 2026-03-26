"""
Tests for web/backend/utils.py

Covers the four safe conversion helpers: safe_float, safe_int, safe_str,
safe_datetime_iso — including None, Decimal, boundary, and error paths.
"""

from decimal import Decimal
from datetime import datetime

import pytest

from web.backend.utils import safe_float, safe_int, safe_str, safe_datetime_iso


class TestSafeFloat:
    def test_none_returns_zero_default(self):
        assert safe_float(None) == 0.0

    def test_none_with_custom_default(self):
        assert safe_float(None, 99.9) == 99.9

    def test_decimal_value(self):
        assert safe_float(Decimal("3.14")) == pytest.approx(3.14)

    def test_decimal_negative(self):
        assert safe_float(Decimal("-1.5")) == pytest.approx(-1.5)

    def test_int_value(self):
        assert safe_float(5) == 5.0

    def test_float_value(self):
        assert safe_float(2.718) == pytest.approx(2.718)

    def test_numeric_string(self):
        assert safe_float("3.14") == pytest.approx(3.14)

    def test_non_numeric_string_returns_default(self):
        assert safe_float("not_a_number") == 0.0

    def test_non_numeric_string_custom_default(self):
        assert safe_float("bad", -1.0) == -1.0

    def test_unconvertible_type_returns_default(self):
        assert safe_float(object()) == 0.0

    def test_list_returns_default(self):
        assert safe_float([1, 2]) == 0.0


class TestSafeInt:
    def test_none_returns_zero_default(self):
        assert safe_int(None) == 0

    def test_none_with_custom_default(self):
        assert safe_int(None, 42) == 42

    def test_int_value(self):
        assert safe_int(7) == 7

    def test_float_value_truncates(self):
        assert safe_int(3.9) == 3

    def test_numeric_string(self):
        assert safe_int("10") == 10

    def test_non_numeric_string_returns_default(self):
        assert safe_int("abc") == 0

    def test_non_numeric_string_custom_default(self):
        assert safe_int("bad", -1) == -1

    def test_unconvertible_type_returns_default(self):
        assert safe_int(object()) == 0

    def test_list_returns_default(self):
        assert safe_int([1, 2]) == 0


class TestSafeStr:
    def test_none_returns_empty_string(self):
        assert safe_str(None) == ""

    def test_none_with_custom_default(self):
        assert safe_str(None, "fallback") == "fallback"

    def test_string_passthrough(self):
        assert safe_str("hello") == "hello"

    def test_int_to_string(self):
        assert safe_str(42) == "42"

    def test_float_to_string(self):
        assert safe_str(3.14) == "3.14"


class TestSafeDatetimeIso:
    def test_none_returns_none(self):
        assert safe_datetime_iso(None) is None

    def test_datetime_returns_iso_string(self):
        dt = datetime(2026, 1, 15, 12, 30, 0)
        result = safe_datetime_iso(dt)
        assert result == "2026-01-15T12:30:00"

    def test_iso_string_is_str(self):
        result = safe_datetime_iso(datetime(2026, 3, 21, 0, 0, 0))
        assert isinstance(result, str)
