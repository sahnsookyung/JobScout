from datetime import date, datetime, timezone
from decimal import Decimal

from core.resume_variants.hashing import canonical_json_hash


def test_canonical_json_hash_is_order_independent() -> None:
    left = {"b": [2, 1], "a": {"z": "x"}}
    right = {"a": {"z": "x"}, "b": [2, 1]}

    assert canonical_json_hash(left) == canonical_json_hash(right)


def test_canonical_json_hash_changes_when_resume_content_changes() -> None:
    base = {"profile": {"summary": {"text": "Python engineer"}}}
    changed = {"profile": {"summary": {"text": "Go engineer"}}}

    assert canonical_json_hash(base) != canonical_json_hash(changed)

def test_canonical_json_hash_handles_dates_decimals_and_unknown_objects() -> None:
    class _Custom:
        def __str__(self) -> str:
            return "custom-value"

    value = {
        "date": date(2026, 5, 24),
        "datetime": datetime(2026, 5, 24, 1, 2, 3, tzinfo=timezone.utc),
        "decimal": Decimal("12.34"),
        "custom": _Custom(),
    }

    digest = canonical_json_hash(value)

    assert len(digest) == 64
    assert digest == canonical_json_hash(value)
