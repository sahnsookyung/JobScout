from core.resume_variants.hashing import canonical_json_hash


def test_canonical_json_hash_is_order_independent() -> None:
    left = {"b": [2, 1], "a": {"z": "x"}}
    right = {"a": {"z": "x"}, "b": [2, 1]}

    assert canonical_json_hash(left) == canonical_json_hash(right)


def test_canonical_json_hash_changes_when_resume_content_changes() -> None:
    base = {"profile": {"summary": {"text": "Python engineer"}}}
    changed = {"profile": {"summary": {"text": "Go engineer"}}}

    assert canonical_json_hash(base) != canonical_json_hash(changed)
