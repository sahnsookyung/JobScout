from unittest.mock import MagicMock

from database.repositories.user_feature_capability import UserFeatureCapabilityRepository


def test_get_capability_returns_scalar_result():
    db = MagicMock()
    expected = object()
    db.execute.return_value.scalar_one_or_none.return_value = expected
    repo = UserFeatureCapabilityRepository(db)

    result = repo.get_capability("user-1", "fit.semantic.allowed_modes")

    assert result is expected
    db.execute.assert_called_once()


def test_upsert_capability_updates_existing_row():
    db = MagicMock()
    repo = UserFeatureCapabilityRepository(db)
    existing = MagicMock()
    repo.get_capability = MagicMock(return_value=existing)

    result = repo.upsert_capability(
        "user-1",
        "fit.semantic.preferred_mode",
        enabled=False,
        value_json={"mode": "cross_encoder"},
        source="test",
    )

    assert result is existing
    assert existing.enabled is False
    assert existing.value_json == {"mode": "cross_encoder"}
    assert existing.source == "test"
    db.add.assert_not_called()
    db.flush.assert_called_once()


def test_upsert_capability_creates_new_row_when_missing():
    db = MagicMock()
    repo = UserFeatureCapabilityRepository(db)
    repo.get_capability = MagicMock(return_value=None)

    result = repo.upsert_capability(
        "user-1",
        "fit.semantic.allowed_modes",
        value_json={"modes": ["cross_encoder"]},
        source="seed",
    )

    assert result.owner_id == "user-1"
    assert result.feature_key == "fit.semantic.allowed_modes"
    assert result.enabled is True
    assert result.value_json == {"modes": ["cross_encoder"]}
    assert result.source == "seed"
    db.add.assert_called_once_with(result)
    db.flush.assert_called_once()
