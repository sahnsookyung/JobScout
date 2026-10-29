"""Private additive columns must not mask drift in the public schema contract."""

from unittest.mock import MagicMock, patch

import pytest

from database import bootstrap


@pytest.mark.parametrize("change", [None, "missing", "type", "nullable", "default"])
def test_private_column_is_allowed_but_public_column_drift_fails(change):
    public = {"name": "id", "type": "uuid", "nullable": False, "default": None}
    private = {"name": "retention_claimed_at", "type": "timestamp with time zone", "nullable": True, "default": None}
    expected = {"extensions": [], "enums": [], "indexes": [], "constraints": [],
                "tables": {"users": {"columns": [public]}}}
    columns = [dict(public), private]
    if change == "missing":
        columns = [private]
    elif change:
        columns[0][change] = {"type": "text", "nullable": True, "default": "'unexpected'"}[change]
    actual = {**expected, "tables": {"users": {"columns": columns}}}
    with patch.object(bootstrap, "load", return_value=expected), patch.object(bootstrap, "capture", return_value=actual):
        if change:
            with pytest.raises(bootstrap.DatabaseSchemaError, match="schema drifted"):
                bootstrap._verify_bootstrapped_schema(MagicMock())
        else:
            bootstrap._verify_bootstrapped_schema(MagicMock())
    assert actual['tables']['users']['columns'] == columns
