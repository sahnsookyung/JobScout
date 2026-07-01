import base64

import pytest

from web.backend.services.cursors import CursorDecodeError, MatchCursorCodec


def test_cursor_round_trip_preserves_payload():
    cursor = MatchCursorCodec.encode(
        "pipeline_runs",
        created_at="2026-07-01T12:00:00+00:00",
        id="11111111-1111-1111-1111-111111111111",
    )

    decoded = MatchCursorCodec.decode(cursor, expected_kind="pipeline_runs")

    assert decoded == {
        "v": 1,
        "kind": "pipeline_runs",
        "created_at": "2026-07-01T12:00:00+00:00",
        "id": "11111111-1111-1111-1111-111111111111",
    }


def test_decode_empty_cursor_returns_none():
    assert MatchCursorCodec.decode(None, expected_kind="matches") is None
    assert MatchCursorCodec.decode("", expected_kind="matches") is None


@pytest.mark.parametrize(
    "cursor",
    [
        "not-valid-base64!",
        base64.urlsafe_b64encode(b"[]").decode("ascii").rstrip("="),
    ],
)
def test_decode_rejects_invalid_payloads(cursor):
    with pytest.raises(CursorDecodeError, match="Invalid cursor"):
        MatchCursorCodec.decode(cursor, expected_kind="matches")


def test_decode_rejects_wrong_version_or_kind():
    wrong_kind = MatchCursorCodec.encode("jobs", id="job-1")

    with pytest.raises(CursorDecodeError, match="Cursor does not apply"):
        MatchCursorCodec.decode(wrong_kind, expected_kind="matches")

    wrong_version = base64.urlsafe_b64encode(
        b'{"v":2,"kind":"matches","id":"match-1"}',
    ).decode("ascii").rstrip("=")

    with pytest.raises(CursorDecodeError, match="Cursor does not apply"):
        MatchCursorCodec.decode(wrong_version, expected_kind="matches")
