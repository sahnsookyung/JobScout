"""Unit tests for database/repositories/match.py"""

import pytest
from unittest.mock import MagicMock

from database.repositories.match import MatchRepository
from database.models import JobMatch


def make_repo():
    mock_db = MagicMock()
    return MatchRepository(mock_db), mock_db


def make_match(job_post_id="job-1", fingerprint="fp-1", status="active", is_hidden=False):
    m = MagicMock(spec=JobMatch)
    m.id = "match-id-1"
    m.job_post_id = job_post_id
    m.resume_fingerprint = fingerprint
    m.status = status
    m.is_hidden = is_hidden
    m.overall_score = 80.0
    return m


# ---------------------------------------------------------------------------
# get_existing_match
# ---------------------------------------------------------------------------

class TestGetExistingMatch:
    def test_returns_match_when_found(self):
        repo, mock_db = make_repo()
        expected = make_match()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected

        result = repo.get_existing_match("job-1", "fp-1")
        assert result is expected

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_existing_match("job-1", "fp-1")
        assert result is None

    def test_with_load_job_post_true_still_returns_result(self):
        """load_job_post=True works correctly and returns the match."""
        repo, mock_db = make_repo()
        expected = make_match()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected

        result = repo.get_existing_match("job-1", "fp-1", load_job_post=True)
        assert result is expected

    def test_without_load_job_post_returns_result(self):
        repo, mock_db = make_repo()
        expected = make_match()
        mock_db.execute.return_value.scalar_one_or_none.return_value = expected

        result = repo.get_existing_match("job-1", "fp-1", load_job_post=False)
        assert result is expected


# ---------------------------------------------------------------------------
# get_matches_for_resume
# ---------------------------------------------------------------------------

class TestGetMatchesForResume:
    def test_returns_matches(self):
        repo, mock_db = make_repo()
        matches = [make_match(), make_match("job-2")]
        mock_db.execute.return_value.scalars.return_value.all.return_value = matches

        result = repo.get_matches_for_resume("fp-1")
        assert result == matches

    def test_empty_result(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        result = repo.get_matches_for_resume("fp-none")
        assert result == []

    def test_min_score_filter_applied(self):
        """min_score parameter should add a where clause."""
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        # Should not raise; the filter is applied via SQLAlchemy query building
        result = repo.get_matches_for_resume("fp-1", min_score=70.0)
        assert result == []

    def test_default_status_is_active(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        # Verify it can be called with default status
        repo.get_matches_for_resume("fp-1")
        mock_db.execute.assert_called_once()

    def test_custom_status(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        repo.get_matches_for_resume("fp-1", status="stale")
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# invalidate_matches_for_job
# ---------------------------------------------------------------------------

class TestInvalidateMatchesForJob:
    def test_invalidates_active_matches(self):
        repo, mock_db = make_repo()
        match1 = make_match(status="active")
        match2 = make_match("job-1", "fp-2", status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [match1, match2]

        count = repo.invalidate_matches_for_job("job-1", reason="Content changed")
        assert count == 2
        assert match1.status == "stale"
        assert match1.invalidated_reason == "Content changed"
        assert match2.status == "stale"

    def test_returns_zero_when_no_matches(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        count = repo.invalidate_matches_for_job("job-none")
        assert count == 0

    def test_default_reason(self):
        repo, mock_db = make_repo()
        match = make_match(status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [match]

        repo.invalidate_matches_for_job("job-1")
        assert match.invalidated_reason == "Job content changed"


# ---------------------------------------------------------------------------
# invalidate_matches_for_resume
# ---------------------------------------------------------------------------

class TestInvalidateMatchesForResume:
    def test_invalidates_matches(self):
        repo, mock_db = make_repo()
        match = make_match(status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [match]

        count = repo.invalidate_matches_for_resume("fp-old", reason="Resume changed")
        assert count == 1
        assert match.status == "stale"
        assert match.invalidated_reason == "Resume changed"

    def test_returns_zero_when_no_matches(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        count = repo.invalidate_matches_for_resume("fp-none")
        assert count == 0

    def test_default_reason(self):
        repo, mock_db = make_repo()
        match = make_match(status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [match]

        repo.invalidate_matches_for_resume("fp-1")
        assert match.invalidated_reason == "Resume changed"


class TestInvalidateMatchesForResumeExcept:
    def test_invalidates_only_jobs_not_in_keep_set(self):
        repo, mock_db = make_repo()
        keep_match = make_match("job-1", "fp-1", status="active")
        stale_match = make_match("job-2", "fp-1", status="active")
        keep_match.invalidated_reason = None
        stale_match.invalidated_reason = None
        mock_db.execute.return_value.scalars.return_value.all.return_value = [
            keep_match,
            stale_match,
        ]

        count = repo.invalidate_matches_for_resume_except(
            "fp-1",
            active_job_ids={"job-1"},
            reason="Refresh latest set",
        )

        assert count == 1
        assert keep_match.status == "active"
        assert keep_match.invalidated_reason is None
        assert stale_match.status == "stale"
        assert stale_match.invalidated_reason == "Refresh latest set"

    def test_returns_zero_when_every_match_is_retained(self):
        repo, mock_db = make_repo()
        keep_match = make_match("job-1", "fp-1", status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [keep_match]

        count = repo.invalidate_matches_for_resume_except(
            "fp-1",
            active_job_ids={"job-1"},
        )

        assert count == 0
        assert keep_match.status == "active"


# ---------------------------------------------------------------------------
# get_stale_matches
# ---------------------------------------------------------------------------

class TestGetStaleMatches:
    def test_returns_stale_matches(self):
        repo, mock_db = make_repo()
        stale = [make_match(status="stale"), make_match("job-2", status="stale")]
        mock_db.execute.return_value.scalars.return_value.all.return_value = stale

        result = repo.get_stale_matches()
        assert result == stale

    def test_default_limit(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        repo.get_stale_matches()
        mock_db.execute.assert_called_once()

    def test_custom_limit(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        repo.get_stale_matches(limit=5)
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# batch_invalidate_matches_for_jobs
# ---------------------------------------------------------------------------

class TestBatchInvalidateMatchesForJobs:
    def test_empty_list_returns_zero(self):
        repo, mock_db = make_repo()

        count = repo.batch_invalidate_matches_for_jobs([])
        assert count == 0
        mock_db.execute.assert_not_called()

    def test_invalidates_across_multiple_jobs(self):
        repo, mock_db = make_repo()
        matches = [
            make_match("job-1", "fp-1", status="active"),
            make_match("job-2", "fp-1", status="active"),
            make_match("job-3", "fp-2", status="active"),
        ]
        mock_db.execute.return_value.scalars.return_value.all.return_value = matches

        count = repo.batch_invalidate_matches_for_jobs(["job-1", "job-2", "job-3"])
        assert count == 3
        for m in matches:
            assert m.status == "stale"

    def test_custom_reason(self):
        repo, mock_db = make_repo()
        match = make_match(status="active")
        mock_db.execute.return_value.scalars.return_value.all.return_value = [match]

        repo.batch_invalidate_matches_for_jobs(["job-1"], reason="Batch update")
        assert match.invalidated_reason == "Batch update"

    def test_no_matches_returns_zero(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []

        count = repo.batch_invalidate_matches_for_jobs(["job-none"])
        assert count == 0


# ---------------------------------------------------------------------------
# get_match_by_id
# ---------------------------------------------------------------------------

class TestGetMatchById:
    def test_returns_match(self):
        repo, mock_db = make_repo()
        match = make_match()
        mock_db.execute.return_value.scalar_one_or_none.return_value = match

        result = repo.get_match_by_id("match-id-1")
        assert result is match

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.get_match_by_id("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# update_hidden_status
# ---------------------------------------------------------------------------

class TestUpdateHiddenStatus:
    def test_hides_match(self):
        repo, mock_db = make_repo()
        match = make_match(is_hidden=False)
        mock_db.execute.return_value.scalar_one_or_none.return_value = match

        result = repo.update_hidden_status("match-id-1", is_hidden=True)
        assert result.is_hidden is True
        assert result is match

    def test_unhides_match(self):
        repo, mock_db = make_repo()
        match = make_match(is_hidden=True)
        mock_db.execute.return_value.scalar_one_or_none.return_value = match

        result = repo.update_hidden_status("match-id-1", is_hidden=False)
        assert result.is_hidden is False

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        result = repo.update_hidden_status("nonexistent", is_hidden=True)
        assert result is None


# ---------------------------------------------------------------------------
# get_hidden_count
# ---------------------------------------------------------------------------

class TestGetHiddenCount:
    def test_returns_count(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar.return_value = 3

        result = repo.get_hidden_count("fp-1")
        assert result == 3

    def test_returns_zero_when_none(self):
        """scalar() returning None should give 0."""
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar.return_value = None

        result = repo.get_hidden_count("fp-empty")
        assert result == 0

    def test_returns_zero_when_no_hidden(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar.return_value = 0

        result = repo.get_hidden_count("fp-none-hidden")
        assert result == 0
