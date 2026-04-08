"""Unit tests for core/scorer/persistence.py"""

import pytest
from unittest.mock import MagicMock
from sqlalchemy.exc import IntegrityError

from core.scorer.persistence import (
    _to_float,
    _to_native_types,
    _extract_job_data,
    _extract_scores,
    _extract_requirement_matches,
    save_match_to_db,
)
from core.matcher.dto import (
    MatchResultDTO,
    JobMatchDTO,
    RequirementMatchDTO,
    JobRequirementDTO,
    JobEvidenceDTO,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_dto(
    job_id="job-abc",
    fingerprint="fp-xyz",
    matched_reqs=None,
    missing_reqs=None,
):
    job = JobMatchDTO(
        id=job_id,
        title="Engineer",
        company="ACME",
        location_text="Remote",
        is_remote=True,
        content_hash="hash-1",
    )
    if matched_reqs is None:
        matched_reqs = [
            RequirementMatchDTO(
                requirement=JobRequirementDTO(id="req-1", req_type="required"),
                evidence=JobEvidenceDTO(text="Used Python", source_section="Experience", tags={"t": 1}),
                similarity=0.85,
                is_covered=True,
            )
        ]
    if missing_reqs is None:
        missing_reqs = [
            RequirementMatchDTO(
                requirement=JobRequirementDTO(id="req-2", req_type="preferred"),
                evidence=None,
                similarity=0.3,
                is_covered=False,
            )
        ]
    return MatchResultDTO(
        job=job,
        fit_score=70.0,
        job_similarity=0.8,
        jd_required_coverage=0.7,
        jd_preferred_requirement_coverage=0.5,
        requirement_matches=matched_reqs,
        missing_requirements=missing_reqs,
        resume_fingerprint=fingerprint,
        preference_score=None,
        fit_components={"required": 0.7},
        preference_components={"preference_mode_used": "semantic_rerank"},
        penalty_details={"details": [], "total": 0.0},
        base_score=72.0,
        penalties=2.0,
        match_type="requirements_only",
    )


def make_execute_chain(*scalar_values):
    """Return a side_effect list for mock_db.execute that yields scalar_one_or_none values."""
    results = []
    for val in scalar_values:
        m = MagicMock()
        m.scalar_one_or_none.return_value = val
        m.scalar.return_value = val
        results.append(m)
    return results


def make_repo(execute_side_effects=None):
    mock_db = MagicMock()
    if execute_side_effects is not None:
        mock_db.execute.side_effect = execute_side_effects
    repo = MagicMock()
    repo.db = mock_db
    return repo


# ---------------------------------------------------------------------------
# _to_float
# ---------------------------------------------------------------------------

class TestToFloat:
    def test_none_returns_zero(self):
        assert _to_float(None) == 0.0

    def test_int_converts(self):
        assert _to_float(5) == 5.0

    def test_float_passthrough(self):
        assert _to_float(3.14) == pytest.approx(3.14)

    def test_string_float(self):
        assert _to_float("2.5") == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# _to_native_types
# ---------------------------------------------------------------------------

class TestToNativeTypes:
    def test_none(self):
        assert _to_native_types(None) is None

    def test_plain_value(self):
        assert _to_native_types(42) == 42
        assert _to_native_types("hello") == "hello"

    def test_dict_recursive(self):
        result = _to_native_types({"a": 1, "b": [2, 3]})
        assert result == {"a": 1, "b": [2, 3]}

    def test_list_recursive(self):
        result = _to_native_types([1, "x", None])
        assert result == [1, "x", None]

    def test_numpy_scalar(self):
        """Objects with .item() method are converted."""
        mock_scalar = MagicMock()
        mock_scalar.item.return_value = 3.14
        # Don't have .tolist attribute — so it goes to .item()
        del mock_scalar.tolist
        result = _to_native_types(mock_scalar)
        assert result == 3.14

    def test_numpy_array(self):
        """Objects with .tolist() method are converted."""
        mock_array = MagicMock()
        mock_array.tolist.return_value = [1.0, 2.0, 3.0]
        result = _to_native_types(mock_array)
        assert result == [1.0, 2.0, 3.0]


# ---------------------------------------------------------------------------
# _extract_job_data
# ---------------------------------------------------------------------------

class TestExtractJobData:
    def test_dto_case(self):
        dto = make_dto()
        result = _extract_job_data(dto)
        assert result["id"] == "job-abc"
        assert result["content_hash"] == "hash-1"

    def test_orm_case(self):
        from core.scorer.models import ScoredJobMatch
        job = MagicMock()
        job.id = "orm-job-id"
        job.content_hash = "orm-hash"
        scored = ScoredJobMatch(job=job)
        result = _extract_job_data(scored)
        assert result["id"] == "orm-job-id"
        assert result["content_hash"] == "orm-hash"

    def test_orm_no_content_hash(self):
        from core.scorer.models import ScoredJobMatch
        job = MagicMock(spec=["id"])
        job.id = "no-hash-job"
        scored = ScoredJobMatch(job=job)
        result = _extract_job_data(scored)
        assert result["content_hash"] == ""


# ---------------------------------------------------------------------------
# _extract_scores
# ---------------------------------------------------------------------------

class TestExtractScores:
    def test_dto_scores(self):
        dto = make_dto()
        scores = _extract_scores(dto)
        assert "overall_score" not in scores
        assert scores["fit_score"] == 70.0
        assert scores["preference_score"] is None
        assert scores["job_similarity"] == 0.8
        assert scores["jd_required_coverage"] == 0.7
        assert scores["jd_preferred_requirement_coverage"] == 0.5
        assert scores["preference_components"] == {"preference_mode_used": "semantic_rerank"}
        assert scores["base_score"] == 72.0
        assert scores["penalties"] == 2.0
        assert scores["match_type"] == "requirements_only"

    def test_orm_scores(self):
        from core.scorer.models import ScoredJobMatch
        job = MagicMock()
        job.id = "j1"
        job.content_hash = "h1"
        scored = ScoredJobMatch(
            job=job,
            fit_score=55.0,
            job_similarity=0.6,
            jd_required_coverage=0.5,
            jd_preferred_requirement_coverage=0.4,
            base_score=58.0,
            penalties=3.0,
            penalty_details=[{"reason": "remote"}],
            fit_components={"a": 1},
            preference_components={"preference_mode_used": "semantic_rerank"},
            match_type="hybrid",
        )
        scores = _extract_scores(scored)
        assert "overall_score" not in scores
        assert scores["fit_score"] == 55.0
        assert scores["preference_score"] is None
        assert scores["match_type"] == "hybrid"
        # penalty_details gets wrapped with total
        assert "total" in scores["penalty_details"]
        assert scores["penalty_details"]["total"] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# _extract_requirement_matches
# ---------------------------------------------------------------------------

class TestExtractRequirementMatches:
    def test_dto_matched_and_missing(self):
        dto = make_dto()
        matched, missing = _extract_requirement_matches(dto)

        assert len(matched) == 1
        assert matched[0]["requirement_id"] == "req-1"
        assert matched[0]["req_type"] == "required"
        assert matched[0]["evidence_text"] == "Used Python"
        assert matched[0]["evidence_section"] == "Experience"
        assert matched[0]["similarity"] == pytest.approx(0.85)
        assert matched[0]["is_covered"] is True

        assert len(missing) == 1
        assert missing[0]["requirement_id"] == "req-2"
        assert missing[0]["req_type"] == "preferred"
        assert missing[0]["similarity"] == pytest.approx(0.3)

    def test_dto_missing_evidence_is_none(self):
        dto = make_dto(
            matched_reqs=[
                RequirementMatchDTO(
                    requirement=JobRequirementDTO(id="r1", req_type="required"),
                    evidence=None,
                    similarity=0.5,
                    is_covered=False,
                )
            ],
            missing_reqs=[],
        )
        matched, _ = _extract_requirement_matches(dto)
        assert matched[0]["evidence_text"] == ""
        assert matched[0]["evidence_section"] is None
        assert matched[0]["evidence_tags"] == {}

    def test_orm_matched_and_missing(self):
        from core.scorer.models import ScoredJobMatch
        from core.matcher.models import RequirementMatchResult
        from etl.resume.models import ResumeEvidenceUnit

        req1 = MagicMock()
        req1.id = "req-orm-1"
        req1.req_type = "required"
        evidence1 = MagicMock(spec=ResumeEvidenceUnit)
        evidence1.text = "ORM evidence"
        evidence1.source_section = "Skills"
        evidence1.tags = {"k": "v"}
        match1 = RequirementMatchResult(
            requirement=req1, evidence=evidence1, similarity=0.9, is_covered=True
        )

        req2 = MagicMock()
        req2.id = "req-orm-2"
        req2.req_type = "preferred"
        miss1 = RequirementMatchResult(
            requirement=req2, evidence=None, similarity=0.2, is_covered=False
        )

        job = MagicMock()
        job.id = "j1"
        scored = ScoredJobMatch(
            job=job,
            matched_requirements=[match1],
            missing_requirements=[miss1],
        )

        matched, missing = _extract_requirement_matches(scored)
        assert matched[0]["evidence_text"] == "ORM evidence"
        assert matched[0]["evidence_section"] == "Skills"
        assert matched[0]["is_covered"] is True
        assert missing[0]["requirement_id"] == "req-orm-2"


# ---------------------------------------------------------------------------
# save_match_to_db
# ---------------------------------------------------------------------------
# NOTE: JobMatch and JobMatchRequirement are NOT patched here because they are
# used inside SQLAlchemy's select()/delete() query builders. Patching them with
# a MagicMock causes an ArgumentError at query construction time.
# Tests instead inspect repo.db.add call arguments and the returned object.

class TestSaveMatchToDb:
    def _make_existing_match(self, **kwargs):
        """Create a mock that looks like an existing JobMatch row."""
        m = MagicMock()
        m.id = kwargs.get("id", "match-id-existing")
        m.is_hidden = kwargs.get("is_hidden", False)
        m.status = "active"
        return m

    def _make_execute_iter(self, *scalar_values):
        """Iterator of execute() return values, each yielding a scalar_one_or_none()."""
        results = []
        for val in scalar_values:
            r = MagicMock()
            r.scalar_one_or_none.return_value = val
            results.append(r)
        it = iter(results + [MagicMock()] * 10)  # pad for delete/other calls
        return lambda *a, **kw: next(it)

    # --- new record creation ---

    def test_creates_new_record_when_none_exists(self):
        """No existing + no hidden match → new JobMatch added to session."""
        from database.models import JobMatch as RealJobMatch
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(None, None)
        dto = make_dto()

        result = save_match_to_db(dto, repo)

        # db.add called at least once; first call is the new JobMatch
        assert repo.db.add.call_count >= 1
        first_added = repo.db.add.call_args_list[0][0][0]
        assert isinstance(first_added, RealJobMatch)
        repo.db.flush.assert_called()
        repo.db.commit.assert_called_once()
        assert result is first_added

    def test_new_record_not_hidden_by_default(self):
        """New record is not hidden when no hidden match exists."""
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(None, None)

        result = save_match_to_db(make_dto(), repo)

        assert result.is_hidden is False

    def test_hidden_status_propagated_to_new_record(self):
        """When a hidden match exists for the job, new record inherits is_hidden=True."""
        hidden_match = self._make_existing_match(is_hidden=True)
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(None, hidden_match)

        result = save_match_to_db(make_dto(), repo)

        assert result.is_hidden is True

    def test_requirements_saved_for_new_record(self):
        """1 matched + 1 missing req → db.add called 3 times total (1 match + 2 reqs)."""
        from database.models import JobMatch as RealJobMatch, JobMatchRequirement as RealJMR
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(None, None)

        save_match_to_db(make_dto(), repo)

        # 1 JobMatch + 2 JobMatchRequirement objects
        assert repo.db.add.call_count == 3
        added_types = [type(c[0][0]) for c in repo.db.add.call_args_list]
        assert added_types[0] is RealJobMatch
        assert added_types[1] is RealJMR
        assert added_types[2] is RealJMR

    # --- update existing record ---

    def test_updates_existing_record(self):
        """When existing match found, updates scores in place and returns it."""
        existing = self._make_existing_match()
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(existing)
        dto = make_dto()

        result = save_match_to_db(dto, repo)

        assert result is existing
        assert existing.status == "active"
        assert existing.fit_score == pytest.approx(70.0)
        repo.db.commit.assert_called_once()

    def test_update_does_not_add_match_to_session(self):
        """Update path: db.add is only called for requirements, not the match."""
        from database.models import JobMatch as RealJobMatch
        existing = self._make_existing_match()
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(existing)

        save_match_to_db(make_dto(), repo)

        for c in repo.db.add.call_args_list:
            assert not isinstance(c[0][0], RealJobMatch)

    # --- stale replacement ---

    def test_stale_replacement_creates_new_record(self):
        """is_stale_replacement=True with existing → new record, old reqs NOT deleted."""
        from database.models import JobMatch as RealJobMatch
        existing = self._make_existing_match()
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(existing)

        result = save_match_to_db(make_dto(), repo, is_stale_replacement=True)

        # A new JobMatch should have been added (not the existing mock)
        assert result is not existing
        assert isinstance(result, RealJobMatch)
        # db.add should have been called for the new match
        added = [c[0][0] for c in repo.db.add.call_args_list]
        assert result in added

    # --- race condition ---

    def test_race_condition_integrity_error_handled(self):
        """IntegrityError on first flush → rollback, refetch existing, update in place."""
        existing_after_race = self._make_existing_match(id="race-match-id")
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(
            None,               # initial fingerprint check → no existing
            None,               # hidden check → no hidden
            existing_after_race,  # refetch after rollback
            # subsequent calls for delete requirements (unlimited padding above)
        )
        repo.db.flush.side_effect = [
            IntegrityError("stmt", "params", Exception("unique violation")),
            None,  # second flush succeeds
        ]

        result = save_match_to_db(make_dto(), repo)

        repo.db.rollback.assert_called_once()
        assert result is existing_after_race
        # Second flush should succeed
        assert repo.db.flush.call_count == 2

    # --- requirements deletion on update ---

    def test_existing_requirements_deleted_on_update(self):
        """Update path calls execute(delete(JobMatchRequirement)) before re-adding."""
        existing = self._make_existing_match()
        repo = make_repo()
        repo.db.execute.side_effect = self._make_execute_iter(existing)

        save_match_to_db(make_dto(), repo)

        # execute() called at least twice: once for the SELECT, once for the DELETE
        assert repo.db.execute.call_count >= 2
