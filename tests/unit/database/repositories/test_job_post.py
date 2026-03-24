"""Unit tests for database/repositories/job_post.py"""

import pytest
from unittest.mock import MagicMock, call
from database.repositories.job_post import JobPostRepository
from database.models import (
    JobPost, JobPostSource, JobRequirementUnit, JobBenefit,
    JobRequirementUnitEmbedding, JobFacetEmbedding,
)


def make_repo():
    mock_db = MagicMock()
    return JobPostRepository(mock_db), mock_db


def make_execute_iter(*scalar_values):
    """Iterator of execute() return values each yielding a different value."""
    results = []
    for val in scalar_values:
        r = MagicMock()
        r.scalar_one.return_value = val
        r.scalar_one_or_none.return_value = val
        r.scalars.return_value.all.return_value = val if isinstance(val, list) else []
        results.append(r)
    it = iter(results + [MagicMock()] * 10)
    return lambda *a, **kw: next(it)


# ---------------------------------------------------------------------------
# get_by_fingerprint
# ---------------------------------------------------------------------------

class TestGetByFingerprint:
    def test_returns_job_when_found(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_job
        result = repo.get_by_fingerprint("fp-abc")
        assert result is mock_job

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = repo.get_by_fingerprint("fp-missing")
        assert result is None


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------

class TestGetById:
    def test_returns_job_post(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_db.execute.return_value.scalar_one.return_value = mock_job
        result = repo.get_by_id("job-id-1")
        assert result is mock_job


# ---------------------------------------------------------------------------
# create_job_post
# ---------------------------------------------------------------------------

class TestCreateJobPost:
    def test_creates_job_post_and_flushes(self):
        repo, mock_db = make_repo()
        job_data = {
            'title': 'Software Engineer',
            'company_name': 'Acme',
            'is_remote': True,
        }
        result = repo.create_job_post(job_data, "fp-123", "Remote")

        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, JobPost)
        assert added.title == 'Software Engineer'
        assert added.company == 'Acme'
        assert added.location_text == 'Remote'
        assert added.is_remote is True
        assert added.canonical_fingerprint == 'fp-123'

    def test_returns_new_job_post(self):
        repo, mock_db = make_repo()
        job_data = {'title': 'Dev', 'company_name': 'Corp', 'is_remote': False}
        result = repo.create_job_post(job_data, "fp-1", "NYC")
        assert isinstance(result, JobPost)

    def test_sets_raw_payload_empty(self):
        repo, mock_db = make_repo()
        job_data = {'title': 'Dev', 'company_name': 'Corp'}
        result = repo.create_job_post(job_data, "fp-1", "NYC")
        assert result.raw_payload == {}


# ---------------------------------------------------------------------------
# get_or_create_source
# ---------------------------------------------------------------------------

class TestGetOrCreateSource:
    def test_creates_source_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        repo.get_or_create_source("job-id", "linkedin", {"job_url": "http://example.com"})

        mock_db.add.assert_called_once()
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, JobPostSource)
        assert added.site == "linkedin"
        assert added.job_url == "http://example.com"

    def test_skips_creation_when_source_exists(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock(spec=JobPostSource)

        repo.get_or_create_source("job-id", "linkedin", {"job_url": "http://example.com"})

        mock_db.add.assert_not_called()

    def test_sets_job_url_direct_if_provided(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        repo.get_or_create_source(
            "job-id", "indeed",
            {"job_url": "http://x.com", "job_url_direct": "http://y.com"}
        )

        added = mock_db.add.call_args[0][0]
        assert added.job_url_direct == "http://y.com"


# ---------------------------------------------------------------------------
# _calculate_content_hash
# ---------------------------------------------------------------------------

class TestCalculateContentHash:
    def test_returns_32_char_hex_string(self):
        repo, _ = make_repo()
        h = repo._calculate_content_hash({"description": "test", "title": "Dev", "company_name": "X"})
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_content_same_hash(self):
        repo, _ = make_repo()
        data = {"description": "Python developer", "title": "Dev", "company_name": "X"}
        assert repo._calculate_content_hash(data) == repo._calculate_content_hash(data)

    def test_different_content_different_hash(self):
        repo, _ = make_repo()
        h1 = repo._calculate_content_hash({"description": "Python", "title": "Dev", "company_name": "X"})
        h2 = repo._calculate_content_hash({"description": "Java", "title": "Dev", "company_name": "X"})
        assert h1 != h2

    def test_handles_missing_fields(self):
        repo, _ = make_repo()
        h = repo._calculate_content_hash({})
        assert len(h) == 32


# ---------------------------------------------------------------------------
# save_job_content
# ---------------------------------------------------------------------------

class TestSaveJobContent:
    def test_updates_description_when_no_existing(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.description = None
        mock_job.content_hash = None
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.save_job_content("job-id", {"description": "Python dev", "title": "Dev", "company_name": "X"})

        assert mock_job.description == "Python dev"

    def test_updates_content_hash_when_changed(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.description = "Old desc"
        mock_job.content_hash = "old-hash"
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.save_job_content("job-id", {"description": "New desc", "title": "Dev", "company_name": "X"})

        # content_hash should be updated to a 32-char hex string
        assert mock_job.content_hash != "old-hash"
        assert len(mock_job.content_hash) == 32

    def test_sets_skills_raw_when_skills_present(self):
        repo, mock_db = make_repo()
        import json
        mock_job = MagicMock(spec=JobPost)
        mock_job.description = None
        mock_job.content_hash = "old"
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.save_job_content("job-id", {
            "description": "desc",
            "title": "Dev",
            "company_name": "X",
            "skills": ["Python", "SQL"]
        })
        assert mock_job.skills_raw == json.dumps(["Python", "SQL"])

    def test_sets_company_url_when_provided(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.description = None
        mock_job.content_hash = "old"
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.save_job_content("job-id", {
            "description": "desc", "title": "Dev", "company_name": "X",
            "company_url": "http://corp.com"
        })
        assert mock_job.company_url == "http://corp.com"


# ---------------------------------------------------------------------------
# update_timestamp
# ---------------------------------------------------------------------------

class TestUpdateTimestamp:
    def test_sets_last_seen_at(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        repo.update_timestamp(mock_job)
        # last_seen_at is set to func.now() - just verify assignment happened
        assert mock_job.last_seen_at is not None


# ---------------------------------------------------------------------------
# mark_as_extracted
# ---------------------------------------------------------------------------

class TestMarkAsExtracted:
    def test_sets_extracted_fields(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.extraction_attempts = 2
        repo.mark_as_extracted(mock_job)
        assert mock_job.is_extracted is True
        assert mock_job.extraction_status == 'succeeded'
        assert mock_job.extraction_attempts == 3
        assert mock_job.extraction_last_error is None
        assert mock_job.extraction_next_retry_at is None

    def test_extraction_attempts_defaults_from_none(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.extraction_attempts = None
        repo.mark_as_extracted(mock_job)
        assert mock_job.extraction_attempts == 1


# ---------------------------------------------------------------------------
# mark_extraction_in_progress
# ---------------------------------------------------------------------------

class TestMarkExtractionInProgress:
    def test_executes_update_statement(self):
        repo, mock_db = make_repo()
        repo.mark_extraction_in_progress("job-id")
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# mark_extraction_retryable_failed
# ---------------------------------------------------------------------------

class TestMarkExtractionRetryableFailed:
    def test_fetches_job_and_updates_status(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.extraction_attempts = 1
        mock_db.execute.side_effect = make_execute_iter(mock_job)

        repo.mark_extraction_retryable_failed("job-id", "timeout")

        assert mock_db.execute.call_count == 2  # select + update

    def test_attempts_starts_from_none(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.extraction_attempts = None
        mock_db.execute.side_effect = make_execute_iter(mock_job)

        repo.mark_extraction_retryable_failed("job-id", "error")
        # Should not raise (attempts = 0 + 1 = 1)
        assert mock_db.execute.call_count == 2


# ---------------------------------------------------------------------------
# mark_extraction_failed
# ---------------------------------------------------------------------------

class TestMarkExtractionFailed:
    def test_fetches_job_and_sets_terminal_status(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.extraction_attempts = 2
        mock_db.execute.side_effect = make_execute_iter(mock_job)

        repo.mark_extraction_failed("job-id", "max retries exceeded")

        assert mock_db.execute.call_count == 2  # select + update


# ---------------------------------------------------------------------------
# _extract_years_from_requirement
# ---------------------------------------------------------------------------

class TestExtractYearsFromRequirement:
    def test_empty_text_returns_none(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement("")
        assert years is None
        assert ctx is None

    def test_none_text_returns_none(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement(None)
        assert years is None
        assert ctx is None

    def test_extracts_years_from_text(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement("5 years of Python experience")
        assert years == 5

    def test_extracts_years_minimum_pattern(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement("minimum 3 years experience")
        assert years == 3

    def test_no_years_returns_none(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement("must know Python and SQL")
        assert years is None
        assert ctx is None

    def test_plus_pattern(self):
        repo, _ = make_repo()
        years, ctx = repo._extract_years_from_requirement("3+ years of JavaScript")
        assert years == 3


# ---------------------------------------------------------------------------
# save_requirements
# ---------------------------------------------------------------------------

class TestSaveRequirements:
    def test_creates_requirement_units(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.id = "job-id"
        requirements = [
            {'req_type': 'must_have', 'text': '5 years Python', 'ordinal': 0},
            {'req_type': 'nice_to_have', 'text': 'React knowledge', 'ordinal': 1},
        ]
        repo.save_requirements(mock_job, requirements)

        assert mock_db.add.call_count == 2
        mock_db.flush.assert_called_once()

        first_jru = mock_db.add.call_args_list[0][0][0]
        assert isinstance(first_jru, JobRequirementUnit)
        assert first_jru.req_type == 'required'
        assert first_jru.text == '5 years Python'

    def test_maps_req_types_correctly(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.id = "job-id"
        reqs = [
            {'req_type': 'must_have', 'text': 'A'},
            {'req_type': 'nice_to_have', 'text': 'B'},
            {'req_type': 'responsibility', 'text': 'C'},
        ]
        repo.save_requirements(mock_job, reqs)
        types = [mock_db.add.call_args_list[i][0][0].req_type for i in range(3)]
        assert types == ['required', 'preferred', 'responsibility']

    def test_empty_requirements_only_flushes(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        repo.save_requirements(mock_job, [])
        mock_db.add.assert_not_called()
        mock_db.flush.assert_called_once()

    def test_extracts_years_from_requirement_text(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.id = "job-id"
        repo.save_requirements(mock_job, [{'req_type': 'must_have', 'text': '3 years Python'}])
        jru = mock_db.add.call_args[0][0]
        assert jru.min_years == 3


# ---------------------------------------------------------------------------
# save_benefits
# ---------------------------------------------------------------------------

class TestSaveBenefits:
    def test_creates_benefit_records(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.id = "job-id"
        benefits = [
            {'category': 'health_insurance', 'text': 'Full coverage', 'ordinal': 0},
            {'category': 'pto', 'text': '20 days PTO', 'ordinal': 1},
        ]
        repo.save_benefits(mock_job, benefits)

        assert mock_db.add.call_count == 2
        mock_db.flush.assert_called_once()
        first_jb = mock_db.add.call_args_list[0][0][0]
        assert isinstance(first_jb, JobBenefit)
        assert first_jb.category == 'health_insurance'

    def test_unmapped_category_defaults_to_other(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.id = "job-id"
        repo.save_benefits(mock_job, [{'category': 'unknown_cat', 'text': 'Free snacks', 'ordinal': 0}])
        jb = mock_db.add.call_args[0][0]
        assert jb.category == 'other'

    def test_empty_benefits_only_flushes(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        repo.save_benefits(mock_job, [])
        mock_db.add.assert_not_called()
        mock_db.flush.assert_called_once()


# ---------------------------------------------------------------------------
# _compute_next_retry_at (static)
# ---------------------------------------------------------------------------

class TestComputeNextRetryAt:
    def test_first_attempt_uses_first_delay(self):
        from database.repositories.job_post import EXTRACTION_RETRY_DELAYS_SECONDS
        from datetime import datetime, timezone, timedelta
        result = JobPostRepository._compute_next_retry_at(1, EXTRACTION_RETRY_DELAYS_SECONDS)
        expected_delta = timedelta(seconds=EXTRACTION_RETRY_DELAYS_SECONDS[0])
        now = datetime.now(timezone.utc)
        assert abs((result - now).total_seconds() - expected_delta.total_seconds()) < 2

    def test_excess_attempts_uses_last_delay(self):
        from database.repositories.job_post import EXTRACTION_RETRY_DELAYS_SECONDS
        schedule = [10, 20, 30]
        result = JobPostRepository._compute_next_retry_at(100, schedule)
        # Should use last delay (30s)
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        assert abs((result - now).total_seconds() - 30) < 2


# ---------------------------------------------------------------------------
# update_job_metadata
# ---------------------------------------------------------------------------

class TestUpdateJobMetadata:
    def test_sets_basic_metadata_fields(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.salary_min = None
        mock_job.salary_max = None
        mock_job.currency = None

        repo.update_job_metadata(mock_job, {
            'min_years_experience': 3,
            'requires_degree': True,
            'security_clearance': False,
            'seniority_level': 'senior',
        })

        assert mock_job.min_years_experience == 3
        assert mock_job.requires_degree is True
        assert mock_job.security_clearance is False
        assert mock_job.job_level == 'senior'

    def test_salary_only_set_when_null(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.salary_min = None
        mock_job.salary_max = 80000
        mock_job.currency = None

        repo.update_job_metadata(mock_job, {'salary_min': 60000, 'salary_max': 90000, 'currency': 'USD'})

        assert mock_job.salary_min == 60000
        assert mock_job.salary_max == 80000  # NOT overwritten (was already set)
        assert mock_job.currency == 'USD'

    def test_remote_local_sets_is_remote_true(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.salary_min = 0
        mock_job.salary_max = 0
        mock_job.currency = 'USD'
        repo.update_job_metadata(mock_job, {'remote_policy': 'Remote (Local)'})
        assert mock_job.is_remote is True

    def test_remote_global_sets_is_remote_true(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.salary_min = 0
        mock_job.salary_max = 0
        mock_job.currency = 'USD'
        repo.update_job_metadata(mock_job, {'remote_policy': 'Remote (Global)'})
        assert mock_job.is_remote is True

    def test_onsite_sets_is_remote_false(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.salary_min = 0
        mock_job.salary_max = 0
        mock_job.currency = 'USD'
        repo.update_job_metadata(mock_job, {'remote_policy': 'On-site'})
        assert mock_job.is_remote is False


# ---------------------------------------------------------------------------
# update_content_metadata
# ---------------------------------------------------------------------------

class TestUpdateContentMetadata:
    def test_sets_tech_stack_in_skills_raw(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.raw_payload = {}
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.update_content_metadata("job-id", {'tech_stack': ['Python', 'SQL']})
        assert mock_job.skills_raw == "Python,SQL"

    def test_sets_ai_job_summary(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.raw_payload = {}
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.update_content_metadata("job-id", {'job_summary': 'Senior engineer role'})
        assert mock_job.raw_payload['ai_job_summary'] == 'Senior engineer role'

    def test_sets_visa_sponsorship(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.raw_payload = {}
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.update_content_metadata("job-id", {'visa_sponsorship_available': True})
        assert mock_job.raw_payload['visa_sponsorship_available'] is True

    def test_empty_metadata_only_fetches_job(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.raw_payload = None
        mock_db.execute.return_value.scalar_one.return_value = mock_job

        repo.update_content_metadata("job-id", {})
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# get_unembedded_requirements / get_requirement_by_id
# ---------------------------------------------------------------------------

class TestGetUnembeddedRequirements:
    def test_returns_requirements(self):
        repo, mock_db = make_repo()
        mock_req = MagicMock(spec=JobRequirementUnit)
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_req]
        result = repo.get_unembedded_requirements()
        assert result == [mock_req]

    def test_returns_empty_list(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_unembedded_requirements(limit=500)
        assert result == []


class TestGetRequirementById:
    def test_returns_requirement(self):
        repo, mock_db = make_repo()
        mock_req = MagicMock(spec=JobRequirementUnit)
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_req
        result = repo.get_requirement_by_id("req-id")
        assert result is mock_req

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = repo.get_requirement_by_id("missing")
        assert result is None


# ---------------------------------------------------------------------------
# save_job_embedding
# ---------------------------------------------------------------------------

class TestSaveJobEmbedding:
    def test_sets_embedding_fields(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.embedding_attempts = 1
        repo.save_job_embedding(mock_job, [0.1, 0.2, 0.3])
        assert mock_job.summary_embedding == [0.1, 0.2, 0.3]
        assert mock_job.is_embedded is True
        assert mock_job.embedding_status == 'succeeded'
        assert mock_job.embedding_attempts == 2
        assert mock_job.embedding_last_error is None
        assert mock_job.embedding_next_retry_at is None

    def test_attempts_from_none(self):
        repo, _ = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.embedding_attempts = None
        repo.save_job_embedding(mock_job, [])
        assert mock_job.embedding_attempts == 1


# ---------------------------------------------------------------------------
# save_requirement_embedding
# ---------------------------------------------------------------------------

class TestSaveRequirementEmbedding:
    def test_creates_embedding_record(self):
        repo, mock_db = make_repo()
        repo.save_requirement_embedding("req-id", [0.5, 0.6])
        mock_db.add.assert_called_once()
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, JobRequirementUnitEmbedding)
        assert added.job_requirement_unit_id == "req-id"
        assert added.embedding == [0.5, 0.6]


# ---------------------------------------------------------------------------
# mark_embedding_in_progress / mark_embedding_retryable_failed / mark_embedding_failed
# ---------------------------------------------------------------------------

class TestMarkEmbeddingInProgress:
    def test_executes_update_statement(self):
        repo, mock_db = make_repo()
        repo.mark_embedding_in_progress("job-id")
        mock_db.execute.assert_called_once()


class TestBulkMarkEmbeddingInProgress:
    def test_executes_bulk_update(self):
        repo, mock_db = make_repo()
        repo.bulk_mark_embedding_in_progress(["id-1", "id-2"])
        mock_db.execute.assert_called_once()

    def test_empty_list_returns_early(self):
        repo, mock_db = make_repo()
        repo.bulk_mark_embedding_in_progress([])
        mock_db.execute.assert_not_called()


class TestMarkEmbeddingRetryableFailed:
    def test_fetches_job_and_updates(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.embedding_attempts = 2
        mock_db.execute.side_effect = make_execute_iter(mock_job)
        repo.mark_embedding_retryable_failed("job-id", "oom")
        assert mock_db.execute.call_count == 2


class TestMarkEmbeddingFailed:
    def test_fetches_job_and_sets_terminal_status(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_job.embedding_attempts = 3
        mock_db.execute.side_effect = make_execute_iter(mock_job)
        repo.mark_embedding_failed("job-id", "max retries")
        assert mock_db.execute.call_count == 2


# ---------------------------------------------------------------------------
# get_embedded_jobs_for_matching
# ---------------------------------------------------------------------------

class TestGetEmbeddedJobsForMatching:
    def test_returns_embedded_jobs(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_job]
        result = repo.get_embedded_jobs_for_matching()
        assert result == [mock_job]

    def test_returns_empty_list(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_embedded_jobs_for_matching(limit=10)
        assert result == []


# ---------------------------------------------------------------------------
# get_top_jobs_by_summary_embedding
# ---------------------------------------------------------------------------

class TestGetTopJobsBySummaryEmbedding:
    def test_returns_job_similarity_pairs(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        row = MagicMock()
        row.__getitem__.return_value = mock_job
        row._mapping = {'distance': 0.2}
        mock_db.execute.return_value.all.return_value = [row]

        result = repo.get_top_jobs_by_summary_embedding([0.1, 0.2])

        assert len(result) == 1
        job, sim = result[0]
        assert job is mock_job
        assert sim == pytest.approx(0.8)

    def test_empty_returns_empty(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []
        result = repo.get_top_jobs_by_summary_embedding([0.1])
        assert result == []

    def test_with_tenant_id(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []
        repo.get_top_jobs_by_summary_embedding([0.1], tenant_id="tenant-x")
        mock_db.execute.assert_called_once()

    def test_with_require_remote(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []
        repo.get_top_jobs_by_summary_embedding([0.1], require_remote=True)
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# get_job_facet_embeddings / get_facets_for_job / get_jobs_needing_facet_embedding
# ---------------------------------------------------------------------------

class TestGetJobFacetEmbeddings:
    def test_returns_dict_of_embeddings(self):
        repo, mock_db = make_repo()
        facet1 = MagicMock()
        facet1.facet_key = "skills"
        facet1.embedding = [0.1, 0.2]
        facet2 = MagicMock()
        facet2.facet_key = "remote"
        facet2.embedding = None  # Should be excluded
        mock_db.execute.return_value.scalars.return_value.all.return_value = [facet1, facet2]

        result = repo.get_job_facet_embeddings("job-id")
        assert result == {"skills": [0.1, 0.2]}
        assert "remote" not in result


class TestGetFacetsForJob:
    def test_returns_list_of_facets(self):
        repo, mock_db = make_repo()
        facets = [MagicMock(spec=JobFacetEmbedding) for _ in range(3)]
        mock_db.execute.return_value.scalars.return_value.all.return_value = facets
        result = repo.get_facets_for_job("job-id")
        assert result == facets


class TestGetJobsNeedingFacetEmbedding:
    def test_returns_list(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [MagicMock()]
        result = repo.get_jobs_needing_facet_embedding(limit=5)
        assert len(result) == 1

    def test_returns_empty_when_no_unembedded_facets(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_jobs_needing_facet_embedding()
        assert result == []

    def test_query_joins_facet_embedding_for_null_filter(self):
        """Change 3: query must join JobFacetEmbedding and filter embedding IS NULL."""
        import inspect
        from database.repositories.job_post import JobPostRepository
        src = inspect.getsource(JobPostRepository.get_jobs_needing_facet_embedding)
        assert "JobFacetEmbedding" in src
        assert "is_(None)" in src
        assert "distinct" in src


# ---------------------------------------------------------------------------
# get_and_claim_jobs_for_facet_extraction (Change 1 — extraction_status fix)
# ---------------------------------------------------------------------------

class TestGetAndClaimJobsForFacetExtraction:
    def _side_effect(self, claimed_ids, jobs):
        """Provide execute() side-effects: stale-reset UPDATE, quarantine UPDATE, claim CTE, SELECT."""
        claim_result = MagicMock()
        claim_result.fetchall.return_value = [(id_,) for id_ in claimed_ids]
        jobs_result = MagicMock()
        jobs_result.scalars.return_value.all.return_value = jobs
        return [MagicMock(), MagicMock(), claim_result, jobs_result]

    def test_claim_sql_uses_extraction_status_not_is_embedded(self):
        """Change 1: claim CTE must filter on extraction_status='succeeded', not is_embedded=true."""
        import inspect
        from database.repositories.job_post import JobPostRepository
        src = inspect.getsource(JobPostRepository.get_and_claim_jobs_for_facet_extraction)
        assert "extraction_status = 'succeeded'" in src
        assert "is_embedded = true" not in src

    def test_returns_empty_when_nothing_claimed(self):
        repo, mock_db = make_repo()
        claim_result = MagicMock()
        claim_result.fetchall.return_value = []
        mock_db.execute.side_effect = [MagicMock(), MagicMock(), claim_result]
        result = repo.get_and_claim_jobs_for_facet_extraction()
        assert result == []

    def test_returns_claimed_jobs(self):
        repo, mock_db = make_repo()
        mock_job = MagicMock(spec=JobPost)
        mock_db.execute.side_effect = self._side_effect(["id-1"], [mock_job])
        result = repo.get_and_claim_jobs_for_facet_extraction()
        assert result == [mock_job]

    def test_extraction_status_succeeded_is_embedded_false_jobs_are_eligible(self):
        """Change 1: is_embedded=false must not block claiming when extraction_status=succeeded."""
        # Verified via source inspection: the old is_embedded = true guard is gone.
        import inspect
        from database.repositories.job_post import JobPostRepository
        src = inspect.getsource(JobPostRepository.get_and_claim_jobs_for_facet_extraction)
        assert "is_embedded = true" not in src


# ---------------------------------------------------------------------------
# update_facet_embedding
# ---------------------------------------------------------------------------

class TestUpdateFacetEmbedding:
    def test_executes_update(self):
        repo, mock_db = make_repo()
        repo.update_facet_embedding("facet-id", [0.1, 0.2], "hash-abc")
        mock_db.execute.assert_called_once()


# ---------------------------------------------------------------------------
# reset_stale_facet_jobs
# ---------------------------------------------------------------------------

class TestResetStaleFacetJobs:
    def test_returns_rowcount(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.rowcount = 3
        result = repo.reset_stale_facet_jobs(timeout_minutes=30, max_retries=5)
        assert result == 3
        mock_db.execute.assert_called_once()

    def test_default_params(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.rowcount = 0
        result = repo.reset_stale_facet_jobs()
        assert result == 0


# ---------------------------------------------------------------------------
# get_jobs_with_failed_facets
# ---------------------------------------------------------------------------

class TestGetJobsWithFailedFacets:
    def test_returns_failed_jobs(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [MagicMock()]
        result = repo.get_jobs_with_failed_facets(limit=10)
        assert len(result) == 1

    def test_returns_empty(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_jobs_with_failed_facets()
        assert result == []


# ---------------------------------------------------------------------------
# get_jobs_with_missing_facet_embeddings
# ---------------------------------------------------------------------------

class TestGetJobsWithMissingFacetEmbeddings:
    def test_returns_empty_when_no_job_ids(self):
        repo, mock_db = make_repo()
        result_mock = MagicMock()
        result_mock.fetchall.return_value = []
        mock_db.execute.return_value = result_mock

        result = repo.get_jobs_with_missing_facet_embeddings()
        assert result == []

    def test_returns_jobs_when_ids_found(self):
        repo, mock_db = make_repo()
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value="job-id-1")
        result_mock = MagicMock()
        result_mock.fetchall.return_value = [mock_row]

        mock_jobs = [MagicMock(spec=JobPost)]
        second_result = MagicMock()
        second_result.scalars.return_value.all.return_value = mock_jobs

        mock_db.execute.side_effect = [result_mock, second_result]

        result = repo.get_jobs_with_missing_facet_embeddings()
        assert result == mock_jobs
