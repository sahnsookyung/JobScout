"""Unit tests for database/repository.py

Tests lazy-loading properties and delegation methods of JobRepository.
"""

from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql

from database.models import RESUME_FINGERPRINT_VERSION, SYSTEM_OWNER_ID
from database.repository import JobRepository
from database.repositories.job_post import JobPostRepository
from database.repositories.resume import ResumeRepository
from database.repositories.match import MatchRepository
from database.repositories.embedding import EmbeddingRepository
from database.repositories.candidate_preferences import CandidatePreferencesRepository
from database.repositories.user_feature_capability import UserFeatureCapabilityRepository


def make_repo():
    mock_db = MagicMock()
    return JobRepository(mock_db), mock_db


def _compiled_sql(stmt) -> str:
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


# ---------------------------------------------------------------------------
# Lazy-loading properties
# ---------------------------------------------------------------------------

class TestJobRepositoryProperties:
    def test_job_post_returns_job_post_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.job_post, JobPostRepository)

    def test_job_post_cached_on_second_access(self):
        repo, _ = make_repo()
        first = repo.job_post
        second = repo.job_post
        assert first is second

    def test_resume_returns_resume_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.resume, ResumeRepository)

    def test_resume_cached(self):
        repo, _ = make_repo()
        first = repo.resume
        second = repo.resume
        assert first is second

    def test_match_returns_match_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.match, MatchRepository)

    def test_match_cached(self):
        repo, _ = make_repo()
        first = repo.match
        second = repo.match
        assert first is second

    def test_embedding_returns_embedding_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.embedding, EmbeddingRepository)

    def test_embedding_cached(self):
        repo, _ = make_repo()
        first = repo.embedding
        second = repo.embedding
        assert first is second

    def test_candidate_preferences_returns_candidate_preferences_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.candidate_preferences, CandidatePreferencesRepository)

    def test_candidate_preferences_cached(self):
        repo, _ = make_repo()
        first = repo.candidate_preferences
        second = repo.candidate_preferences
        assert first is second

    def test_match_selection_returns_match_selection_repository(self):
        repo, _ = make_repo()
        assert repo.match_selection is not None

    def test_notification_settings_returns_notification_settings_repository(self):
        repo, _ = make_repo()
        assert repo.notification_settings is not None

    def test_user_feature_capability_returns_repository(self):
        repo, _ = make_repo()
        assert isinstance(repo.user_feature_capability, UserFeatureCapabilityRepository)

    def test_all_sub_repos_use_same_db(self):
        mock_db = MagicMock()
        repo = JobRepository(mock_db)
        assert repo.job_post.db is mock_db
        assert repo.resume.db is mock_db
        assert repo.match.db is mock_db

    def test_each_property_independent(self):
        repo, _ = make_repo()
        # Accessing one property doesn't pre-instantiate others
        _ = repo.job_post
        assert repo._resume_repo is None
        assert repo._match_repo is None


class TestMatchRepositoryBacklogCounts:
    def test_pending_matching_count_applies_hard_candidate_preferences(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0
        repo = MatchRepository(db)

        result = repo.count_pending_matching_jobs(
            "resume-fp",
            candidate_preferences={
                "remote_mode": "remote",
                "target_locations": ["Remote"],
                "visa_sponsorship_required": False,
                "salary_min": None,
                "employment_types": [],
            },
        )

        assert result == 0
        stmt = db.execute.call_args.args[0]
        sql = _compiled_sql(stmt).lower()
        assert "job_match.resume_fingerprint" in sql
        assert "job_post.is_remote is true" in sql
        assert "job_post.location_text" in sql
        assert "job_post.summary_embedding is not null" in sql

    def test_pending_matching_count_excludes_explicit_remote_for_onsite_preference(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0
        repo = MatchRepository(db)

        repo.count_pending_matching_jobs(
            "resume-fp",
            candidate_preferences={
                "remote_mode": "onsite",
                "target_locations": [],
                "visa_sponsorship_required": False,
                "salary_min": None,
                "employment_types": [],
            },
        )

        stmt = db.execute.call_args.args[0]
        sql = _compiled_sql(stmt).lower()
        assert "job_post.is_remote is true" in sql
        assert "remote" in sql
        assert "hybrid" in sql


# ---------------------------------------------------------------------------
# commit / rollback
# ---------------------------------------------------------------------------

class TestCommitRollback:
    def test_commit_delegates_to_db(self):
        repo, mock_db = make_repo()
        repo.commit()
        mock_db.commit.assert_called_once()

    def test_rollback_delegates_to_db(self):
        repo, mock_db = make_repo()
        repo.rollback()
        mock_db.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# job_post delegation
# ---------------------------------------------------------------------------

class TestJobPostDelegation:
    def test_get_by_fingerprint(self):
        repo, _ = make_repo()
        repo.job_post.get_by_fingerprint = MagicMock(return_value="job-1")
        result = repo.get_by_fingerprint("fp-abc")
        repo.job_post.get_by_fingerprint.assert_called_once_with("fp-abc", tenant_id=None)
        assert result == "job-1"

    def test_get_by_id(self):
        repo, _ = make_repo()
        repo.job_post.get_by_id = MagicMock(return_value="job-obj")
        result = repo.get_by_id("id-1")
        repo.job_post.get_by_id.assert_called_once_with("id-1")
        assert result == "job-obj"

    def test_get_by_source(self):
        repo, _ = make_repo()
        repo.job_post.get_by_source = MagicMock(return_value="job-from-source")
        result = repo.get_by_source("greenhouse", "https://example.com/jobs/1", tenant_id="tenant-1")
        repo.job_post.get_by_source.assert_called_once_with(
            "greenhouse",
            "https://example.com/jobs/1",
            tenant_id="tenant-1",
        )
        assert result == "job-from-source"

    def test_create_job_post(self):
        repo, _ = make_repo()
        repo.job_post.create_job_post = MagicMock(return_value="new-job")
        result = repo.create_job_post({"title": "Dev"}, "fp-1", "Remote")
        repo.job_post.create_job_post.assert_called_once_with(
            {"title": "Dev"},
            "fp-1",
            "Remote",
            tenant_id=None,
        )
        assert result == "new-job"

    def test_get_or_create_source(self):
        repo, _ = make_repo()
        repo.job_post.get_or_create_source = MagicMock(return_value=None)
        repo.get_or_create_source("job-id", "linkedin", {"url": "x"}, tenant_id="tenant-1")
        repo.job_post.get_or_create_source.assert_called_once_with(
            "job-id",
            "linkedin",
            {"url": "x"},
            tenant_id="tenant-1",
        )

    def test_save_job_content(self):
        repo, _ = make_repo()
        repo.job_post.save_job_content = MagicMock()
        repo.save_job_content("job-id", {"description": "..."})
        repo.job_post.save_job_content.assert_called_once_with("job-id", {"description": "..."})

    def test_update_timestamp(self):
        repo, _ = make_repo()
        repo.job_post.update_timestamp = MagicMock()
        mock_job = MagicMock()
        repo.update_timestamp(mock_job)
        repo.job_post.update_timestamp.assert_called_once_with(mock_job)

    def test_calculate_content_hash(self):
        repo, _ = make_repo()
        repo.job_post._calculate_content_hash = MagicMock(return_value="hash-123")
        result = repo._calculate_content_hash({"title": "Platform Engineer"})
        repo.job_post._calculate_content_hash.assert_called_once_with({"title": "Platform Engineer"})
        assert result == "hash-123"

    def test_deactivate_missing_sources(self):
        repo, _ = make_repo()
        repo.job_post.deactivate_missing_sources = MagicMock(return_value=2)
        result = repo.deactivate_missing_sources(
            "greenhouse",
            ["https://example.com/jobs/1"],
            tenant_id="tenant-1",
        )
        repo.job_post.deactivate_missing_sources.assert_called_once_with(
            "greenhouse",
            ["https://example.com/jobs/1"],
            tenant_id="tenant-1",
        )
        assert result == 2

    def test_get_unextracted_jobs(self):
        repo, _ = make_repo()
        repo.job_post.get_unextracted_jobs = MagicMock(return_value=["j1", "j2"])
        result = repo.get_unextracted_jobs(limit=50)
        repo.job_post.get_unextracted_jobs.assert_called_once_with(50)
        assert result == ["j1", "j2"]

    def test_mark_as_extracted(self):
        repo, _ = make_repo()
        repo.job_post.mark_as_extracted = MagicMock()
        mock_job = MagicMock()
        repo.mark_as_extracted(mock_job)
        repo.job_post.mark_as_extracted.assert_called_once_with(mock_job)

    def test_mark_extraction_in_progress(self):
        repo, _ = make_repo()
        repo.job_post.mark_extraction_in_progress = MagicMock()
        repo.mark_extraction_in_progress("job-id")
        repo.job_post.mark_extraction_in_progress.assert_called_once_with("job-id")

    def test_mark_extraction_retryable_failed(self):
        repo, _ = make_repo()
        repo.job_post.mark_extraction_retryable_failed = MagicMock()
        repo.mark_extraction_retryable_failed("job-id", "timeout")
        repo.job_post.mark_extraction_retryable_failed.assert_called_once_with("job-id", "timeout")

    def test_mark_extraction_failed(self):
        repo, _ = make_repo()
        repo.job_post.mark_extraction_failed = MagicMock()
        repo.mark_extraction_failed("job-id", "error msg")
        repo.job_post.mark_extraction_failed.assert_called_once_with("job-id", "error msg")

    def test_extract_years_from_requirement(self):
        repo, _ = make_repo()
        repo.job_post._extract_years_from_requirement = MagicMock(return_value=(5, "5 years"))
        result = repo._extract_years_from_requirement("5 years of Python")
        repo.job_post._extract_years_from_requirement.assert_called_once_with("5 years of Python")
        assert result == (5, "5 years")

    def test_save_requirements(self):
        repo, _ = make_repo()
        repo.job_post.save_requirements = MagicMock()
        mock_job = MagicMock()
        repo.save_requirements(mock_job, [{"req": "python"}])
        repo.job_post.save_requirements.assert_called_once_with(mock_job, [{"req": "python"}])

    def test_save_benefits(self):
        repo, _ = make_repo()
        repo.job_post.save_benefits = MagicMock()
        mock_job = MagicMock()
        repo.save_benefits(mock_job, [{"benefit": "health"}])
        repo.job_post.save_benefits.assert_called_once_with(mock_job, [{"benefit": "health"}])

    def test_update_job_metadata(self):
        repo, _ = make_repo()
        repo.job_post.update_job_metadata = MagicMock()
        mock_job = MagicMock()
        repo.update_job_metadata(mock_job, {"source": "linkedin"})
        repo.job_post.update_job_metadata.assert_called_once_with(mock_job, {"source": "linkedin"})

    def test_update_content_metadata(self):
        repo, _ = make_repo()
        repo.job_post.update_content_metadata = MagicMock()
        repo.update_content_metadata("job-id", {"hash": "abc"})
        repo.job_post.update_content_metadata.assert_called_once_with("job-id", {"hash": "abc"})

    def test_get_unembedded_jobs(self):
        repo, _ = make_repo()
        repo.job_post.get_unembedded_jobs = MagicMock(return_value=[])
        result = repo.get_unembedded_jobs(limit=200)
        repo.job_post.get_unembedded_jobs.assert_called_once_with(200)
        assert result == []

    def test_get_unembedded_requirements(self):
        repo, _ = make_repo()
        repo.job_post.get_unembedded_requirements = MagicMock(return_value=["req-1"])
        result = repo.get_unembedded_requirements(limit=500)
        assert result == ["req-1"]

    def test_get_requirement_by_id(self):
        repo, _ = make_repo()
        repo.job_post.get_requirement_by_id = MagicMock(return_value="req-obj")
        result = repo.get_requirement_by_id("req-id")
        assert result == "req-obj"

    def test_save_job_embedding(self):
        repo, _ = make_repo()
        repo.job_post.save_job_embedding = MagicMock()
        mock_job = MagicMock()
        repo.save_job_embedding(mock_job, [0.1, 0.2])
        repo.job_post.save_job_embedding.assert_called_once_with(mock_job, [0.1, 0.2])

    def test_mark_embedding_in_progress(self):
        repo, _ = make_repo()
        repo.job_post.mark_embedding_in_progress = MagicMock()
        repo.mark_embedding_in_progress("job-id")
        repo.job_post.mark_embedding_in_progress.assert_called_once_with("job-id")

    def test_mark_embedding_retryable_failed(self):
        repo, _ = make_repo()
        repo.job_post.mark_embedding_retryable_failed = MagicMock()
        repo.mark_embedding_retryable_failed("job-id", "err")
        repo.job_post.mark_embedding_retryable_failed.assert_called_once_with("job-id", "err")

    def test_bulk_mark_embedding_in_progress(self):
        repo, _ = make_repo()
        repo.job_post.bulk_mark_embedding_in_progress = MagicMock()
        repo.bulk_mark_embedding_in_progress(["id-1", "id-2"])
        repo.job_post.bulk_mark_embedding_in_progress.assert_called_once_with(["id-1", "id-2"])

    def test_save_requirement_embedding(self):
        repo, _ = make_repo()
        repo.job_post.save_requirement_embedding = MagicMock()
        repo.save_requirement_embedding("req-id", [0.5, 0.6])
        repo.job_post.save_requirement_embedding.assert_called_once_with("req-id", [0.5, 0.6])

    def test_mark_embedding_failed(self):
        repo, _ = make_repo()
        repo.job_post.mark_embedding_failed = MagicMock()
        repo.mark_embedding_failed("job-id", "oom error")
        repo.job_post.mark_embedding_failed.assert_called_once_with("job-id", "oom error")

    def test_get_embedded_jobs_for_matching(self):
        repo, _ = make_repo()
        repo.job_post.get_embedded_jobs_for_matching = MagicMock(return_value=["j"])
        result = repo.get_embedded_jobs_for_matching(limit=10)
        assert result == ["j"]

    def test_get_top_jobs_by_summary_embedding(self):
        repo, _ = make_repo()
        repo.job_post.get_top_jobs_by_summary_embedding = MagicMock(return_value=[("job", 0.9)])
        result = repo.get_top_jobs_by_summary_embedding([0.1, 0.2], 10, None, True)
        assert result == [("job", 0.9)]

    def test_get_top_jobs_by_lexical_query(self):
        repo, _ = make_repo()
        repo.job_post.get_top_jobs_by_lexical_query = MagicMock(return_value=[("job", 0.5, 0.8)])
        result = repo.get_top_jobs_by_lexical_query(
            "python | aws",
            resume_embedding=[0.1, 0.2],
            limit=10,
            tenant_id="tenant-x",
            require_remote=True,
        )
        repo.job_post.get_top_jobs_by_lexical_query.assert_called_once_with(
            "python | aws",
            resume_embedding=[0.1, 0.2],
            limit=10,
            tenant_id="tenant-x",
            require_remote=True,
            exclude_reusable_resume_fingerprint=None,
        )
        assert result == [("job", 0.5, 0.8)]

    def test_quarantine_null_description_jobs(self):
        repo, _ = make_repo()
        repo.job_post.quarantine_null_description_jobs = MagicMock(return_value=4)
        result = repo.quarantine_null_description_jobs(older_than_days=14)
        repo.job_post.quarantine_null_description_jobs.assert_called_once_with(14)
        assert result == 4


# ---------------------------------------------------------------------------
# resume delegation
# ---------------------------------------------------------------------------

class TestResumeDelegation:
    def test_get_resume_summary_embedding(self):
        repo, _ = make_repo()
        repo.resume.get_resume_summary_embedding = MagicMock(return_value=[0.1, 0.2])
        result = repo.get_resume_summary_embedding("fp-1")
        repo.resume.get_resume_summary_embedding.assert_called_once_with("fp-1")
        assert result == [0.1, 0.2]

    def test_save_structured_resume(self):
        repo, _ = make_repo()
        repo.resume.save_structured_resume = MagicMock(return_value="resume-record")
        result = repo.save_structured_resume("fp-1", {"name": "Alice"})
        assert result == "resume-record"
        repo.resume.save_structured_resume.assert_called_once_with(
            owner_id=SYSTEM_OWNER_ID,
            resume_fingerprint="fp-1",
            extracted_data={"name": "Alice"},
            total_experience_years=None,
            extraction_confidence=None,
            extraction_warnings=None,
            fingerprint_version=RESUME_FINGERPRINT_VERSION,
        )

    def test_get_resume_upload(self):
        repo, _ = make_repo()
        repo.resume.get_resume_upload = MagicMock(return_value="upload")
        result = repo.get_resume_upload("upload-1", owner_id="owner-1")
        repo.resume.get_resume_upload.assert_called_once_with("upload-1", "owner-1")
        assert result == "upload"

    def test_get_latest_resume_upload_for_hash(self):
        repo, _ = make_repo()
        repo.resume.get_latest_resume_upload_for_hash = MagicMock(return_value="upload")
        result = repo.get_latest_resume_upload_for_hash("owner-1", "hash-1")
        repo.resume.get_latest_resume_upload_for_hash.assert_called_once_with("owner-1", "hash-1")
        assert result == "upload"

    def test_update_resume_upload(self):
        repo, _ = make_repo()
        repo.resume.update_resume_upload = MagicMock(return_value="updated")
        result = repo.update_resume_upload(
            "upload-1",
            status="failed",
            last_error="timeout",
            failure_debug_context={"stage": "parse"},
        )
        repo.resume.update_resume_upload.assert_called_once_with(
            "upload-1",
            status="failed",
            last_error="timeout",
            processing_task_id=None,
            failure_stage=None,
            failure_class=None,
            retryable=None,
            user_safe_message=None,
            failure_debug_context={"stage": "parse"},
        )
        assert result == "updated"

    def test_save_resume_section_embeddings(self):
        repo, _ = make_repo()
        repo.resume.save_resume_section_embeddings = MagicMock(return_value=[])
        result = repo.save_resume_section_embeddings("fp-1", [])
        repo.resume.save_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1",
            sections=[],
            owner_id=SYSTEM_OWNER_ID,
            fingerprint_version=RESUME_FINGERPRINT_VERSION,
        )
        assert result == []

    def test_get_resume_section_embeddings(self):
        repo, _ = make_repo()
        repo.resume.get_resume_section_embeddings = MagicMock(return_value=["sec-1"])
        result = repo.get_resume_section_embeddings("fp-1", section_type="summary")
        repo.resume.get_resume_section_embeddings.assert_called_once_with("fp-1", "summary")
        assert result == ["sec-1"]

    def test_get_structured_resume_by_fingerprint(self):
        repo, _ = make_repo()
        repo.resume.get_structured_resume_by_fingerprint = MagicMock(return_value={"name": "Alice"})
        result = repo.get_structured_resume_by_fingerprint("fp-1")
        repo.resume.get_structured_resume_by_fingerprint.assert_called_once_with("fp-1")
        assert result == {"name": "Alice"}

    def test_save_evidence_unit_embeddings(self):
        repo, _ = make_repo()
        repo.resume.save_evidence_unit_embeddings = MagicMock(return_value=[])
        result = repo.save_evidence_unit_embeddings("fp-1", [])
        repo.resume.save_evidence_unit_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1",
            evidence_units=[],
            owner_id=SYSTEM_OWNER_ID,
            fingerprint_version=RESUME_FINGERPRINT_VERSION,
        )
        assert result == []

    def test_find_best_evidence_for_requirement(self):
        repo, _ = make_repo()
        repo.resume.find_best_evidence_for_requirement = MagicMock(return_value=[("unit", 0.9)])
        result = repo.find_best_evidence_for_requirement([0.1], "fp-1", top_k=3)
        repo.resume.find_best_evidence_for_requirement.assert_called_once_with([0.1], "fp-1", 3)
        assert result == [("unit", 0.9)]


# ---------------------------------------------------------------------------
# match delegation
# ---------------------------------------------------------------------------

class TestMatchDelegation:
    def test_get_existing_match(self):
        repo, _ = make_repo()
        repo.match.get_existing_match = MagicMock(return_value="match")
        result = repo.get_existing_match("job-1", "fp-1", load_job_post=True)
        repo.match.get_existing_match.assert_called_once_with(
            "job-1",
            "fp-1",
            True,
            SYSTEM_OWNER_ID,
        )
        assert result == "match"

    def test_get_matches_for_resume(self):
        repo, _ = make_repo()
        repo.match.get_matches_for_resume = MagicMock(return_value=["m1"])
        result = repo.get_matches_for_resume("fp-1", min_score=70.0)
        assert result == ["m1"]

    def test_invalidate_matches_for_job(self):
        repo, _ = make_repo()
        repo.match.invalidate_matches_for_job = MagicMock(return_value=2)
        result = repo.invalidate_matches_for_job("job-1", reason="Changed")
        assert result == 2

    def test_invalidate_matches_for_resume(self):
        repo, _ = make_repo()
        repo.match.invalidate_matches_for_resume = MagicMock(return_value=1)
        result = repo.invalidate_matches_for_resume("fp-1")
        assert result == 1

    def test_invalidate_matches_for_resume_except(self):
        repo, _ = make_repo()
        repo.match.invalidate_matches_for_resume_except = MagicMock(return_value=2)
        result = repo.invalidate_matches_for_resume_except("fp-1", ["job-1"])
        repo.match.invalidate_matches_for_resume_except.assert_called_once_with(
            "fp-1",
            ["job-1"],
            "Resume changed",
        )
        assert result == 2

    def test_get_stale_matches(self):
        repo, _ = make_repo()
        repo.match.get_stale_matches = MagicMock(return_value=["s1"])
        result = repo.get_stale_matches(limit=5)
        repo.match.get_stale_matches.assert_called_once_with(5)
        assert result == ["s1"]

    def test_batch_invalidate_matches_for_jobs(self):
        repo, _ = make_repo()
        repo.match.batch_invalidate_matches_for_jobs = MagicMock(return_value=3)
        result = repo.batch_invalidate_matches_for_jobs(["j1", "j2", "j3"])
        assert result == 3


# ---------------------------------------------------------------------------
# candidate preferences repository property
# ---------------------------------------------------------------------------

class TestCandidatePreferencesRepositoryProperty:
    def test_get_preferences(self):
        repo, _ = make_repo()
        repo.candidate_preferences.get_preferences = MagicMock(return_value="prefs")
        result = repo.candidate_preferences.get_preferences("user-1")
        repo.candidate_preferences.get_preferences.assert_called_once_with("user-1")
        assert result == "prefs"


class TestFeatureCapabilityDelegation:
    def test_get_capability(self):
        repo, _ = make_repo()
        repo.user_feature_capability.get_capability = MagicMock(return_value="capability")
        result = repo.get_capability("user-1", "fit.semantic.allowed_modes")
        repo.user_feature_capability.get_capability.assert_called_once_with(
            "user-1",
            "fit.semantic.allowed_modes",
        )
        assert result == "capability"

    def test_upsert_capability(self):
        repo, _ = make_repo()
        repo.user_feature_capability.upsert_capability = MagicMock(return_value="updated")
        result = repo.upsert_capability(
            "user-1",
            "fit.semantic.preferred_mode",
            enabled=True,
            value_json={"mode": "llm"},
            source="admin",
        )
        repo.user_feature_capability.upsert_capability.assert_called_once_with(
            "user-1",
            "fit.semantic.preferred_mode",
            enabled=True,
            value_json={"mode": "llm"},
            source="admin",
        )
        assert result == "updated"


# ---------------------------------------------------------------------------
# embedding delegation
# ---------------------------------------------------------------------------

class TestEmbeddingDelegation:
    def test_find_similar_resume_sections(self):
        repo, _ = make_repo()
        repo.embedding.find_similar_resume_sections = MagicMock(return_value=["sec"])
        result = repo.find_similar_resume_sections([0.1, 0.2], section_type="summary", top_k=5)
        repo.embedding.find_similar_resume_sections.assert_called_once_with([0.1, 0.2], "summary", 5)
        assert result == ["sec"]
