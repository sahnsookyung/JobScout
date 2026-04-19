"""Unit tests for core/matcher/service.py — MatcherService."""

import threading
from unittest.mock import MagicMock, patch

import pytest

from core.matcher.models import JobMatchPreliminary


def make_service():
    """Create a MatcherService with mocked profiler and config."""
    mock_profiler = MagicMock()
    mock_config = MagicMock()
    mock_config.similarity_threshold = 0.5
    mock_config.batch_size = 100
    mock_config.hybrid_retrieval_enabled = False
    mock_config.lexical_limit = 100
    mock_config.fusion_rank_constant = 60
    mock_config.lexical_query_token_limit = 24
    with patch("core.matcher.service.RequirementMatcher"):
        from core.matcher.service import MatcherService

        service = MatcherService(mock_profiler, mock_config)
    return service, mock_profiler, mock_config


def make_repo():
    repo = MagicMock()
    repo.is_resume_ready.return_value = False
    repo.get_top_jobs_by_lexical_query.return_value = []
    return repo


class TestMatchResumeTwoStage:
    def test_raises_value_error_when_no_fingerprint(self):
        service, _, _ = make_service()
        with pytest.raises(ValueError, match="resume_fingerprint is required"):
            service.match_resume_two_stage(make_repo(), {}, resume_fingerprint=None)

    def test_raises_when_fingerprint_empty_string(self):
        service, _, _ = make_service()
        with pytest.raises(ValueError):
            service.match_resume_two_stage(make_repo(), {}, resume_fingerprint="")

    def test_returns_empty_when_no_evidence_units(self):
        service, mock_profiler, _ = make_service()
        mock_profiler.profile_resume.return_value = (MagicMock(), [], None)

        result = service.match_resume_two_stage(make_repo(), {}, resume_fingerprint="fp-1")
        assert result == []

    def test_returns_empty_when_no_candidates(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1, 0.2]
        repo.get_top_jobs_by_summary_embedding.return_value = []

        result = service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-1")
        assert result == []

    def test_raises_when_embedding_not_found(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = None

        with pytest.raises(ValueError, match="No summary embedding"):
            service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-1")

    def test_stop_event_interrupts_stage2_loop(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        stop_event = threading.Event()
        stop_event.set()

        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1, 0.2]
        repo.get_top_jobs_by_summary_embedding.return_value = [(MagicMock(), 0.9)]

        result = service.match_resume_two_stage(
            repo, {}, resume_fingerprint="fp-1", stop_event=stop_event
        )
        assert result == []

    def test_full_pipeline_returns_sorted_matches(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1, 0.2]

        job_a = MagicMock()
        job_b = MagicMock()
        repo.get_top_jobs_by_summary_embedding.return_value = [(job_a, 0.7), (job_b, 0.95)]
        service.requirement_matcher.match_requirements.return_value = ([], [])

        result = service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-1")

        assert len(result) == 2
        assert result[0].job_similarity >= result[1].job_similarity
        assert result[0].job_similarity == pytest.approx(0.95)
        assert result[0].retrieval_score == pytest.approx(0.95)
        assert result[1].job_similarity == pytest.approx(0.7)

    def test_pre_extracted_resume_passed_to_profiler(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [], None)
        pre_extracted = MagicMock()

        service.match_resume_two_stage(
            repo, {}, resume_fingerprint="fp-1", pre_extracted_resume=pre_extracted
        )

        call_kwargs = mock_profiler.profile_resume.call_args[1]
        assert call_kwargs.get("pre_extracted_resume") is pre_extracted

    def test_pre_extracted_ready_resume_reuses_persisted_artifacts(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        repo.is_resume_ready.return_value = True
        repo.get_resume_summary_embedding.return_value = [0.1, 0.2]
        repo.get_top_jobs_by_summary_embedding.return_value = []
        pre_extracted = MagicMock()

        result = service.match_resume_two_stage(
            repo,
            {},
            resume_fingerprint="fp-ready",
            pre_extracted_resume=pre_extracted,
        )

        assert result == []
        mock_profiler.profile_resume.assert_not_called()
        repo.is_resume_ready.assert_called_once_with("fp-ready")

    def test_pre_extracted_ready_resume_honors_cancelled_stop_event_before_db_work(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        repo.is_resume_ready.return_value = True
        stop_event = threading.Event()
        stop_event.set()

        result = service.match_resume_two_stage(
            repo,
            {},
            resume_fingerprint="fp-ready",
            pre_extracted_resume=MagicMock(),
            stop_event=stop_event,
        )

        assert result == []
        mock_profiler.profile_resume.assert_not_called()
        repo.is_resume_ready.assert_not_called()
        repo.get_top_jobs_by_summary_embedding.assert_not_called()

    def test_tenant_id_passed_to_retrieval(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1]
        repo.get_top_jobs_by_summary_embedding.return_value = []

        service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-1", tenant_id="tenant-xyz")

        call_kwargs = repo.get_top_jobs_by_summary_embedding.call_args[1]
        assert call_kwargs.get("tenant_id") == "tenant-xyz"

    def test_stop_event_none_processes_all_candidates(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1]
        jobs = [(MagicMock(), 0.9), (MagicMock(), 0.8), (MagicMock(), 0.7)]
        repo.get_top_jobs_by_summary_embedding.return_value = jobs
        service.requirement_matcher.match_requirements.return_value = ([], [])

        result = service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-1", stop_event=None)
        assert len(result) == 3

    def test_fingerprints_stored_in_preliminaries(self):
        service, mock_profiler, _ = make_service()
        repo = make_repo()
        mock_profiler.profile_resume.return_value = (MagicMock(), [MagicMock()], None)
        repo.get_resume_summary_embedding.return_value = [0.1]
        repo.get_top_jobs_by_summary_embedding.return_value = [(MagicMock(), 0.8)]
        service.requirement_matcher.match_requirements.return_value = ([], [])

        result = service.match_resume_two_stage(repo, {}, resume_fingerprint="fp-abc")
        assert result[0].resume_fingerprint == "fp-abc"


class TestGetResumeEmbeddingOrRaise:
    def test_returns_embedding_when_found(self):
        service, _, _ = make_service()
        repo = make_repo()
        repo.get_resume_summary_embedding.return_value = [0.1, 0.2, 0.3]

        result = service._get_resume_embedding_or_raise(repo, "fp-1")
        assert result == [0.1, 0.2, 0.3]
        repo.get_resume_summary_embedding.assert_called_once_with("fp-1")

    def test_raises_when_not_found(self):
        service, _, _ = make_service()
        repo = make_repo()
        repo.get_resume_summary_embedding.return_value = None

        with pytest.raises(ValueError, match="No summary embedding"):
            service._get_resume_embedding_or_raise(repo, "fp-1")


class TestRetrieveCandidates:
    def test_returns_dense_candidates_when_hybrid_retrieval_disabled(self):
        service, _, _ = make_service()
        repo = make_repo()
        pairs = [(MagicMock(), 0.9), (MagicMock(), 0.7)]
        repo.get_top_jobs_by_summary_embedding.return_value = pairs

        result = service._retrieve_candidates(repo, {}, [0.1, 0.2], tenant_id=None)

        assert [candidate.job_similarity for candidate in result] == [0.9, 0.7]
        assert [candidate.retrieval_score for candidate in result] == [0.9, 0.7]
        repo.get_top_jobs_by_lexical_query.assert_not_called()

    def test_returns_empty_when_no_candidates(self):
        service, _, _ = make_service()
        repo = make_repo()
        repo.get_top_jobs_by_summary_embedding.return_value = []

        result = service._retrieve_candidates(repo, {}, [0.1], tenant_id=None)
        assert result == []

    def test_passes_tenant_id_to_query(self):
        service, _, _ = make_service()
        repo = make_repo()
        repo.get_top_jobs_by_summary_embedding.return_value = []

        service._retrieve_candidates(repo, {}, [0.1], tenant_id="t-1")

        call_kwargs = repo.get_top_jobs_by_summary_embedding.call_args[1]
        assert call_kwargs.get("tenant_id") == "t-1"

    def test_uses_config_batch_size_as_limit(self):
        service, _, mock_config = make_service()
        mock_config.batch_size = 50
        repo = make_repo()
        repo.get_top_jobs_by_summary_embedding.return_value = []

        service._retrieve_candidates(repo, {}, [0.1], tenant_id=None)

        call_kwargs = repo.get_top_jobs_by_summary_embedding.call_args[1]
        assert call_kwargs.get("limit") == 50

    def test_hybrid_retrieval_fuses_dense_and_lexical_results(self):
        service, _, mock_config = make_service()
        mock_config.hybrid_retrieval_enabled = True
        mock_config.batch_size = 3
        repo = make_repo()
        dense_job = MagicMock()
        dense_job.id = "dense"
        shared_job = MagicMock()
        shared_job.id = "shared"
        lexical_job = MagicMock()
        lexical_job.id = "lexical"
        repo.get_top_jobs_by_summary_embedding.return_value = [(dense_job, 0.91), (shared_job, 0.6)]
        repo.get_top_jobs_by_lexical_query.return_value = [(shared_job, 0.7, 0.6), (lexical_job, 0.5, 0.55)]

        result = service._retrieve_candidates(
            repo,
            {"sections": [{"items": [{"description": "Python FastAPI AWS"}]}]},
            [0.1],
            tenant_id=None,
        )

        assert [candidate.job.id for candidate in result] == ["shared", "dense", "lexical"]
        assert result[0].job_similarity == pytest.approx(0.6)
        assert result[0].lexical_score == pytest.approx(0.7)
        repo.get_top_jobs_by_lexical_query.assert_called_once()

    def test_hybrid_retrieval_skips_lexical_query_when_no_tokens(self):
        service, _, mock_config = make_service()
        mock_config.hybrid_retrieval_enabled = True
        repo = make_repo()
        repo.get_top_jobs_by_summary_embedding.return_value = [(MagicMock(), 0.9)]

        result = service._retrieve_candidates(repo, {"sections": [None]}, [0.1], tenant_id=None)

        assert len(result) == 1
        repo.get_top_jobs_by_lexical_query.assert_not_called()

    def test_build_lexical_query_text_deduplicates_and_limits_tokens(self):
        service, _, mock_config = make_service()
        mock_config.lexical_query_token_limit = 4

        result = service._build_lexical_query_text(
            {
                "title": "Senior Python Engineer",
                "sections": [
                    {"items": [{"description": "Python AWS FastAPI Kubernetes Python"}]},
                ],
            }
        )

        assert result == "senior | python | engineer | aws"


class TestBuildPreliminary:
    def test_returns_job_match_preliminary(self):
        service, _, _ = make_service()
        repo = make_repo()
        mock_job = MagicMock()
        mock_job.requirements = []
        matched = [MagicMock()]
        missing = [MagicMock()]
        service.requirement_matcher.match_requirements.return_value = (matched, missing)

        result = service._build_preliminary(
            repo,
            mock_job,
            0.85,
            "fp-1",
            retrieval_score=0.9,
            lexical_score=0.4,
        )

        assert isinstance(result, JobMatchPreliminary)
        assert result.job is mock_job
        assert result.job_similarity == pytest.approx(0.85)
        assert result.requirement_matches == matched
        assert result.missing_requirements == missing
        assert result.resume_fingerprint == "fp-1"
        assert result.retrieval_score == pytest.approx(0.9)
        assert result.lexical_score == pytest.approx(0.4)

    def test_calls_requirement_matcher(self):
        service, _, _ = make_service()
        repo = make_repo()
        mock_job = MagicMock()
        mock_job.requirements = ["req-1", "req-2"]
        service.requirement_matcher.match_requirements.return_value = ([], [])

        service._build_preliminary(repo, mock_job, 0.7, "fp-abc")

        service.requirement_matcher.match_requirements.assert_called_once_with(
            repo, ["req-1", "req-2"], "fp-abc", top_k=5
        )

    def test_zero_similarity(self):
        service, _, _ = make_service()
        repo = make_repo()
        mock_job = MagicMock()
        service.requirement_matcher.match_requirements.return_value = ([], [])

        result = service._build_preliminary(repo, mock_job, 0.0, "fp-1")
        assert result.job_similarity == pytest.approx(0.0)
        assert result.retrieval_score == pytest.approx(0.0)

    @patch("core.matcher.service.rerank_requirement_evidence")
    def test_reranks_requirement_evidence_when_cross_encoder_available(self, mock_rerank):
        service, _, _ = make_service()
        repo = make_repo()
        mock_job = MagicMock()
        mock_job.requirements = ["req-1"]
        matched = [MagicMock()]
        missing = [MagicMock()]
        service.requirement_matcher.match_requirements.return_value = (matched, missing)
        service.cross_encoder_provider = MagicMock()

        service._build_preliminary(repo, mock_job, 0.85, "fp-1")

        mock_rerank.assert_called_once_with(
            provider=service.cross_encoder_provider,
            requirement_matches=matched + missing,
        )
