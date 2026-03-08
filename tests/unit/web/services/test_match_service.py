#!/usr/bin/env python3
"""
Tests for Match Service
Covers: web/backend/services/match_service.py
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.orm import Session


class TestMatchServiceGetMatches:
    """Test MatchService.get_matches method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service(self, mock_db):
        """Create MatchService instance."""
        from web.backend.services.match_service import MatchService
        return MatchService(mock_db)

    def test_get_matches_all_filters(self, service, mock_db):
        """Test get_matches with all filters applied."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.limit.return_value = mock_query

        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.job_post = Mock(
            id="job-1",
            title="Developer",
            company="TechCorp",
            location_text="Remote",
            is_remote=True
        )
        mock_query.all.return_value = [mock_match]

        results = service.get_matches(
            status="active",
            min_fit=0.7,
            top_k=10,
            remote_only=True,
            show_hidden=False
        )

        assert len(results) == 1
        assert mock_query.filter.call_count >= 2  # status and min_fit filters

    def test_get_matches_status_filter(self, service, mock_db):
        """Test get_matches with status filter."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(status="active")

        # Should filter by status
        mock_query.filter.assert_called()

    def test_get_matches_status_all(self, service, mock_db):
        """Test get_matches with status='all' (no status filter)."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(status="all")

        # Should not apply status filter
        assert mock_query.filter.call_count == 0

    def test_get_matches_min_fit_filter(self, service, mock_db):
        """Test get_matches with min_fit filter."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(min_fit=0.8)

        mock_query.filter.assert_called()

    def test_get_matches_remote_filter(self, service, mock_db):
        """Test get_matches with remote_only filter."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(remote_only=True)

        mock_query.join.assert_called()

    def test_get_matches_top_k_limit(self, service, mock_db):
        """Test get_matches with top_k limit."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(top_k=5)

        mock_query.limit.assert_called_once_with(5)

    def test_get_matches_hidden_filter(self, service, mock_db):
        """Test get_matches excludes hidden by default."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(show_hidden=False)

        mock_query.filter.assert_called()

    def test_get_matches_show_hidden(self, service, mock_db):
        """Test get_matches includes hidden when requested."""
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches(show_hidden=True)

        # Should not filter by hidden
        filter_calls = [str(call) for call in mock_query.filter.call_args_list]
        assert not any('is_hidden' in call for call in filter_calls)

    def test_get_matches_eager_loading(self, service, mock_db):
        """Test get_matches uses eager loading for job_post."""
        from sqlalchemy.orm import joinedload
        from database.models import JobMatch

        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        service.get_matches()

        mock_query.options.assert_called()


class TestMatchServiceGetMatchDetail:
    """Test MatchService.get_match_detail method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service(self, mock_db):
        """Create MatchService instance."""
        from web.backend.services.match_service import MatchService
        return MatchService(mock_db)

    def test_get_match_detail_success(self, service, mock_db):
        """Test successful match detail retrieval."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.job_post_id = "job-1"
        mock_match.resume_fingerprint = "fp-123"
        mock_match.fit_score = 0.85
        mock_match.want_score = 0.75
        mock_match.overall_score = 0.80
        mock_match.penalty_details = None

        mock_job = Mock()
        mock_job.id = "job-1"
        mock_job.title = "Developer"

        mock_requirement = Mock()
        mock_requirement.job_match_id = "match-1"
        mock_requirement.requirement = Mock(text="Python experience")
        mock_requirement.evidence_text = "5 years Python"
        mock_requirement.similarity_score = 0.9

        mock_db.query.return_value.get.side_effect = [mock_match, mock_job]
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = [mock_requirement]

        result = service.get_match_detail("match-1")

        assert result.success is True
        assert result.match.match_id == "match-1"
        assert result.job.title == "Developer"
        assert len(result.requirements) == 1

    def test_get_match_detail_not_found(self, service, mock_db):
        """Test get_match_detail when match not found."""
        from web.backend.exceptions import MatchNotFoundException

        mock_db.query.return_value.get.return_value = None

        with pytest.raises(MatchNotFoundException) as exc_info:
            service.get_match_detail("nonexistent")

        assert "not found" in str(exc_info.value)

    def test_get_match_detail_no_job(self, service, mock_db):
        """Test get_match_detail when job not found."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.job_post_id = "job-1"

        mock_db.query.return_value.get.side_effect = [mock_match, None]
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = []

        result = service.get_match_detail("match-1")

        assert result.success is True
        assert result.job.job_id is None

    def test_get_match_detail_database_error(self, service, mock_db):
        """Test get_match_detail handles database errors."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.job_post_id = "job-1"

        mock_db.query.return_value.get.side_effect = [mock_match, Exception("DB error")]

        with pytest.raises(Exception):
            service.get_match_detail("match-1")


class TestMatchServiceToggleHidden:
    """Test MatchService.toggle_hidden method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service(self, mock_db):
        """Create MatchService instance."""
        from web.backend.services.match_service import MatchService
        return MatchService(mock_db)

    def test_toggle_hidden_success(self, service, mock_db):
        """Test successful hidden toggle."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.is_hidden = False

        with patch('web.backend.services.match_service.MatchRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo.get_match_by_id.return_value = mock_match
            mock_repo_class.return_value = mock_repo

            result = service.toggle_hidden("match-1")

            assert result is True  # Toggled from False to True
            mock_repo.update_hidden_status.assert_called_once_with("match-1", True)
            mock_db.commit.assert_called_once()

    def test_toggle_hidden_unhide(self, service, mock_db):
        """Test toggle hidden when already hidden."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.is_hidden = True

        with patch('web.backend.services.match_service.MatchRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo.get_match_by_id.return_value = mock_match
            mock_repo_class.return_value = mock_repo

            result = service.toggle_hidden("match-1")

            assert result is False  # Toggled from True to False

    def test_toggle_hidden_not_found(self, service, mock_db):
        """Test toggle_hidden when match not found."""
        from web.backend.exceptions import MatchNotFoundException

        with patch('web.backend.services.match_service.MatchRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo.get_match_by_id.return_value = None
            mock_repo_class.return_value = mock_repo

            with pytest.raises(MatchNotFoundException):
                service.toggle_hidden("nonexistent")


class TestMatchServiceGetMatchExplanation:
    """Test MatchService.get_match_explanation method."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service(self, mock_db):
        """Create MatchService instance."""
        from web.backend.services.match_service import MatchService
        return MatchService(mock_db)

    def test_get_match_explanation_success(self, service, mock_db):
        """Test successful match explanation."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.resume_fingerprint = "fp-123"
        mock_match.job_post_id = "job-1"

        mock_job = Mock()
        mock_job.id = "job-1"
        mock_job.requirements = ["Python", "SQL"]

        mock_db.query.return_value.get.side_effect = [mock_match, mock_job]

        with patch('web.backend.services.match_service.explain_match') as mock_explain:
            mock_explain.return_value = {"explanation": "test"}

            result = service.get_match_explanation("match-1")

            assert result['success'] is True
            assert result['match_id'] == "match-1"
            assert 'explanation' in result

    def test_get_match_explanation_not_found(self, service, mock_db):
        """Test get_match_explanation when match not found."""
        from web.backend.exceptions import MatchNotFoundException

        mock_db.query.return_value.get.return_value = None

        with pytest.raises(MatchNotFoundException):
            service.get_match_explanation("nonexistent")

    def test_get_match_explanation_no_fingerprint(self, service, mock_db):
        """Test get_match_explanation when no fingerprint."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.resume_fingerprint = None

        mock_db.query.return_value.get.return_value = mock_match

        result = service.get_match_explanation("match-1")

        assert result['success'] is True
        assert result['explanation'] is None
        assert 'no resume fingerprint' in result['message'].lower()

    def test_get_match_explanation_job_not_found(self, service, mock_db):
        """Test get_match_explanation when job not found."""
        from web.backend.exceptions import JobNotFoundException

        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.resume_fingerprint = "fp-123"
        mock_match.job_post_id = "job-1"

        mock_db.query.return_value.get.side_effect = [mock_match, None]

        with pytest.raises(JobNotFoundException):
            service.get_match_explanation("match-1")

    def test_get_match_explanation_no_requirements(self, service, mock_db):
        """Test get_match_explanation when job has no requirements."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.resume_fingerprint = "fp-123"
        mock_match.job_post_id = "job-1"

        mock_job = Mock()
        mock_job.id = "job-1"
        mock_job.requirements = None

        mock_db.query.return_value.get.side_effect = [mock_match, mock_job]

        result = service.get_match_explanation("match-1")

        assert result['success'] is True
        assert result['explanation'] is None
        assert 'no requirements' in result['message'].lower()


class TestMatchServiceHelpers:
    """Test MatchService helper methods."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def service(self, mock_db):
        """Create MatchService instance."""
        from web.backend.services.match_service import MatchService
        return MatchService(mock_db)

    def test_to_match_summary_success(self, service):
        """Test _to_match_summary with complete data."""
        from datetime import datetime, timezone

        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.fit_score = 0.85
        mock_match.want_score = 0.75
        mock_match.overall_score = 0.80
        mock_match.base_score = 0.70
        mock_match.penalties = 0.05
        mock_match.required_coverage = 0.90
        mock_match.preferred_coverage = 0.60
        mock_match.match_type = "exact"
        mock_match.is_hidden = False
        mock_match.created_at = datetime.now(timezone.utc)
        mock_match.calculated_at = datetime.now(timezone.utc)

        mock_job = Mock()
        mock_job.id = "job-1"
        mock_job.title = "Developer"
        mock_job.company = "TechCorp"
        mock_job.location_text = "Remote"
        mock_job.is_remote = True

        mock_match.job_post = mock_job

        result = service._to_match_summary(mock_match)

        assert result.match_id == "match-1"
        assert result.job_id == "job-1"
        assert result.title == "Developer"
        assert result.company == "TechCorp"
        assert result.is_remote is True

    def test_to_match_summary_job_error(self, service):
        """Test _to_match_summary handles job access errors."""
        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.job_post = None
        mock_match.fit_score = None
        mock_match.want_score = None
        mock_match.overall_score = None

        result = service._to_match_summary(mock_match)

        assert result.title == "Unknown"
        assert result.company == "Unknown"
        assert result.job_id is None

    def test_to_match_detail(self, service):
        """Test _to_match_detail."""
        from datetime import datetime, timezone

        mock_match = Mock()
        mock_match.id = "match-1"
        mock_match.resume_fingerprint = "fp-123"
        mock_match.fit_score = 0.85
        mock_match.want_score = 0.75
        mock_match.overall_score = 0.80
        mock_match.fit_components = {"skill": 0.9}
        mock_match.want_components = {"culture": 0.8}
        mock_match.fit_weight = 0.7
        mock_match.want_weight = 0.3
        mock_match.base_score = 0.70
        mock_match.penalties = 0.05
        mock_match.required_coverage = 0.90
        mock_match.preferred_coverage = 0.60
        mock_match.total_requirements = 10
        mock_match.matched_requirements_count = 8
        mock_match.match_type = "exact"
        mock_match.status = "active"
        mock_match.created_at = datetime.now(timezone.utc)
        mock_match.calculated_at = datetime.now(timezone.utc)
        mock_match.penalty_details = None

        result = service._to_match_detail(mock_match, {})

        assert result.match_id == "match-1"
        assert result.fit_score == 0.85
        assert result.penalty_details == {}

    def test_to_job_details_success(self, service):
        """Test _to_job_details with complete data."""
        mock_job = Mock()
        mock_job.id = "job-1"
        mock_job.title = "Developer"
        mock_job.company = "TechCorp"
        mock_job.location_text = "Remote"
        mock_job.is_remote = True
        mock_job.description = "Job description"
        mock_job.salary_min = 100000
        mock_job.salary_max = 150000
        mock_job.currency = "USD"
        mock_job.min_years_experience = 5
        mock_job.requires_degree = True
        mock_job.security_clearance = None
        mock_job.job_level = "mid"

        result = service._to_job_details(mock_job)

        assert result.job_id == "job-1"
        assert result.title == "Developer"
        assert result.salary_min == 100000.0

    def test_to_job_details_null(self, service):
        """Test _to_job_details with null job."""
        result = service._to_job_details(None)

        assert result.job_id is None
        assert result.title is None

    def test_to_requirement_detail(self, service):
        """Test _to_requirement_detail."""
        mock_req = Mock()
        mock_req.job_requirement_unit_id = "req-1"
        mock_req.requirement = Mock(text="Python experience")
        mock_req.evidence_text = "5 years Python"
        mock_req.evidence_section = "skills"
        mock_req.similarity_score = 0.9
        mock_req.is_covered = True
        mock_req.req_type = "required"

        result = service._to_requirement_detail(mock_req)

        assert result.requirement_id == "req-1"
        assert result.requirement_text == "Python experience"
        assert result.is_covered is True

    def test_parse_penalty_details_dict(self, service):
        """Test _parse_penalty_details with dict input."""
        penalty_details = {"missing_skill": "Python"}
        result = service._parse_penalty_details(penalty_details)
        assert result == penalty_details

    def test_parse_penalty_details_json(self, service):
        """Test _parse_penalty_details with JSON string."""
        penalty_details = '{"missing_skill": "Python"}'
        result = service._parse_penalty_details(penalty_details)
        assert result == {"missing_skill": "Python"}

    def test_parse_penalty_details_invalid_json(self, service):
        """Test _parse_penalty_details with invalid JSON."""
        penalty_details = "not valid json"
        result = service._parse_penalty_details(penalty_details)
        assert result == {}

    def test_parse_penalty_details_none(self, service):
        """Test _parse_penalty_details with None."""
        result = service._parse_penalty_details(None)
        assert result == {}

    def test_parse_penalty_details_other(self, service):
        """Test _parse_penalty_details with unexpected type."""
        penalty_details = 123
        result = service._parse_penalty_details(penalty_details)
        assert result == {}
