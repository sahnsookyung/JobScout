#!/usr/bin/env python3
"""
Tests for Matches Router
Covers: web/backend/routers/matches.py
"""

from types import SimpleNamespace

import pytest
import uuid
from unittest.mock import Mock, patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from core.llm_evaluation import (
    LlmJudgeConflictError,
    LlmJudgeQuotaExceededError,
    LlmJudgeUnavailableError,
)
from web.backend.dependencies import get_current_user
from web.backend.exceptions import InvalidMatchOperationException
from web.backend.routers.matches import router, validate_uuid


class TestValidateUuid:
    """Test validate_uuid helper function."""

    def test_valid_uuid_v4(self):
        """Test validation of valid UUID v4."""
        valid_uuid = str(uuid.uuid4())
        result = validate_uuid(valid_uuid)
        assert result == valid_uuid

    def test_valid_uuid_string(self):
        """Test validation of valid UUID as string."""
        valid_uuid = "550e8400-e29b-41d4-a716-446655440000"
        result = validate_uuid(valid_uuid)
        assert result == valid_uuid

    def test_invalid_uuid_format(self):
        """Test validation of invalid UUID format."""
        with pytest.raises(HTTPException) as exc_info:
            validate_uuid("not-a-uuid")

        assert exc_info.value.status_code == 400
        assert "Invalid match_id format" in str(exc_info.value.detail)

    def test_invalid_uuid_empty_string(self):
        """Test validation of empty string."""
        with pytest.raises(HTTPException) as exc_info:
            validate_uuid("")

        assert exc_info.value.status_code == 400
        assert "Invalid match_id format" in str(exc_info.value.detail)

    def test_invalid_uuid_partial(self):
        """Test validation of partial UUID."""
        with pytest.raises(HTTPException) as exc_info:
            validate_uuid("550e8400-e29b")

        assert exc_info.value.status_code == 400
        assert "Invalid match_id format" in str(exc_info.value.detail)

    def test_invalid_uuid_extra_chars(self):
        """Test validation of UUID with extra characters."""
        with pytest.raises(HTTPException) as exc_info:
            validate_uuid(f"{uuid.uuid4()}-extra")

        assert exc_info.value.status_code == 400
        assert "Invalid match_id format" in str(exc_info.value.detail)


class TestMatchesRouter:
    """Test matches router endpoints."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with matches router."""
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_user] = lambda: Mock(id="user-123")
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=True)

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def mock_match_service(self):
        """Create mock match service."""
        with patch('web.backend.routers.matches.MatchService') as mock:
            mock_service_instance = Mock()
            mock.return_value = mock_service_instance
            yield mock_service_instance

    @pytest.fixture
    def mock_llm_evaluation_service(self):
        """Create mock LLM evaluation service."""
        with patch('web.backend.routers.matches.MatchLlmEvaluationService') as mock:
            mock_service_instance = Mock()
            mock.return_value = mock_service_instance
            yield mock_service_instance

    @pytest.fixture
    def mock_policy_service(self):
        """Create mock policy service."""
        with patch('web.backend.routers.matches.get_policy_service') as mock:
            policy_service = Mock()
            policy = Mock()
            policy.min_fit = 50.0
            policy.top_k = 100
            policy_service.get_current_policy.return_value = policy
            mock.return_value = policy_service
            yield policy_service

    def test_get_matches_success(self, client, mock_match_service, mock_policy_service):
        """Test successful get matches."""
        mock_match_service.get_matches.return_value = [
            {
                'match_id': str(uuid.uuid4()),
                'title': 'Software Engineer',
                'company': 'Tech Corp',
                'fit_score': 80.0,
                'is_remote': True,
                'is_hidden': False,
                'required_coverage': 0.85,
                'preferred_requirement_coverage': 0.70,
                'match_type': 'requirements_only',
                'base_score': 85.0,
                'penalties': 0.0,
            }
        ]

        response = client.get('/api/matches')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['count'] == 1
        assert 'matches' in data
        mock_match_service.get_matches.assert_called_once()
        assert mock_match_service.get_matches.call_args.kwargs["owner_id"] == "user-123"
        assert mock_match_service.get_matches.call_args.kwargs["tenant_id"] is None

    def test_get_matches_with_filters(self, client, mock_match_service, mock_policy_service):
        """Test get matches with query filters."""
        mock_match_service.get_matches.return_value = []

        response = client.get(
            '/api/matches',
            params={
                'status': 'active',
                'min_fit': 70.0,
                'top_k': 50,
                'remote_only': True,
                'show_hidden': True
            }
        )

        assert response.status_code == 200
        mock_match_service.get_matches.assert_called_once()
        call_kwargs = mock_match_service.get_matches.call_args[1]
        assert call_kwargs['status'] == 'active'
        assert call_kwargs['owner_id'] == 'user-123'
        assert call_kwargs['min_fit'] == 70.0
        assert call_kwargs['top_k'] == 50
        assert call_kwargs['remote_only'] is True
        assert call_kwargs['show_hidden'] is True

    def test_get_matches_passes_tenant_header(self, client, mock_match_service, mock_policy_service):
        mock_match_service.get_matches.return_value = []
        tenant_id = "00000000-0000-4000-8000-000000000201"

        response = client.get('/api/matches?tier=all', headers={"X-Tenant-Id": tenant_id})

        assert response.status_code == 200
        assert str(mock_match_service.get_matches.call_args.kwargs["tenant_id"]) == tenant_id

    def test_get_matches_rejects_invalid_tenant_header(self, client, mock_match_service):
        response = client.get('/api/matches', headers={"X-Tenant-Id": "not-a-uuid"})

        assert response.status_code == 400
        assert response.json()["detail"] == "X-Tenant-Id must be a UUID."
        mock_match_service.get_matches.assert_not_called()

    def test_get_matches_documents_tenant_header_validation(self, app):
        responses = app.openapi()["paths"]["/api/matches"]["get"]["responses"]

        assert responses["400"]["description"] == "Invalid tenant header"

    def test_get_matches_uses_only_top_k_policy_default(self, client, mock_match_service, mock_policy_service):
        """Test get matches only uses policy defaults for top_k when not specified."""
        mock_match_service.get_matches.return_value = []

        client.get('/api/matches')

        call_kwargs = mock_match_service.get_matches.call_args[1]
        assert call_kwargs['min_fit'] is None
        assert call_kwargs['top_k'] == 100  # From policy

    def test_get_matches_tier_all_without_explicit_top_k_returns_full_run(self, client, mock_match_service, mock_policy_service):
        mock_match_service.get_matches.return_value = []
        cfg = SimpleNamespace(matching=SimpleNamespace(two_tier_selection_enabled=True))
        with patch("core.config_loader.load_config", return_value=cfg):
            response = client.get('/api/matches', params={'tier': 'all'})

        assert response.status_code == 200
        assert mock_match_service.get_matches.call_args.kwargs["top_k"] is None

    def test_get_matches_invalid_status(self, client):
        """Test get matches with invalid status parameter."""
        response = client.get('/api/matches', params={'status': 'invalid_status'})

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        assert 'Invalid status' in data['detail']

    def test_get_matches_valid_statuses(self, client, mock_match_service, mock_policy_service):
        """Test get matches with all valid status values."""
        mock_match_service.get_matches.return_value = []

        for status in ['active', 'stale', 'all']:
            response = client.get('/api/matches', params={'status': status})
            assert response.status_code == 200

    def test_match_routes_document_invalid_match_id_response(self, app):
        schema = app.openapi()

        assert schema['paths']['/api/matches/{match_id}']['get']['responses']['400']['description'] == 'Invalid match ID'
        assert schema['paths']['/api/matches/{match_id}/hide']['post']['responses']['400']['description'] == 'Invalid match ID'
        assert schema['paths']['/api/matches/{match_id}/hide']['post']['responses']['409']['description'] == (
            'Match cannot be hidden in its current selection tier'
        )
        assert schema['paths']['/api/matches/{match_id}/explanation']['get']['responses']['400']['description'] == 'Invalid match ID'

    def test_get_matches_min_fit_bounds(self, client, mock_match_service, mock_policy_service):
        """Test get matches with min_fit boundary values."""
        mock_match_service.get_matches.return_value = []

        # Test minimum bound
        response = client.get('/api/matches', params={'min_fit': 0})
        assert response.status_code == 200

        # Test maximum bound
        response = client.get('/api/matches', params={'min_fit': 100})
        assert response.status_code == 200

        # Test out of bounds (below)
        response = client.get('/api/matches', params={'min_fit': -1})
        assert response.status_code == 422

        # Test out of bounds (above)
        response = client.get('/api/matches', params={'min_fit': 101})
        assert response.status_code == 422

    def test_get_matches_top_k_bounds(self, client, mock_match_service, mock_policy_service):
        """Test get matches with top_k boundary values."""
        mock_match_service.get_matches.return_value = []

        # Test minimum bound
        response = client.get('/api/matches', params={'top_k': 1})
        assert response.status_code == 200

        # Test maximum bound
        response = client.get('/api/matches', params={'top_k': 500})
        assert response.status_code == 200

        # Test out of bounds (below)
        response = client.get('/api/matches', params={'top_k': 0})
        assert response.status_code == 422

        # Test out of bounds (above)
        response = client.get('/api/matches', params={'top_k': 501})
        assert response.status_code == 422

    def test_get_matches_invalid_tier_returns_422(self, client, mock_match_service, mock_policy_service):
        response = client.get('/api/matches', params={'tier': 'banana'})
        assert response.status_code == 422
        assert "Invalid tier" in response.json()["detail"]

    def test_get_matches_tier_all_passed_through_when_two_tier_enabled(
        self, client, mock_match_service, mock_policy_service
    ):
        mock_match_service.get_matches.return_value = []
        cfg = SimpleNamespace(matching=SimpleNamespace(two_tier_selection_enabled=True))
        with patch("core.config_loader.load_config", return_value=cfg):
            response = client.get('/api/matches', params={'tier': 'all'})
        assert response.status_code == 200
        assert mock_match_service.get_matches.call_args.kwargs["tier"] == "all"

    def test_get_matches_tier_all_collapses_to_primary_when_disabled(
        self, client, mock_match_service, mock_policy_service
    ):
        mock_match_service.get_matches.return_value = []
        cfg = SimpleNamespace(matching=SimpleNamespace(two_tier_selection_enabled=False))
        with patch("core.config_loader.load_config", return_value=cfg):
            response = client.get('/api/matches', params={'tier': 'all'})
        assert response.status_code == 200
        assert mock_match_service.get_matches.call_args.kwargs["tier"] == "primary"

    def test_get_matches_tier_primary_default_skips_config_lookup(
        self, client, mock_match_service, mock_policy_service
    ):
        mock_match_service.get_matches.return_value = []
        with patch("core.config_loader.load_config") as mock_load:
            response = client.get('/api/matches')  # tier defaults to primary
        assert response.status_code == 200
        mock_load.assert_not_called()
        assert mock_match_service.get_matches.call_args.kwargs["tier"] == "primary"

    def test_get_match_details_success(self, client, mock_match_service):
        """Test successful get match details."""
        mock_match_service.get_match_detail.return_value = {
            'success': True,
            'match': {
                'match_id': str(uuid.uuid4()),
                'resume_fingerprint': 'fp-123',
                'fit_score': 82.0,
                'base_score': 85.0,
                'penalties': 0.0,
                'required_coverage': 0.85,
                'preferred_requirement_coverage': 0.70,
                'total_requirements': 10,
                'matched_requirements_count': 8,
                'match_type': 'requirements_only',
                'status': 'active',
                'penalty_details': {},
            },
            'job': {
                'id': 'job-123',
                'title': 'Software Engineer',
                'company': 'Tech Corp',
                'description': 'Job description here...',
            },
            'requirements': [
                {
                    'requirement_id': 'req-1',
                    'requirement_text': 'Python',
                    'similarity_score': 0.95,
                    'is_covered': True,
                    'req_type': 'required',
                    'evidence': '5 years Python experience',
                }
            ]
        }

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        mock_match_service.get_match_detail.assert_called_once_with(
            match_id,
            owner_id="user-123",
            tenant_id=None,
        )

    def test_get_match_details_invalid_uuid(self, client):
        """Test get match details with invalid UUID."""
        response = client.get('/api/matches/not-a-uuid')

        assert response.status_code == 400
        data = response.json()
        assert 'Invalid match_id format' in data['detail']

    def test_get_match_details_not_found(self, client, mock_match_service):
        """Test get match details when match not found."""
        mock_match_service.get_match_detail.side_effect = HTTPException(
            status_code=404, detail="Match not found"
        )

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}')

        assert response.status_code == 404

    def test_toggle_match_hidden_success(self, client, mock_match_service):
        """Test successful toggle match hidden."""
        mock_match_service.toggle_hidden.return_value = True

        match_id = str(uuid.uuid4())
        response = client.post(f'/api/matches/{match_id}/hide')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['match_id'] == match_id
        assert data['is_hidden'] is True
        mock_match_service.toggle_hidden.assert_called_once_with(
            match_id,
            owner_id="user-123",
            tenant_id=None,
        )

    def test_toggle_match_hidden_unhide(self, client, mock_match_service):
        """Test toggle match hidden to unhide."""
        mock_match_service.toggle_hidden.return_value = False

        match_id = str(uuid.uuid4())
        response = client.post(f'/api/matches/{match_id}/hide')

        assert response.status_code == 200
        data = response.json()
        assert data['is_hidden'] is False

    def test_toggle_match_hidden_invalid_uuid(self, client):
        """Test toggle match hidden with invalid UUID."""
        response = client.post('/api/matches/not-a-uuid/hide')

        assert response.status_code == 400
        assert 'Invalid match_id format' in response.json()['detail']

    def test_toggle_match_hidden_rejects_excluded_match(self, client, mock_match_service):
        mock_match_service.toggle_hidden.side_effect = InvalidMatchOperationException(
            "Excluded matches are browse-only and cannot be hidden."
        )

        match_id = str(uuid.uuid4())
        response = client.post(f'/api/matches/{match_id}/hide')

        assert response.status_code == 409
        assert "browse-only" in response.json()["detail"]

    def test_get_match_explanation_success(self, client, mock_match_service):
        """Test successful get match explanation."""
        mock_match_service.get_match_explanation.return_value = {
            'success': True,
            'match_id': str(uuid.uuid4()),
            'explanation': {
                'per_requirement': [
                    {
                        'requirement_id': 'req-1',
                        'requirement_text': 'Python experience',
                        'similarity': 0.85,
                        'matched_sections': ['experience']
                    }
                ],
                'section_summary': {
                    'experience': {'avg_similarity': 0.80, 'requirements_covered': 5}
                },
                'strengths': [{'section': 'experience', 'score': 0.90}],
                'gaps': []
            }
        }

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}/explanation')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert 'explanation' in data
        mock_match_service.get_match_explanation.assert_called_once_with(
            match_id,
            owner_id="user-123",
            tenant_id=None,
        )

    def test_get_match_explanation_invalid_uuid(self, client):
        """Test get match explanation with invalid UUID."""
        response = client.get('/api/matches/not-a-uuid/explanation')

        assert response.status_code == 400
        assert 'Invalid match_id format' in response.json()['detail']

    def test_get_match_explanation_not_found(self, client, mock_match_service):
        """Test get match explanation when match not found."""
        mock_match_service.get_match_explanation.side_effect = HTTPException(
            status_code=404, detail="Match not found"
        )

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}/explanation')

        assert response.status_code == 404

    def _evaluation(self, match_id=None, evaluation_id=None):
        return SimpleNamespace(
            id=evaluation_id or str(uuid.uuid4()),
            job_match_id=match_id or str(uuid.uuid4()),
            job_post_id=str(uuid.uuid4()),
            status="succeeded",
            llm_score=88.0,
            confidence=0.91,
            verdict="good",
            summary="Strong overlap on Python and backend work.",
            reason_codes=["skills_match"],
            requirement_verdicts=[],
            provider="openai",
            model="judge-model",
            prompt_version="match-judge-v1",
            schema_version="1",
            error_code=None,
            retryable=False,
            created_at=None,
            started_at=None,
            completed_at=None,
        )

    def test_list_llm_evaluations_success(self, client, mock_llm_evaluation_service):
        match_id = str(uuid.uuid4())
        evaluation = self._evaluation(match_id=match_id)
        mock_llm_evaluation_service.list_for_match.return_value = [evaluation]

        response = client.get(f'/api/matches/{match_id}/llm-evaluations')

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] == 1
        assert data["evaluations"][0]["status"] == "succeeded"
        assert "owner_id" not in data["evaluations"][0]
        mock_llm_evaluation_service.list_for_match.assert_called_once_with(
            match_id,
            owner_id="user-123",
            tenant_id=None,
        )

    def test_list_llm_evaluations_not_found(self, client, mock_llm_evaluation_service):
        match_id = str(uuid.uuid4())
        mock_llm_evaluation_service.list_for_match.side_effect = LookupError("missing")

        response = client.get(f'/api/matches/{match_id}/llm-evaluations')

        assert response.status_code == 404

    def test_generate_llm_evaluation_force_passes_tenant_header(
        self,
        client,
        mock_llm_evaluation_service,
    ):
        match_id = str(uuid.uuid4())
        tenant_id = "00000000-0000-4000-8000-000000000201"
        evaluation = self._evaluation(match_id=match_id)
        mock_llm_evaluation_service.start_for_match.return_value = SimpleNamespace(
            evaluation=evaluation,
            reused=False,
            should_run=False,
        )

        response = client.post(
            f'/api/matches/{match_id}/llm-evaluations',
            headers={"X-Tenant-Id": tenant_id},
            json={"force": True},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["reused"] is False
        assert data["accepted"] is False
        assert data["evaluation"]["id"] == str(evaluation.id)
        mock_llm_evaluation_service.start_for_match.assert_called_once()
        call_kwargs = mock_llm_evaluation_service.start_for_match.call_args.kwargs
        assert call_kwargs["owner_id"] == "user-123"
        assert str(call_kwargs["tenant_id"]) == tenant_id
        assert call_kwargs["force"] is True

    def test_generate_llm_evaluation_queues_background_task(
        self,
        client,
        mock_llm_evaluation_service,
    ):
        match_id = str(uuid.uuid4())
        evaluation = self._evaluation(match_id=match_id)
        mock_llm_evaluation_service.start_for_match.return_value = SimpleNamespace(
            evaluation=evaluation,
            reused=False,
            should_run=True,
            provider_payload={"job": {"description": "Full JD"}},
            truncation={"truncated": False, "fields": {}},
        )

        with patch("web.backend.routers.matches._run_match_llm_evaluation_background") as background:
            response = client.post(f'/api/matches/{match_id}/llm-evaluations', json={})

        assert response.status_code == 200
        data = response.json()
        assert data["accepted"] is True
        assert data["message"] == "Queued LLM evaluation."
        background.assert_called_once_with(
            str(evaluation.id),
            {"job": {"description": "Full JD"}},
            {"truncated": False, "fields": {}},
        )

    @pytest.mark.parametrize(
        ("exc", "status_code"),
        [
            (LookupError("missing"), 404),
            (LlmJudgeConflictError("running"), 409),
            (LlmJudgeQuotaExceededError("quota"), 429),
            (LlmJudgeUnavailableError("disabled"), 503),
        ],
    )
    def test_generate_llm_evaluation_error_mapping(
        self,
        client,
        mock_llm_evaluation_service,
        exc,
        status_code,
    ):
        match_id = str(uuid.uuid4())
        mock_llm_evaluation_service.start_for_match.side_effect = exc

        response = client.post(f'/api/matches/{match_id}/llm-evaluations', json={})

        assert response.status_code == status_code

    def test_delete_llm_evaluation_success(self, client, mock_llm_evaluation_service):
        match_id = str(uuid.uuid4())
        evaluation_id = str(uuid.uuid4())

        response = client.delete(f'/api/matches/{match_id}/llm-evaluations/{evaluation_id}')

        assert response.status_code == 200
        assert response.json()["evaluation"] is None
        mock_llm_evaluation_service.delete_evaluation.assert_called_once_with(
            match_id,
            evaluation_id,
            owner_id="user-123",
            tenant_id=None,
        )

    def test_delete_llm_evaluation_not_found(self, client, mock_llm_evaluation_service):
        match_id = str(uuid.uuid4())
        evaluation_id = str(uuid.uuid4())
        mock_llm_evaluation_service.delete_evaluation.side_effect = LookupError("missing")

        response = client.delete(f'/api/matches/{match_id}/llm-evaluations/{evaluation_id}')

        assert response.status_code == 404


class TestMatchesRouterIntegration:
    """Integration tests for matches router with mocked dependencies."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with matches router."""
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_user] = lambda: Mock(id="user-123")
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=True)

    def test_full_get_matches_flow(self, client):
        """Test complete flow of getting matches."""
        with patch('web.backend.routers.matches.MatchService') as MockMatchService:
            with patch('web.backend.routers.matches.get_policy_service') as MockPolicyService:
                # Setup policy service
                mock_policy_service = Mock()
                mock_policy = Mock()
                mock_policy.min_fit = 50.0
                mock_policy.top_k = 100
                mock_policy_service.get_current_policy.return_value = mock_policy
                MockPolicyService.return_value = mock_policy_service

                # Setup match service - mock returns instance when called
                mock_match_service = Mock()
                from web.backend.models.responses import MatchSummary
                mock_match_service.get_matches.return_value = [
                    MatchSummary(
                        match_id=str(uuid.uuid4()),
                        title='Senior Software Engineer',
                        company='Google',
                        location='Mountain View, CA',
                        is_remote=False,
                        fit_score=92.5,
                        preference_score=None,
                        required_coverage=0.95,
                        preferred_requirement_coverage=0.80,
                        match_type='requirements_only',
                        is_hidden=False,
                        penalties=0.0,
                    )
                ]
                MockMatchService.return_value = mock_match_service

                response = client.get('/api/matches')

                assert response.status_code == 200
                data = response.json()
                assert data['success'] is True
                assert data['count'] >= 1
                assert len(data['matches']) == 1

                match = data['matches'][0]
                assert match['title'] == 'Senior Software Engineer'
                assert match['company'] == 'Google'
                assert match['fit_score'] == 92.5

    def test_full_match_details_flow(self, client):
        """Test complete flow of getting match details."""
        with patch('web.backend.routers.matches.MatchService') as MockMatchService:
            mock_match_service = Mock()
            match_id = str(uuid.uuid4())
            mock_match_service.get_match_detail.return_value = {
                'success': True,
                'match': {
                    'match_id': match_id,
                    'resume_fingerprint': 'fp-123',
                    'fit_score': 82.0,
                    'base_score': 85.0,
                    'penalties': 0.0,
                    'required_coverage': 0.85,
                    'preferred_requirement_coverage': 0.70,
                    'total_requirements': 10,
                    'matched_requirements_count': 8,
                    'match_type': 'requirements_only',
                    'status': 'active',
                    'penalty_details': {},
                },
                'job': {
                    'id': 'job-123',
                    'title': 'Software Engineer',
                    'company': 'Tech Corp',
                    'description': 'Job description here...',
                },
                'requirements': []
            }
            MockMatchService.return_value = mock_match_service

            response = client.get(f'/api/matches/{match_id}')

            assert response.status_code == 200
            data = response.json()
            assert data['success'] is True
            assert data['match']['match_id'] == match_id
            assert data['job']['title'] == 'Software Engineer'

    def test_full_toggle_hidden_flow(self, client):
        """Test complete flow of toggling hidden status."""
        with patch('web.backend.routers.matches.MatchService') as MockMatchService:
            mock_match_service = Mock()
            mock_match_service.toggle_hidden.side_effect = [True, False]
            MockMatchService.return_value = mock_match_service

            match_id = str(uuid.uuid4())

            # First toggle - hide
            response1 = client.post(f'/api/matches/{match_id}/hide')
            assert response1.status_code == 200
            assert response1.json()['is_hidden'] is True

            # Second toggle - unhide
            response2 = client.post(f'/api/matches/{match_id}/hide')
            assert response2.status_code == 200
            assert response2.json()['is_hidden'] is False

    def test_full_explanation_flow(self, client):
        """Test complete flow of getting match explanation."""
        with patch('web.backend.routers.matches.MatchService') as MockMatchService:
            mock_match_service = Mock()
            mock_match_service.get_match_explanation.return_value = {
                'success': True,
                'match_id': str(uuid.uuid4()),
                'explanation': {
                    'per_requirement': [
                        {
                            'requirement_id': 'req-1',
                            'requirement_text': '5+ years Python experience',
                            'similarity': 0.88,
                            'details': {
                                'best_section': 'experience',
                                'best_section_text': 'Worked with Python for 6 years...'
                            }
                        },
                        {
                            'requirement_id': 'req-2',
                            'requirement_text': 'AWS experience',
                            'similarity': 0.65,
                            'details': {
                                'best_section': 'skills',
                                'best_section_text': 'AWS: EC2, S3, Lambda'
                            }
                        }
                    ],
                    'section_summary': {
                        'experience': {'avg_similarity': 0.85, 'max_similarity': 0.95, 'requirements_covered': 8},
                        'skills': {'avg_similarity': 0.70, 'max_similarity': 0.80, 'requirements_covered': 5}
                    },
                    'strengths': [
                        {'section': 'experience', 'score': 0.95},
                        {'section': 'projects', 'score': 0.88}
                    ],
                    'gaps': [
                        {'section': 'skills', 'avg_score': 0.70}
                    ]
                }
            }
            MockMatchService.return_value = mock_match_service

            match_id = str(uuid.uuid4())
            response = client.get(f'/api/matches/{match_id}/explanation')

            assert response.status_code == 200
            data = response.json()
            assert data['success'] is True
            assert 'explanation' in data
            explanation = data['explanation']
            assert len(explanation.get('strengths', [])) == 2
            assert len(explanation.get('gaps', [])) == 1
            assert 'section_summary' in explanation
