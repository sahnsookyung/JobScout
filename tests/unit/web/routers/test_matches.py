#!/usr/bin/env python3
"""
Tests for Matches Router
Covers: web/backend/routers/matches.py
"""

import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

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
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def mock_match_service(self):
        """Create mock match service."""
        with patch('web.backend.routers.matches.MatchService') as mock:
            yield mock

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
            yield mock

    def test_get_matches_success(self, client, mock_match_service, mock_policy_service):
        """Test successful get matches."""
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = [
            {
                'match_id': str(uuid.uuid4()),
                'title': 'Software Engineer',
                'company': 'Tech Corp',
                'overall_score': 85.5,
                'fit_score': 80.0,
                'is_remote': True,
                'is_hidden': False,
                'required_coverage': 0.85,
                'match_type': 'requirements_only'
            }
        ]
        mock_match_service.return_value = mock_service_instance

        response = client.get('/api/matches')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['count'] == 1
        assert 'matches' in data
        mock_service_instance.get_matches.assert_called_once()

    def test_get_matches_with_filters(self, client, mock_match_service, mock_policy_service):
        """Test get matches with query filters."""
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = []
        mock_match_service.return_value = mock_service_instance

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
        mock_service_instance.get_matches.assert_called_once()
        call_kwargs = mock_service_instance.get_matches.call_args[1]
        assert call_kwargs['status'] == 'active'
        assert call_kwargs['min_fit'] == 70.0
        assert call_kwargs['top_k'] == 50
        assert call_kwargs['remote_only'] is True
        assert call_kwargs['show_hidden'] is True

    def test_get_matches_uses_policy_defaults(self, client, mock_match_service, mock_policy_service):
        """Test get matches uses policy defaults when not specified."""
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = []
        mock_match_service.return_value = mock_service_instance

        client.get('/api/matches')

        call_kwargs = mock_service_instance.get_matches.call_args[1]
        assert call_kwargs['min_fit'] == 50.0  # From policy
        assert call_kwargs['top_k'] == 100  # From policy

    def test_get_matches_invalid_status(self, client):
        """Test get matches with invalid status parameter."""
        response = client.get('/api/matches', params={'status': 'invalid_status'})

        assert response.status_code == 422
        data = response.json()
        assert 'detail' in data
        assert 'Invalid status' in data['detail']

    def test_get_matches_valid_statuses(self, client, mock_match_service, mock_policy_service):
        """Test get matches with all valid status values."""
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = []
        mock_match_service.return_value = mock_service_instance

        for status in ['active', 'stale', 'all']:
            response = client.get('/api/matches', params={'status': status})
            assert response.status_code == 200

    def test_get_matches_min_fit_bounds(self, client, mock_match_service, mock_policy_service):
        """Test get matches with min_fit boundary values."""
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = []
        mock_match_service.return_value = mock_service_instance

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
        mock_service_instance = Mock()
        mock_service_instance.get_matches.return_value = []
        mock_match_service.return_value = mock_service_instance

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

    def test_get_match_details_success(self, client, mock_match_service):
        """Test successful get match details."""
        mock_service_instance = Mock()
        mock_service_instance.get_match_detail.return_value = {
            'match_id': str(uuid.uuid4()),
            'title': 'Software Engineer',
            'company': 'Tech Corp',
            'overall_score': 85.5,
            'job_description': 'We are looking for...',
            'requirement_matches': []
        }
        mock_match_service.return_value = mock_service_instance

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}')

        assert response.status_code == 200
        data = response.json()
        assert data['match_id'] == match_id
        mock_service_instance.get_match_detail.assert_called_once_with(match_id)

    def test_get_match_details_invalid_uuid(self, client):
        """Test get match details with invalid UUID."""
        response = client.get('/api/matches/not-a-uuid')

        assert response.status_code == 400
        data = response.json()
        assert 'Invalid match_id format' in data['detail']

    def test_get_match_details_not_found(self, client, mock_match_service):
        """Test get match details when match not found."""
        mock_service_instance = Mock()
        mock_service_instance.get_match_detail.side_effect = HTTPException(
            status_code=404, detail="Match not found"
        )
        mock_match_service.return_value = mock_service_instance

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}')

        assert response.status_code == 404

    def test_toggle_match_hidden_success(self, client, mock_match_service):
        """Test successful toggle match hidden."""
        mock_service_instance = Mock()
        mock_service_instance.toggle_hidden.return_value = True
        mock_match_service.return_value = mock_service_instance

        match_id = str(uuid.uuid4())
        response = client.post(f'/api/matches/{match_id}/hide')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['match_id'] == match_id
        assert data['is_hidden'] is True
        mock_service_instance.toggle_hidden.assert_called_once_with(match_id)

    def test_toggle_match_hidden_unhide(self, client, mock_match_service):
        """Test toggle match hidden to unhide."""
        mock_service_instance = Mock()
        mock_service_instance.toggle_hidden.return_value = False
        mock_match_service.return_value = mock_service_instance

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

    def test_get_match_explanation_success(self, client, mock_match_service):
        """Test successful get match explanation."""
        mock_service_instance = Mock()
        mock_service_instance.get_match_explanation.return_value = {
            'match_id': str(uuid.uuid4()),
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
        mock_match_service.return_value = mock_service_instance

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}/explanation')

        assert response.status_code == 200
        data = response.json()
        assert 'per_requirement' in data
        assert 'section_summary' in data
        assert 'strengths' in data
        assert 'gaps' in data
        mock_service_instance.get_match_explanation.assert_called_once_with(match_id)

    def test_get_match_explanation_invalid_uuid(self, client):
        """Test get match explanation with invalid UUID."""
        response = client.get('/api/matches/not-a-uuid/explanation')

        assert response.status_code == 400
        assert 'Invalid match_id format' in response.json()['detail']

    def test_get_match_explanation_not_found(self, client, mock_match_service):
        """Test get match explanation when match not found."""
        mock_service_instance = Mock()
        mock_service_instance.get_match_explanation.side_effect = HTTPException(
            status_code=404, detail="Match not found"
        )
        mock_match_service.return_value = mock_service_instance

        match_id = str(uuid.uuid4())
        response = client.get(f'/api/matches/{match_id}/explanation')

        assert response.status_code == 404


class TestMatchesRouterIntegration:
    """Integration tests for matches router with mocked dependencies."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with matches router."""
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

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

                # Setup match service
                mock_match_service = Mock()
                mock_match_service.get_matches.return_value = [
                    {
                        'match_id': str(uuid.uuid4()),
                        'title': 'Senior Software Engineer',
                        'company': 'Google',
                        'location': 'Mountain View, CA',
                        'is_remote': False,
                        'overall_score': 92.5,
                        'fit_score': 90.0,
                        'want_score': 85.0,
                        'required_coverage': 0.95,
                        'preferences_coverage': 0.80,
                        'match_type': 'requirements_only',
                        'is_hidden': False
                    }
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
                assert match['overall_score'] == 92.5

    def test_full_match_details_flow(self, client):
        """Test complete flow of getting match details."""
        with patch('web.backend.routers.matches.MatchService') as MockMatchService:
            mock_match_service = Mock()
            mock_match_service.get_match_detail.return_value = {
                'match_id': str(uuid.uuid4()),
                'job': {
                    'id': 'job-123',
                    'title': 'Software Engineer',
                    'company': 'Tech Corp',
                    'description': 'Job description here...',
                    'requirements': ['Python', 'SQL', 'AWS']
                },
                'overall_score': 85.5,
                'fit_score': 82.0,
                'want_score': 78.0,
                'requirement_matches': [
                    {
                        'requirement': 'Python',
                        'evidence': '5 years Python experience',
                        'similarity': 0.95
                    }
                ]
            }
            MockMatchService.return_value = mock_match_service

            match_id = str(uuid.uuid4())
            response = client.get(f'/api/matches/{match_id}')

            assert response.status_code == 200
            data = response.json()
            assert data['match_id'] == match_id
            assert data['job']['title'] == 'Software Engineer'
            assert data['overall_score'] == 85.5

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
                'match_id': str(uuid.uuid4()),
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
            MockMatchService.return_value = mock_match_service

            match_id = str(uuid.uuid4())
            response = client.get(f'/api/matches/{match_id}/explanation')

            assert response.status_code == 200
            data = response.json()
            assert len(data['per_requirement']) == 2
            assert len(data['strengths']) == 2
            assert len(data['gaps']) == 1
            assert 'section_summary' in data
