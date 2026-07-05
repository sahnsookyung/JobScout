#!/usr/bin/env python3
"""
Tests for Policy Router
Covers: web/backend/routers/policy.py
"""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_db
from web.backend.routers import policy as policy_router
from web.backend.routers.policy import router

def _llm_policy(
    *,
    enabled=False,
    auto_enqueue_enabled=False,
    top_n=5,
    top_n_max=10,
    available=False,
    revision=0,
):
    return SimpleNamespace(
        enabled=enabled,
        auto_enqueue_enabled=auto_enqueue_enabled,
        top_n=top_n,
        top_n_max=top_n_max,
        available=available,
        revision=revision,
    )


class TestPolicyRouter:
    """Test policy router endpoints."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with policy router."""
        app = FastAPI()
        test_db = Mock()

        def override_get_db():
            yield test_db

        app.dependency_overrides[get_db] = override_get_db
        app.state.test_db = test_db
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def mock_policy_service(self):
        """Create mock policy service."""
        with patch('web.backend.routers.policy.get_policy_service') as mock:
            policy_service = Mock()
            policy_service.get_llm_judge_policy.return_value = _llm_policy()
            policy_service.update_llm_judge_policy.return_value = _llm_policy()
            mock.return_value = policy_service
            yield policy_service

    def test_get_policy_success(self, client, mock_policy_service):
        """Test successful get policy."""
        mock_policy = Mock()
        mock_policy.min_fit = 55.0
        mock_policy.top_k = 50
        mock_policy.min_jd_required_coverage = 0.6

        mock_policy_service.get_current_policy.return_value = mock_policy

        response = client.get('/api/v1/policy')

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 55.0
        assert data['top_k'] == 50
        assert data['min_jd_required_coverage'] == 0.6
        assert data['llm_judge_enabled'] is False
        assert data['llm_judge_auto_enqueue_enabled'] is False
        assert data['llm_judge_top_n'] == 5
        assert data['llm_judge_top_n_max'] == 10
        mock_policy_service.get_current_policy.assert_called_once()

    def test_get_policy_with_null_coverage(self, client, mock_policy_service):
        """Test get policy with null min_jd_required_coverage."""
        mock_policy = Mock()
        mock_policy.min_fit = 40.0
        mock_policy.top_k = 100
        mock_policy.min_jd_required_coverage = None

        mock_policy_service.get_current_policy.return_value = mock_policy

        response = client.get('/api/v1/policy')

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 40.0
        assert data['top_k'] == 100
        assert data['min_jd_required_coverage'] is None

    def test_update_policy_success(self, client, mock_policy_service):
        """Test successful policy update."""
        mock_updated_policy = Mock()
        mock_updated_policy.min_fit = 70.0
        mock_updated_policy.top_k = 25
        mock_updated_policy.min_jd_required_coverage = 0.8

        mock_policy_service.update_policy.return_value = mock_updated_policy

        response = client.put(
            '/api/v1/policy',
            json={
                'min_fit': 70.0,
                'top_k': 25,
                'min_jd_required_coverage': 0.8
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 70.0
        assert data['top_k'] == 25
        assert data['min_jd_required_coverage'] == 0.8

        mock_policy_service.update_policy.assert_called_once_with(
            min_fit=70.0,
            top_k=25,
            min_jd_required_coverage=0.8
        )
        mock_policy_service.update_llm_judge_policy.assert_called_once()
        assert mock_policy_service.update_llm_judge_policy.call_args.kwargs["enabled"] is None
        assert (
            mock_policy_service.update_llm_judge_policy.call_args.kwargs[
                "auto_enqueue_enabled"
            ]
            is None
        )
        assert mock_policy_service.update_llm_judge_policy.call_args.kwargs["top_n"] is None

    def test_update_policy_updates_llm_judge_fields(self, client, mock_policy_service):
        """Test updating owner-scoped LLM judge controls."""
        default_policy = Mock(min_fit=50.0, top_k=100, min_jd_required_coverage=None)
        mock_policy_service.get_current_policy.return_value = default_policy
        mock_policy_service.get_llm_judge_policy.return_value = _llm_policy(
            enabled=False,
            auto_enqueue_enabled=False,
            top_n=5,
            top_n_max=8,
            available=True,
            revision=1,
        )
        mock_policy_service.update_policy.return_value = default_policy
        mock_policy_service.update_llm_judge_policy.return_value = _llm_policy(
            enabled=True,
            auto_enqueue_enabled=True,
            top_n=3,
            top_n_max=8,
            available=True,
            revision=2,
        )

        with patch(
            'web.backend.routers.policy._enqueue_llm_top_n_after_policy_update',
            return_value=(
                {"attempted": 3, "reused": 1, "created": 2, "enqueued": 2, "failed": 0},
                [],
            ),
        ) as enqueue:
            response = client.put(
                '/api/v1/policy',
                json={
                    'llm_judge_enabled': True,
                    'llm_judge_auto_enqueue_enabled': True,
                    'llm_judge_top_n': 3,
                }
            )

        assert response.status_code == 200
        data = response.json()
        assert data['llm_judge_enabled'] is True
        assert data['llm_judge_auto_enqueue_enabled'] is True
        assert data['llm_judge_top_n'] == 3
        assert data['llm_judge_top_n_max'] == 8
        assert data['llm_judge_available'] is True
        assert data['llm_judge_revision'] == 2
        assert data['llm_judge_enqueue_stats']['enqueued'] == 2
        mock_policy_service.update_llm_judge_policy.assert_called_once()
        assert mock_policy_service.update_llm_judge_policy.call_args.kwargs["enabled"] is True
        assert (
            mock_policy_service.update_llm_judge_policy.call_args.kwargs[
                "auto_enqueue_enabled"
            ]
            is True
        )
        assert mock_policy_service.update_llm_judge_policy.call_args.kwargs["top_n"] == 3
        enqueue.assert_called_once()

    def test_llm_policy_enqueue_helper_queues_increased_top_n(self):
        previous = _llm_policy(enabled=True, top_n=2, available=True, revision=1)
        next_policy = _llm_policy(
            enabled=True,
            auto_enqueue_enabled=True,
            top_n=4,
            available=True,
            revision=2,
        )
        db = Mock()

        with patch('web.backend.routers.policy.MatchService') as match_service_cls:
            with patch('core.llm_evaluation.MatchLlmEvaluationService') as llm_service_cls:
                match_service_cls.return_value._resolve_canonical_selection.return_value = (
                    SimpleNamespace(selection_run_id='selection-run-1')
                )
                llm_service_cls.return_value.evaluate_selection_run.return_value = {
                    "attempted": 4,
                    "reused": 2,
                    "created": 2,
                    "enqueued": 2,
                    "failed": 0,
                }

                stats, degraded = policy_router._enqueue_llm_top_n_after_policy_update(
                    db,
                    owner_id='owner-1',
                    tenant_id='tenant-1',
                    previous_policy=previous,
                    next_policy=next_policy,
                )

        assert degraded == []
        assert stats["enqueued"] == 2
        llm_service_cls.return_value.evaluate_selection_run.assert_called_once_with(
            'selection-run-1',
            owner_id='owner-1',
            tenant_id='tenant-1',
            top_n=4,
        )

    def test_llm_policy_enqueue_helper_skips_when_auto_enqueue_disabled(self):
        previous = _llm_policy(enabled=False, top_n=2, available=True, revision=1)
        next_policy = _llm_policy(enabled=True, top_n=4, available=True, revision=2)

        with patch('web.backend.routers.policy.MatchService') as match_service_cls:
            stats, degraded = policy_router._enqueue_llm_top_n_after_policy_update(
                Mock(),
                owner_id='owner-1',
                tenant_id='tenant-1',
                previous_policy=previous,
                next_policy=next_policy,
            )

        assert stats == {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}
        assert degraded == []
        match_service_cls.assert_not_called()

    def test_llm_policy_enqueue_helper_skips_decreased_top_n(self):
        previous = _llm_policy(enabled=True, top_n=5, available=True, revision=1)
        next_policy = _llm_policy(enabled=True, top_n=3, available=True, revision=2)

        with patch('web.backend.routers.policy.MatchService') as match_service_cls:
            stats, degraded = policy_router._enqueue_llm_top_n_after_policy_update(
                Mock(),
                owner_id='owner-1',
                tenant_id=None,
                previous_policy=previous,
                next_policy=next_policy,
            )

        assert stats == {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}
        assert degraded == []
        match_service_cls.assert_not_called()

    def test_update_policy_partial(self, client, mock_policy_service):
        """Test policy update with partial fields."""
        default_policy = Mock(min_fit=50.0, top_k=100, min_jd_required_coverage=None)
        mock_policy_service.get_current_policy.return_value = default_policy

        mock_updated_policy = Mock()
        mock_updated_policy.min_fit = 60.0
        mock_updated_policy.top_k = 50
        mock_updated_policy.min_jd_required_coverage = None

        mock_policy_service.update_policy.return_value = mock_updated_policy

        response = client.put(
            '/api/v1/policy',
            json={'min_fit': 60.0}
        )

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 60.0

        mock_policy_service.update_policy.assert_called_once_with(
            min_fit=60.0,
            top_k=100,
            min_jd_required_coverage=None
        )

    def test_update_policy_null_coverage(self, client, mock_policy_service):
        """Test policy update with null min_jd_required_coverage."""
        default_policy = Mock(min_fit=50.0, top_k=100, min_jd_required_coverage=0.7)
        mock_policy_service.get_current_policy.return_value = default_policy

        mock_updated_policy = Mock()
        mock_updated_policy.min_fit = 50.0
        mock_updated_policy.top_k = 100
        mock_updated_policy.min_jd_required_coverage = None

        mock_policy_service.update_policy.return_value = mock_updated_policy

        response = client.put(
            '/api/v1/policy',
            json={'min_jd_required_coverage': None}
        )

        assert response.status_code == 200
        data = response.json()
        assert data['min_jd_required_coverage'] is None
        mock_policy_service.update_policy.assert_called_once_with(
            min_fit=50.0,
            top_k=100,
            min_jd_required_coverage=None,
        )

    def test_apply_preset_strict(self, client, mock_policy_service):
        """Test applying strict preset."""
        mock_policy = Mock()
        mock_policy.min_fit = 70.0
        mock_policy.top_k = 25
        mock_policy.min_jd_required_coverage = 0.80

        mock_policy_service.apply_preset.return_value = mock_policy

        response = client.post('/api/v1/policy/preset/strict')

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 70.0
        assert data['top_k'] == 25
        assert data['min_jd_required_coverage'] == 0.80

        mock_policy_service.apply_preset.assert_called_once_with('strict')

    def test_apply_preset_balanced(self, client, mock_policy_service):
        """Test applying balanced preset."""
        mock_policy = Mock()
        mock_policy.min_fit = 55.0
        mock_policy.top_k = 50
        mock_policy.min_jd_required_coverage = 0.60

        mock_policy_service.apply_preset.return_value = mock_policy

        response = client.post('/api/v1/policy/preset/balanced')

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 55.0
        assert data['top_k'] == 50
        assert data['min_jd_required_coverage'] == 0.60

        mock_policy_service.apply_preset.assert_called_once_with('balanced')

    def test_apply_preset_discovery(self, client, mock_policy_service):
        """Test applying discovery preset."""
        mock_policy = Mock()
        mock_policy.min_fit = 40.0
        mock_policy.top_k = 100
        mock_policy.min_jd_required_coverage = None

        mock_policy_service.apply_preset.return_value = mock_policy

        response = client.post('/api/v1/policy/preset/discovery')

        assert response.status_code == 200
        data = response.json()
        assert data['min_fit'] == 40.0
        assert data['top_k'] == 100
        assert data['min_jd_required_coverage'] is None

        mock_policy_service.apply_preset.assert_called_once_with('discovery')

    def test_apply_preset_case_insensitive(self, client, mock_policy_service):
        """Test preset name is case insensitive."""
        mock_policy = Mock()
        mock_policy.min_fit = 70.0
        mock_policy.top_k = 25
        mock_policy.min_jd_required_coverage = 0.80

        mock_policy_service.apply_preset.return_value = mock_policy

        response = client.post('/api/v1/policy/preset/STRICT')

        assert response.status_code == 200
        mock_policy_service.apply_preset.assert_called_once_with('strict')

    def test_apply_preset_unknown(self, client):
        """Test applying unknown preset returns 400."""
        response = client.post('/api/v1/policy/preset/unknown_preset')

        assert response.status_code == 400
        data = response.json()
        assert 'Unknown preset' in data['detail']
        assert 'strict' in data['detail']
        assert 'balanced' in data['detail']
        assert 'discovery' in data['detail']

    def test_apply_preset_invalid_name(self, client):
        """Test applying preset with invalid name format."""
        response = client.post('/api/v1/policy/preset/')

        assert response.status_code == 404

    def test_get_scoring_weights_success(self, client):
        """Test successful get scoring weights."""
        response = client.get('/api/config/scoring-weights')

        assert response.status_code == 200
        assert response.json() == {'fit_score_source': 'fit-only'}

    def test_get_scoring_weights_default_values(self, client):
        """Test get scoring weights with default values."""
        response = client.get('/api/config/scoring-weights')
        assert response.status_code == 200
        assert response.json() == {'fit_score_source': 'fit-only'}


class TestPolicyRouterIntegration:
    """Integration tests for policy router."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with policy router."""
        app = FastAPI()
        test_db = Mock()

        def override_get_db():
            yield test_db

        app.dependency_overrides[get_db] = override_get_db
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    def test_full_policy_lifecycle(self, client):
        """Test complete policy lifecycle: get, update, apply preset."""
        with patch('web.backend.routers.policy.get_policy_service') as MockPolicyService:
            # Setup mock policy service
            mock_policy_service = Mock()
            mock_policy_service.get_llm_judge_policy.return_value = _llm_policy()
            mock_policy_service.update_llm_judge_policy.return_value = _llm_policy()

            # Initial policy
            initial_policy = Mock()
            initial_policy.min_fit = 50.0
            initial_policy.top_k = 100
            initial_policy.min_jd_required_coverage = None
            mock_policy_service.get_current_policy.return_value = initial_policy

            # Updated policy
            updated_policy = Mock()
            updated_policy.min_fit = 65.0
            updated_policy.top_k = 75
            updated_policy.min_jd_required_coverage = 0.7

            def update_side_effect(**kwargs):
                updated_policy.min_fit = kwargs.get('min_fit', updated_policy.min_fit)
                updated_policy.top_k = kwargs.get('top_k', updated_policy.top_k)
                updated_policy.min_jd_required_coverage = kwargs.get(
                    'min_jd_required_coverage', updated_policy.min_jd_required_coverage
                )
                return updated_policy

            mock_policy_service.update_policy.side_effect = update_side_effect
            mock_policy_service.apply_preset.return_value = Mock(
                min_fit=70.0, top_k=25, min_jd_required_coverage=0.80
            )

            MockPolicyService.return_value = mock_policy_service

            # 1. Get initial policy
            response1 = client.get('/api/v1/policy')
            assert response1.status_code == 200
            assert response1.json()['min_fit'] == 50.0

            # 2. Update policy
            response2 = client.put(
                '/api/v1/policy',
                json={'min_fit': 65.0, 'top_k': 75, 'min_jd_required_coverage': 0.7}
            )
            assert response2.status_code == 200
            assert response2.json()['min_fit'] == 65.0
            assert response2.json()['top_k'] == 75

            # 3. Apply strict preset
            response3 = client.post('/api/v1/policy/preset/strict')
            assert response3.status_code == 200
            assert response3.json()['min_fit'] == 70.0
            assert response3.json()['top_k'] == 25

            # 4. Get scoring weights
            response4 = client.get('/api/config/scoring-weights')
            assert response4.status_code == 200
            assert response4.json() == {'fit_score_source': 'fit-only'}

    def test_preset_workflow(self, client):
        """Test applying different presets in sequence."""
        with patch('web.backend.routers.policy.get_policy_service') as MockPolicyService:
            mock_policy_service = Mock()
            mock_policy_service.get_llm_judge_policy.return_value = _llm_policy()

            preset_policies = {
                'strict': Mock(min_fit=70.0, top_k=25, min_jd_required_coverage=0.80),
                'balanced': Mock(min_fit=55.0, top_k=50, min_jd_required_coverage=0.60),
                'discovery': Mock(min_fit=40.0, top_k=100, min_jd_required_coverage=None)
            }

            mock_policy_service.apply_preset.side_effect = lambda name: preset_policies[name]
            MockPolicyService.return_value = mock_policy_service

            # Apply each preset and verify
            for preset_name, expected_policy in preset_policies.items():
                response = client.post(f'/api/v1/policy/preset/{preset_name}')
                assert response.status_code == 200
                data = response.json()
                assert data['min_fit'] == expected_policy.min_fit
                assert data['top_k'] == expected_policy.top_k
                assert data['min_jd_required_coverage'] == expected_policy.min_jd_required_coverage

    def test_update_policy_validation(self, client):
        """Test policy update with various value combinations."""
        with patch('web.backend.routers.policy.get_policy_service') as MockPolicyService:
            mock_policy_service = Mock()
            mock_policy_service.update_llm_judge_policy.return_value = _llm_policy()

            default_policy = Mock(min_fit=50.0, top_k=100, min_jd_required_coverage=None)
            mock_policy_service.get_current_policy.return_value = default_policy

            mock_policy = Mock()
            mock_policy.min_fit = 70.0
            mock_policy.top_k = 25
            mock_policy.min_jd_required_coverage = 0.8
            mock_policy_service.update_policy.return_value = mock_policy

            MockPolicyService.return_value = mock_policy_service

            response = client.put('/api/v1/policy', json={'min_fit': 70.0, 'top_k': 25, 'min_jd_required_coverage': 0.8})
            assert response.status_code == 200
            data = response.json()
            assert data['min_fit'] == 70.0
            assert data['top_k'] == 25
            assert data['min_jd_required_coverage'] == 0.8

            # Test top_k boundary values
            for top_k in [1, 50, 250, 500]:
                mock_policy = Mock()
                mock_policy.min_fit = 50.0
                mock_policy.top_k = top_k
                mock_policy.min_jd_required_coverage = None
                mock_policy_service.update_policy.return_value = mock_policy

                response = client.put('/api/v1/policy', json={'top_k': top_k})
                assert response.status_code == 200
                assert response.json()['top_k'] == top_k

            # Test min_jd_required_coverage boundary values
            for coverage in [0.0, 0.5, 1.0, None]:
                mock_policy = Mock()
                mock_policy.min_fit = 50.0
                mock_policy.top_k = 100
                mock_policy.min_jd_required_coverage = coverage
                mock_policy_service.update_policy.return_value = mock_policy

                response = client.put(
                    '/api/v1/policy',
                    json={'min_jd_required_coverage': coverage}
                )
                assert response.status_code == 200
                assert response.json()['min_jd_required_coverage'] == coverage
