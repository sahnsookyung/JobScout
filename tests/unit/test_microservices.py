import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import json


class TestRedisStreamsModule:
    
    @patch('core.redis_streams.get_redis_client')
    def test_enqueue_job(self, mock_get_client):
        mock_client = Mock()
        mock_client.xadd.return_value = "1234567890-0"
        mock_get_client.return_value = mock_client

        from core.redis_streams import enqueue_job

        result = enqueue_job("test:jobs", {"task_id": "test-123", "data": "value"})

        assert result == "1234567890-0"
        # All values are now JSON-encoded for type preservation
        mock_client.xadd.assert_called_once_with("test:jobs", {"task_id": '"test-123"', "data": '"value"'})

    @patch('core.redis_streams.get_redis_client')
    def test_ack_message(self, mock_get_client):
        mock_client = Mock()
        mock_client.xack.return_value = 1  # Redis xack returns count of acknowledged messages
        mock_get_client.return_value = mock_client

        from core.redis_streams import ack_message

        result = ack_message("test:jobs", "group-1", "1234567890-0")

        assert result == 1
        mock_client.xack.assert_called_once_with("test:jobs", "group-1", "1234567890-0")

    @patch('core.redis_streams.get_redis_client')
    def test_publish_completion(self, mock_get_client):
        mock_client = Mock()
        mock_client.publish.return_value = 1
        mock_get_client.return_value = mock_client
        
        from core.redis_streams import publish_completion
        
        result = publish_completion("test:completed", {"task_id": "test-123", "status": "completed"})
        
        assert result == 1
        mock_client.publish.assert_called_once()

    @patch('core.redis_streams.get_redis_client')
    def test_get_task_state_returns_none_when_not_found(self, mock_get_client):
        mock_client = Mock()
        mock_client.get.return_value = None
        mock_get_client.return_value = mock_client
        
        from core.redis_streams import get_task_state
        
        result = get_task_state("nonexistent-task")
        
        assert result is None

    @patch('core.redis_streams.get_redis_client')
    def test_get_task_state_returns_parsed_json(self, mock_get_client):
        mock_client = Mock()
        mock_client.get.return_value = '{"status": "completed", "matches_count": 5}'
        mock_get_client.return_value = mock_client
        
        from core.redis_streams import get_task_state
        
        result = get_task_state("test-task")
        
        assert result == {"status": "completed", "matches_count": 5}

    @patch('core.redis_streams.get_redis_client')
    def test_set_task_state(self, mock_get_client):
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        from core.redis_streams import set_task_state
        
        set_task_state("test-task", {"status": "running"}, ttl=600)
        
        mock_client.setex.assert_called_once()


class TestServiceClient:
    
    @patch('web.backend.services.clients.httpx.Client')
    def test_client_get_request(self, mock_httpx_client):
        mock_response = Mock()
        mock_response.json.return_value = {"status": "ok"}
        mock_response.raise_for_status = Mock()
        
        mock_client_instance = Mock()
        mock_client_instance.request.return_value = mock_response
        mock_httpx_client.return_value.__enter__ = Mock(return_value=mock_client_instance)
        mock_httpx_client.return_value.__exit__ = Mock(return_value=False)
        
        from web.backend.services.clients import ServiceClient
        
        client = ServiceClient("http://localhost:8084")
        result = client.get("/health")
        
        assert result == {"status": "ok"}
        mock_client_instance.request.assert_called_once_with("GET", "http://localhost:8084/health")

    @patch('web.backend.services.clients.httpx.Client')
    def test_client_post_request(self, mock_httpx_client):
        mock_response = Mock()
        mock_response.json.return_value = {"task_id": "123", "success": True}
        mock_response.raise_for_status = Mock()
        
        mock_client_instance = Mock()
        mock_client_instance.request.return_value = mock_response
        mock_httpx_client.return_value.__enter__ = Mock(return_value=mock_client_instance)
        mock_httpx_client.return_value.__exit__ = Mock(return_value=False)
        
        from web.backend.services.clients import ServiceClient
        
        client = ServiceClient("http://localhost:8084")
        result = client.post("/orchestrate/match", json={})
        
        assert result == {"task_id": "123", "success": True}


class TestOrchestratorClient:
    
    @patch('web.backend.services.clients.ServiceClient.post')
    def test_start_matching_calls_orchestrator(self, mock_post):
        mock_post.return_value = {"success": True, "task_id": "match-abc123", "message": "started"}
        
        from web.backend.services.clients import OrchestratorClient
        
        client = OrchestratorClient()
        result = client.start_matching()
        
        assert result == {"success": True, "task_id": "match-abc123", "message": "started"}
        mock_post.assert_called_once_with("/orchestrate/match", json={})


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
