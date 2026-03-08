#!/usr/bin/env python3
"""
Tests for Service Clients
Covers: web/backend/services/clients.py
"""

import pytest
import httpx
from unittest.mock import Mock, patch, MagicMock
from urllib.parse import urlparse

from web.backend.services.clients import (
    ServiceClient,
    ExtractionClient,
    EmbeddingsClient,
    ScorerMatcherClient,
    OrchestratorClient,
    _validate_url,
    get_extraction_client,
    get_embeddings_client,
    get_scorer_matcher_client,
    get_orchestrator_client,
    HEALTH_ENDPOINT,
)


class TestValidateUrl:
    """Test _validate_url function."""

    def test_valid_http_url(self, caplog):
        """Test validation of valid HTTP URL."""
        url = "http://localhost:8080"
        result = _validate_url(url, "TEST_URL")

        assert result == url
        assert "not configured" not in caplog.text

    def test_valid_https_url(self, caplog):
        """Test validation of valid HTTPS URL."""
        url = "https://api.example.com"
        result = _validate_url(url, "TEST_URL")

        assert result == url

    def test_empty_url_logs_warning(self, caplog):
        """Test that empty URL logs warning but returns empty string."""
        result = _validate_url("", "TEST_URL")

        assert result == ""
        assert "not configured" in caplog.text

    def test_malformed_url_raises(self):
        """Test that malformed URL raises RuntimeError."""
        with pytest.raises(RuntimeError) as exc_info:
            _validate_url("not-a-url", "TEST_URL")

        assert "must be a valid HTTP/HTTPS URL" in str(exc_info.value)

    def test_url_without_scheme_raises(self):
        """Test that URL without scheme raises RuntimeError."""
        with pytest.raises(RuntimeError) as exc_info:
            _validate_url("localhost:8080", "TEST_URL")

        assert "must be a valid HTTP/HTTPS URL" in str(exc_info.value)

    def test_url_with_invalid_scheme_raises(self):
        """Test that URL with invalid scheme raises RuntimeError."""
        with pytest.raises(RuntimeError) as exc_info:
            _validate_url("ftp://localhost:8080", "TEST_URL")

        assert "must be a valid HTTP/HTTPS URL" in str(exc_info.value)

    def test_url_without_netloc_raises(self):
        """Test that URL without netloc raises RuntimeError."""
        with pytest.raises(RuntimeError) as exc_info:
            _validate_url("http://", "TEST_URL")

        assert "must be a valid HTTP/HTTPS URL" in str(exc_info.value)


class TestServiceClient:
    """Test ServiceClient base class."""

    def test_init_with_url(self):
        """Test initialization with URL."""
        client = ServiceClient("http://localhost:8080", timeout=60)

        assert client.base_url == "http://localhost:8080"
        assert client.timeout == 60

    def test_init_with_env_var_validates(self):
        """Test initialization with env_var validates URL."""
        with patch('web.backend.services.clients._validate_url') as mock_validate:
            mock_validate.return_value = "http://validated:8080"

            client = ServiceClient("http://localhost:8080", env_var="TEST_URL")

            mock_validate.assert_called_once_with("http://localhost:8080", "TEST_URL")
            assert client.base_url == "http://validated:8080"

    def test_init_without_env_var_does_not_validate(self):
        """Test initialization without env_var skips validation."""
        with patch('web.backend.services.clients._validate_url') as mock_validate:
            client = ServiceClient("http://localhost:8080")

            mock_validate.assert_not_called()
            assert client.base_url == "http://localhost:8080"

    def test_request_get_success(self):
        """Test successful GET request."""
        client = ServiceClient("http://localhost:8080")

        mock_response = Mock()
        mock_response.json.return_value = {"status": "ok"}

        with patch('httpx.Client') as mock_client_class:
            mock_client_instance = Mock()
            mock_client_instance.request.return_value = mock_response
            mock_client_class.return_value.__enter__.return_value = mock_client_instance

            result = client._request("GET", "/health")

            assert result == {"status": "ok"}
            mock_client_instance.request.assert_called_once_with(
                "GET", "http://localhost:8080/health", timeout=30
            )

    def test_request_post_success(self):
        """Test successful POST request."""
        client = ServiceClient("http://localhost:8080")

        mock_response = Mock()
        mock_response.json.return_value = {"success": True}

        with patch('httpx.Client') as mock_client_class:
            mock_client_instance = Mock()
            mock_client_instance.request.return_value = mock_response
            mock_client_class.return_value.__enter__.return_value = mock_client_instance

            result = client._request(
                "POST", "/api/test", json={"data": "value"}
            )

            assert result == {"success": True}
            mock_client_instance.request.assert_called_once_with(
                "POST", "http://localhost:8080/api/test", json={"data": "value"}, timeout=30
            )

    def test_request_no_base_url_raises(self):
        """Test request without base URL raises RuntimeError."""
        client = ServiceClient("")

        with pytest.raises(RuntimeError) as exc_info:
            client._request("GET", "/health")

        assert "base URL is empty" in str(exc_info.value)

    def test_request_http_status_error(self, caplog):
        """Test request with HTTP status error."""
        client = ServiceClient("http://localhost:8080")

        with patch('httpx.Client') as mock_client_class:
            mock_client_instance = Mock()
            mock_error = httpx.HTTPStatusError(
                "Not Found",
                request=Mock(),
                response=Mock(status_code=404)
            )
            mock_client_instance.request.side_effect = mock_error
            mock_client_class.return_value.__enter__.return_value = mock_client_instance

            with pytest.raises(httpx.HTTPStatusError):
                client._request("GET", "/not-found")

            assert "Service returned error" in caplog.text

    def test_request_connection_error(self, caplog):
        """Test request with connection error."""
        client = ServiceClient("http://localhost:8080")

        with patch('httpx.Client') as mock_client_class:
            mock_client_instance = Mock()
            mock_error = httpx.RequestError("Connection refused", request=Mock())
            mock_client_instance.request.side_effect = mock_error
            mock_client_class.return_value.__enter__.return_value = mock_client_instance

            with pytest.raises(httpx.RequestError):
                client._request("GET", "/health")

            assert "Service call failed" in caplog.text

    def test_request_invalid_json(self, caplog):
        """Test request with invalid JSON response."""
        client = ServiceClient("http://localhost:8080")

        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")

        with patch('httpx.Client') as mock_client_class:
            mock_client_instance = Mock()
            mock_client_instance.request.return_value = mock_response
            mock_client_class.return_value.__enter__.return_value = mock_client_instance

            with pytest.raises(ValueError):
                client._request("GET", "/health")

            assert "Invalid JSON response" in caplog.text

    def test_get_wrapper(self):
        """Test GET method wrapper."""
        client = ServiceClient("http://localhost:8080")

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {"data": "value"}

            result = client.get("/api/test", params={"key": "value"})

            mock_request.assert_called_once_with("GET", "/api/test", params={"key": "value"})
            assert result == {"data": "value"}

    def test_post_wrapper(self):
        """Test POST method wrapper."""
        client = ServiceClient("http://localhost:8080")

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {"success": True}

            result = client.post("/api/test", json={"data": "value"})

            mock_request.assert_called_once_with("POST", "/api/test", json={"data": "value"})
            assert result == {"success": True}


class TestExtractionClient:
    """Test ExtractionClient."""

    def test_init_default_url(self):
        """Test initialization with default URL."""
        with patch('web.backend.services.clients.EXTRACTION_URL', ""):
            client = ExtractionClient()
            assert client.base_url == ""

    def test_init_custom_url(self):
        """Test initialization with custom URL."""
        client = ExtractionClient("http://extraction:8081")
        assert client.base_url == "http://extraction:8081"

    def test_extract_jobs(self):
        """Test extract_jobs method."""
        client = ExtractionClient("http://extraction:8081")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "jobs_extracted": 100}

            result = client.extract_jobs(limit=100)

            mock_post.assert_called_once_with("/extract/jobs", json={"limit": 100})
            assert result == {"success": True, "jobs_extracted": 100}

    def test_extract_resume(self):
        """Test extract_resume method."""
        client = ExtractionClient("http://extraction:8081")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "resume_id": "123"}

            result = client.extract_resume("/path/to/resume.pdf")

            mock_post.assert_called_once_with(
                "/extract/resume", json={"resume_file": "/path/to/resume.pdf"}
            )
            assert result == {"success": True, "resume_id": "123"}

    def test_health(self):
        """Test health method."""
        client = ExtractionClient("http://extraction:8081")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "healthy"}

            result = client.health()

            mock_get.assert_called_once_with(HEALTH_ENDPOINT)
            assert result == {"status": "healthy"}


class TestEmbeddingsClient:
    """Test EmbeddingsClient."""

    def test_init_default_url(self):
        """Test initialization with default URL."""
        with patch('web.backend.services.clients.EMBEDDINGS_URL', ""):
            client = EmbeddingsClient()
            assert client.base_url == ""

    def test_init_custom_url(self):
        """Test initialization with custom URL."""
        client = EmbeddingsClient("http://embeddings:8082")
        assert client.base_url == "http://embeddings:8082"

    def test_embed_jobs(self):
        """Test embed_jobs method."""
        client = EmbeddingsClient("http://embeddings:8082")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "jobs_embedded": 50}

            result = client.embed_jobs(limit=50)

            mock_post.assert_called_once_with("/embed/jobs", json={"limit": 50})
            assert result == {"success": True, "jobs_embedded": 50}

    def test_embed_resume(self):
        """Test embed_resume method."""
        client = EmbeddingsClient("http://embeddings:8082")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "fingerprint": "abc123"}

            result = client.embed_resume("resume-fingerprint-123")

            mock_post.assert_called_once_with(
                "/embed/resume", json={"resume_fingerprint": "resume-fingerprint-123"}
            )
            assert result == {"success": True, "fingerprint": "abc123"}

    def test_health(self):
        """Test health method."""
        client = EmbeddingsClient("http://embeddings:8082")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "healthy"}

            result = client.health()

            mock_get.assert_called_once_with(HEALTH_ENDPOINT)
            assert result == {"status": "healthy"}


class TestScorerMatcherClient:
    """Test ScorerMatcherClient."""

    def test_init_default_url(self):
        """Test initialization with default URL."""
        with patch('web.backend.services.clients.SCORER_MATCHER_URL', ""):
            client = ScorerMatcherClient()
            assert client.base_url == ""

    def test_init_custom_url(self):
        """Test initialization with custom URL."""
        client = ScorerMatcherClient("http://matcher:8083")
        assert client.base_url == "http://matcher:8083"

    def test_match_resume(self):
        """Test match_resume method."""
        client = ScorerMatcherClient("http://matcher:8083")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "matches": 10}

            result = client.match_resume("resume-fingerprint-123")

            mock_post.assert_called_once_with(
                "/match/resume", json={"resume_fingerprint": "resume-fingerprint-123"}
            )
            assert result == {"success": True, "matches": 10}

    def test_match_jobs(self):
        """Test match_jobs method."""
        client = ScorerMatcherClient("http://matcher:8083")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "matches": 5}

            result = client.match_jobs(["job-1", "job-2", "job-3"])

            mock_post.assert_called_once_with(
                "/match/jobs", json={"job_ids": ["job-1", "job-2", "job-3"]}
            )
            assert result == {"success": True, "matches": 5}

    def test_health(self):
        """Test health method."""
        client = ScorerMatcherClient("http://matcher:8083")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "healthy"}

            result = client.health()

            mock_get.assert_called_once_with(HEALTH_ENDPOINT)
            assert result == {"status": "healthy"}


class TestOrchestratorClient:
    """Test OrchestratorClient."""

    def test_init_default_url(self):
        """Test initialization with default URL."""
        with patch('web.backend.services.clients.ORCHESTRATOR_URL', ""):
            client = OrchestratorClient()
            assert client.base_url == ""

    def test_init_custom_url(self):
        """Test initialization with custom URL."""
        client = OrchestratorClient("http://orchestrator:8084")
        assert client.base_url == "http://orchestrator:8084"

    def test_start_matching(self):
        """Test start_matching method."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "task_id": "match-abc123"}

            result = client.start_matching()

            mock_post.assert_called_once_with("/orchestrate/match", json={})
            assert result == {"success": True, "task_id": "match-abc123"}

    def test_get_task_status(self):
        """Test get_task_status method."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {
                "task_id": "match-abc123",
                "status": "running",
                "step": "matching"
            }

            result = client.get_task_status("match-abc123")

            mock_get.assert_called_once_with("/orchestrate/status/match-abc123")
            assert result["status"] == "running"

    def test_get_active_task(self):
        """Test get_active_task method."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {
                "task_id": "match-abc123",
                "status": "running"
            }

            result = client.get_active_task()

            mock_get.assert_called_once_with("/orchestrate/active")
            assert result["task_id"] == "match-abc123"

    def test_stop_task(self):
        """Test stop_task method."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'post') as mock_post:
            mock_post.return_value = {"success": True, "message": "Task stopped"}

            result = client.stop_task()

            mock_post.assert_called_once_with("/orchestrate/stop", json={})
            assert result == {"success": True, "message": "Task stopped"}

    def test_wait_for_completion_success(self):
        """Test wait_for_completion with successful completion."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "completed", "result": "success"}

            result = client.wait_for_completion("match-abc123", timeout=1.0, poll_interval=0.1)

            mock_get.assert_called_once_with("/orchestrate/status/match-abc123")
            assert result["success"] is True
            assert result["status"] == "completed"

    def test_wait_for_completion_failed(self):
        """Test wait_for_completion with failed task."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "failed", "error": "Pipeline error"}

            result = client.wait_for_completion("match-abc123", timeout=1.0, poll_interval=0.1)

            assert result["success"] is True
            assert result["status"] == "failed"

    def test_wait_for_completion_cancelled(self):
        """Test wait_for_completion with cancelled task."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "cancelled"}

            result = client.wait_for_completion("match-abc123", timeout=1.0, poll_interval=0.1)

            assert result["success"] is True
            assert result["status"] == "cancelled"

    def test_wait_for_completion_timeout(self):
        """Test wait_for_completion with timeout."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "running"}

            result = client.wait_for_completion("match-abc123", timeout=0.2, poll_interval=0.1)

            assert result["success"] is False
            assert result["status"] == "timeout"
            assert "Timeout" in result["error"]

    def test_wait_for_completion_http_404_continues(self, caplog):
        """Test wait_for_completion handles 404 by continuing."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.side_effect = httpx.HTTPStatusError(
                "Not Found",
                request=Mock(),
                response=Mock(status_code=404)
            )

            result = client.wait_for_completion("match-abc123", timeout=0.2, poll_interval=0.1)

            assert result["success"] is False
            assert result["status"] == "timeout"

    def test_wait_for_completion_http_error_logs_warning(self, caplog):
        """Test wait_for_completion logs warning for other HTTP errors."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.side_effect = httpx.HTTPStatusError(
                "Internal Error",
                request=Mock(),
                response=Mock(status_code=500)
            )

            result = client.wait_for_completion("match-abc123", timeout=0.2, poll_interval=0.1)

            assert "HTTP error polling" in caplog.text

    def test_wait_for_completion_request_error_logs_warning(self, caplog):
        """Test wait_for_completion logs warning for request errors."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.side_effect = httpx.RequestError(
                "Connection refused",
                request=Mock()
            )

            result = client.wait_for_completion("match-abc123", timeout=0.2, poll_interval=0.1)

            assert "Connection error polling" in caplog.text

    def test_health(self):
        """Test health method."""
        client = OrchestratorClient("http://orchestrator:8084")

        with patch.object(client, 'get') as mock_get:
            mock_get.return_value = {"status": "healthy"}

            result = client.health()

            mock_get.assert_called_once_with(HEALTH_ENDPOINT)
            assert result == {"status": "healthy"}


class TestSingletonFunctions:
    """Test singleton getter functions."""

    def setup_method(self):
        """Reset singletons before each test."""
        import web.backend.services.clients as clients_module
        clients_module._extraction_client = None
        clients_module._embeddings_client = None
        clients_module._scorer_matcher_client = None
        clients_module._orchestrator_client = None

    def test_get_extraction_client_creates_singleton(self):
        """Test get_extraction_client creates singleton instance."""
        with patch('web.backend.services.clients.EXTRACTION_URL', ""):
            client1 = get_extraction_client()
            client2 = get_extraction_client()

            assert client1 is client2
            assert isinstance(client1, ExtractionClient)

    def test_get_embeddings_client_creates_singleton(self):
        """Test get_embeddings_client creates singleton instance."""
        with patch('web.backend.services.clients.EMBEDDINGS_URL', ""):
            client1 = get_embeddings_client()
            client2 = get_embeddings_client()

            assert client1 is client2
            assert isinstance(client1, EmbeddingsClient)

    def test_get_scorer_matcher_client_creates_singleton(self):
        """Test get_scorer_matcher_client creates singleton instance."""
        with patch('web.backend.services.clients.SCORER_MATCHER_URL', ""):
            client1 = get_scorer_matcher_client()
            client2 = get_scorer_matcher_client()

            assert client1 is client2
            assert isinstance(client1, ScorerMatcherClient)

    def test_get_orchestrator_client_creates_singleton(self):
        """Test get_orchestrator_client creates singleton instance."""
        with patch('web.backend.services.clients.ORCHESTRATOR_URL', ""):
            client1 = get_orchestrator_client()
            client2 = get_orchestrator_client()

            assert client1 is client2
            assert isinstance(client1, OrchestratorClient)


class TestLazyLoading:
    """Test lazy loading via __getattr__."""

    def setup_method(self):
        """Reset singletons before each test."""
        import web.backend.services.clients as clients_module
        clients_module._extraction_client = None
        clients_module._embeddings_client = None
        clients_module._scorer_matcher_client = None
        clients_module._orchestrator_client = None

    def test_getattr_extraction_client(self):
        """Test lazy loading of extraction_client."""
        from web.backend.services import clients

        with patch('web.backend.services.clients.EXTRACTION_URL', ""):
            client = clients.extraction_client
            assert isinstance(client, ExtractionClient)

    def test_getattr_embeddings_client(self):
        """Test lazy loading of embeddings_client."""
        from web.backend.services import clients

        with patch('web.backend.services.clients.EMBEDDINGS_URL', ""):
            client = clients.embeddings_client
            assert isinstance(client, EmbeddingsClient)

    def test_getattr_scorer_matcher_client(self):
        """Test lazy loading of scorer_matcher_client."""
        from web.backend.services import clients

        with patch('web.backend.services.clients.SCORER_MATCHER_URL', ""):
            client = clients.scorer_matcher_client
            assert isinstance(client, ScorerMatcherClient)

    def test_getattr_orchestrator_client(self):
        """Test lazy loading of orchestrator_client."""
        from web.backend.services import clients

        with patch('web.backend.services.clients.ORCHESTRATOR_URL', ""):
            client = clients.orchestrator_client
            assert isinstance(client, OrchestratorClient)

    def test_getattr_invalid_attribute(self):
        """Test accessing invalid attribute raises AttributeError."""
        from web.backend.services import clients

        with pytest.raises(AttributeError) as exc_info:
            _ = clients.nonexistent_client

        assert "has no attribute 'nonexistent_client'" in str(exc_info.value)
