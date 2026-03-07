"""
Service client for calling internal microservices.
"""

import os
import logging
import threading
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# Health endpoint constant (avoids string duplication S1192)
HEALTH_ENDPOINT = "/health"

# Environment variables - validated at client instantiation time
EXTRACTION_URL = os.getenv("EXTRACTION_URL", "")
EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "")
SCORER_MATCHER_URL = os.getenv("SCORER_MATCHER_URL", "")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "")


def _validate_url(url: str, env_var: str) -> str:
    """Validate URL is configured and properly formatted.

    Logs warning if URL is empty (allows tests to run without env vars).
    Raises error if URL is provided but malformed.
    """
    if not url:
        logger.warning(f"{env_var} not configured - client will be unavailable")
        return ""
    parsed = urlparse(url)
    if not parsed.scheme or parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise RuntimeError(f"{env_var} must be a valid HTTP/HTTPS URL, got: {url}")
    return url


class ServiceClient:

    def __init__(self, base_url: str, timeout: int = 30, env_var: str = ""):
        if env_var:
            self.base_url = _validate_url(base_url, env_var)
        else:
            self.base_url = base_url
        self.timeout = timeout
    
    def _request(self, method: str, path: str, **kwargs) -> dict:
        if not self.base_url:
            raise RuntimeError("Service client not configured - base URL is empty")
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Service returned error: {method} {url} - {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Service call failed: {method} {url} - {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid JSON response: {method} {url} - {e}")
            raise
    
    def get(self, path: str, **kwargs) -> dict:
        return self._request("GET", path, **kwargs)
    
    def post(self, path: str, **kwargs) -> dict:
        return self._request("POST", path, **kwargs)


class ExtractionClient(ServiceClient):
    """Client for Extraction service."""

    def __init__(self, base_url: str = EXTRACTION_URL):
        super().__init__(base_url, env_var="EXTRACTION_URL")
    
    def extract_jobs(self, limit: int = 200) -> dict:
        """Extract job data."""
        return self.post("/extract/jobs", json={"limit": limit})
    
    def extract_resume(self, resume_file: str) -> dict:
        """Extract resume data."""
        return self.post("/extract/resume", json={"resume_file": resume_file})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get(HEALTH_ENDPOINT)


class EmbeddingsClient(ServiceClient):
    """Client for Embeddings service."""

    def __init__(self, base_url: str = EMBEDDINGS_URL):
        super().__init__(base_url, env_var="EMBEDDINGS_URL")
    
    def embed_jobs(self, limit: int = 100) -> dict:
        """Generate job embeddings."""
        return self.post("/embed/jobs", json={"limit": limit})
    
    def embed_resume(self, resume_fingerprint: str) -> dict:
        """Generate resume embeddings."""
        return self.post("/embed/resume", json={"resume_fingerprint": resume_fingerprint})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get(HEALTH_ENDPOINT)


class ScorerMatcherClient(ServiceClient):
    """Client for Scorer-Matcher service."""

    def __init__(self, base_url: str = SCORER_MATCHER_URL):
        super().__init__(base_url, env_var="SCORER_MATCHER_URL")
    
    def match_resume(self, resume_fingerprint: str) -> dict:
        """Run matching for a resume."""
        return self.post("/match/resume", json={"resume_fingerprint": resume_fingerprint})
    
    def match_jobs(self, job_ids: list[str]) -> dict:
        """Run matching for jobs."""
        return self.post("/match/jobs", json={"job_ids": job_ids})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get(HEALTH_ENDPOINT)


class OrchestratorClient(ServiceClient):
    """Client for Orchestrator service."""

    def __init__(self, base_url: str = ORCHESTRATOR_URL):
        super().__init__(base_url, env_var="ORCHESTRATOR_URL")

    def start_matching(self) -> dict:
        """Start the full pipeline: extraction -> embeddings -> matching."""
        return self.post("/orchestrate/match", json={})

    def get_task_status(self, task_id: str) -> dict:
        """Get status of a specific task."""
        return self.get(f"/orchestrate/status/{task_id}")

    def get_active_task(self) -> dict:
        """Get the currently active task, if any."""
        return self.get("/orchestrate/active")

    def stop_task(self) -> dict:
        """Stop the currently active task."""
        return self.post("/orchestrate/stop", json={})

    def wait_for_completion(self, task_id: str, timeout: float = 600.0, poll_interval: float = 2.0) -> dict:
        """Poll for task completion.
        
        Args:
            task_id: Task ID to wait for
            timeout: Maximum time to wait in seconds (default 10 minutes)
            poll_interval: Time between polls in seconds (default 2s)
            
        Returns:
            Final status dict with 'status' key ('completed', 'failed', 'cancelled', 'timeout')
        """
        import time
        import httpx
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                result = self.get(f"/orchestrate/status/{task_id}")
                status = result.get("status", "unknown")
                
                if status in ("completed", "failed", "cancelled"):
                    return {"success": True, "status": status, "result": result}
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    # Task not found - may not exist yet or was cleaned up
                    pass
                else:
                    # Other HTTP errors - log and continue polling
                    logger.warning(f"HTTP error polling task {task_id}: {e}")
            except httpx.RequestError as e:
                # Connection errors - log and continue polling
                logger.warning(f"Connection error polling task {task_id}: {e}")
            
            time.sleep(poll_interval)
        
        return {"success": False, "status": "timeout", "error": f"Timeout waiting for task {task_id}"}

    def health(self) -> dict:
        """Check service health."""
        return self.get(HEALTH_ENDPOINT)


# Lazy singleton instances - created on first access via __getattr__
_extraction_client = None
_embeddings_client = None
_scorer_matcher_client = None
_orchestrator_client = None

_clients_lock = threading.Lock()


def get_extraction_client() -> ExtractionClient:
    """Get or create ExtractionClient singleton."""
    global _extraction_client
    if _extraction_client is None:
        with _clients_lock:
            if _extraction_client is None:
                _extraction_client = ExtractionClient()
    return _extraction_client


def get_embeddings_client() -> EmbeddingsClient:
    """Get or create EmbeddingsClient singleton."""
    global _embeddings_client
    if _embeddings_client is None:
        with _clients_lock:
            if _embeddings_client is None:
                _embeddings_client = EmbeddingsClient()
    return _embeddings_client


def get_scorer_matcher_client() -> ScorerMatcherClient:
    """Get or create ScorerMatcherClient singleton."""
    global _scorer_matcher_client
    if _scorer_matcher_client is None:
        with _clients_lock:
            if _scorer_matcher_client is None:
                _scorer_matcher_client = ScorerMatcherClient()
    return _scorer_matcher_client


def get_orchestrator_client() -> OrchestratorClient:
    """Get or create OrchestratorClient singleton."""
    global _orchestrator_client
    if _orchestrator_client is None:
        with _clients_lock:
            if _orchestrator_client is None:
                _orchestrator_client = OrchestratorClient()
    return _orchestrator_client


def __getattr__(name: str):
    """Lazy load singleton clients on first access."""
    if name == "extraction_client":
        return get_extraction_client()
    if name == "embeddings_client":
        return get_embeddings_client()
    if name == "scorer_matcher_client":
        return get_scorer_matcher_client()
    if name == "orchestrator_client":
        return get_orchestrator_client()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
