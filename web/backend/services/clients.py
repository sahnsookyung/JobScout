"""
Service client for calling internal microservices.
"""

import os
import logging

import httpx

logger = logging.getLogger(__name__)

EXTRACTION_URL = os.getenv("EXTRACTION_URL", "")
EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "")
SCORER_MATCHER_URL = os.getenv("SCORER_MATCHER_URL", "")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "")


class ServiceClient:
    
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
    
    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
        except httpx.RequestError as e:
            logger.error(f"Service call failed: {method} {url} - {e}")
            raise
    
    def get(self, path: str, **kwargs) -> dict:
        return self._request("GET", path, **kwargs)
    
    def post(self, path: str, **kwargs) -> dict:
        return self._request("POST", path, **kwargs)


class ExtractionClient(ServiceClient):
    """Client for Extraction service."""
    
    def __init__(self, base_url: str = EXTRACTION_URL):
        super().__init__(base_url)
    
    def extract_jobs(self, limit: int = 200) -> dict:
        """Extract job data."""
        return self.post("/extract/jobs", json={"limit": limit})
    
    def extract_resume(self, resume_file: str) -> dict:
        """Extract resume data."""
        return self.post("/extract/resume", json={"resume_file": resume_file})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get("/health")


class EmbeddingsClient(ServiceClient):
    """Client for Embeddings service."""
    
    def __init__(self, base_url: str = EMBEDDINGS_URL):
        super().__init__(base_url)
    
    def embed_jobs(self, limit: int = 100) -> dict:
        """Generate job embeddings."""
        return self.post("/embed/jobs", json={"limit": limit})
    
    def embed_resume(self, resume_fingerprint: str) -> dict:
        """Generate resume embeddings."""
        return self.post("/embed/resume", json={"resume_fingerprint": resume_fingerprint})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get("/health")


class ScorerMatcherClient(ServiceClient):
    """Client for Scorer-Matcher service."""
    
    def __init__(self, base_url: str = SCORER_MATCHER_URL):
        super().__init__(base_url)
    
    def match_resume(self, resume_fingerprint: str) -> dict:
        """Run matching for a resume."""
        return self.post("/match/resume", json={"resume_fingerprint": resume_fingerprint})
    
    def match_jobs(self, job_ids: list[str]) -> dict:
        """Run matching for jobs."""
        return self.post("/match/jobs", json={"job_ids": job_ids})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get("/health")


class OrchestratorClient(ServiceClient):
    """Client for Orchestrator service."""
    
    def __init__(self, base_url: str = ORCHESTRATOR_URL):
        super().__init__(base_url)
    
    def start_matching(self) -> dict:
        """Start the full pipeline: extraction -> embeddings -> matching."""
        return self.post("/orchestrate/match", json={})
    
    def health(self) -> dict:
        """Check service health."""
        return self.get("/health")


# Singleton instances
extraction_client = ExtractionClient()
embeddings_client = EmbeddingsClient()
scorer_matcher_client = ScorerMatcherClient()
orchestrator_client = OrchestratorClient()
