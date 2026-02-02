"""JobSpy API Client with connection reuse and retry logic."""

import logging
import threading
from typing import Optional, Dict, Any, List

import requests
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_fixed, 
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log
)

from core.config_loader import ScraperConfig

logger = logging.getLogger(__name__)


def _is_retryable_error(exc: Exception) -> bool:
    """
    Determine if an exception is retryable.
    
    Only retries on:
    - Timeouts
    - Server errors (5xx)
    - Connection errors without a response
    
    Does NOT retry on client errors (4xx).
    """
    # Always retry on timeouts
    if isinstance(exc, requests.Timeout):
        return True
    
    # Check for HTTP errors with status codes
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, 'response', None)
        if response is not None:
            # Only retry on 5xx server errors, not 4xx client errors
            return response.status_code >= 500
        return True  # Retry if no response (likely connection issue)
    
    # For other request exceptions, check if there's a response with 4xx
    if isinstance(exc, requests.RequestException):
        response = getattr(exc, 'response', None)
        if response is not None and response.status_code >= 400 and response.status_code < 500:
            return False  # Don't retry on 4xx client errors
        return True  # Retry on connection errors, etc.
    
    return False


class JobSpyClient:
    """
    Client for JobSpy API with connection pooling and retry logic.
    
    Responsibilities:
    - Own a requests.Session for connection reuse
    - Submit scraping jobs with retry logic
    - Poll for results with cancellation support
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        poll_interval_seconds: int = 10,
        job_timeout_seconds: int = 300,
        request_timeout_seconds: int = 30
    ):
        """
        Initialize JobSpy client.
        
        Args:
            base_url: Base URL for JobSpy API
            poll_interval_seconds: Seconds between status poll attempts
            job_timeout_seconds: Maximum seconds to wait for job completion
            request_timeout_seconds: Timeout for individual HTTP requests
        """
        self.base_url = base_url or "http://localhost:8000"
        self.poll_interval_seconds = poll_interval_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds
        
        self.session = requests.Session()
        
        logger.info(
            f"JobSpyClient initialized: base_url={self.base_url}, "
            f"poll_interval={poll_interval_seconds}s, "
            f"job_timeout={job_timeout_seconds}s"
        )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception(_is_retryable_error),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def submit_scrape(self, scraper_cfg: ScraperConfig) -> Optional[str]:
        """Submit a scraping job to JobSpy API."""
        payload = scraper_cfg.model_dump(exclude_none=True)
        site_types = payload.get("site_type") or ["unknown"]
        site_name = site_types[0] if site_types else "unknown"
        
        request_timeout = getattr(
            scraper_cfg, 
            "request_timeout", 
            self.request_timeout_seconds
        )
        
        logger.info(f"Submitting job for {site_name}")
        
        response = self.session.post(
            f"{self.base_url}/scrape",
            json=payload,
            timeout=request_timeout
        )
        response.raise_for_status()
        task_id = response.json().get("task_id")
        logger.info(f"Job submitted for {site_name}: task_id={task_id}")
        return task_id
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_exception(_is_retryable_error),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _poll_status(
        self, 
        task_id: str, 
        request_timeout_s: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Poll job status once."""
        timeout = request_timeout_s or self.request_timeout_seconds
        response = self.session.get(
            f"{self.base_url}/status/{task_id}",
            timeout=timeout
        )
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise ValueError(f"Task {task_id} not found")
        elif response.status_code >= 500:
            raise requests.HTTPError(f"Server error {response.status_code}")
        else:
            return None
    
    def wait_for_result(
        self,
        task_id: str,
        *,
        poll_interval_s: Optional[int] = None,
        job_timeout_s: Optional[int] = None,
        request_timeout_s: Optional[int] = None,
        stop_event: Optional[threading.Event] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Poll for job completion with cancellation support.
        
        Args:
            task_id: Task ID to poll
            poll_interval_s: Seconds between status poll attempts
            job_timeout_s: Maximum seconds to wait for job completion
            request_timeout_s: Timeout for individual HTTP requests (per-scraper override)
            stop_event: Threading event for cancellation
        """
        poll_interval = poll_interval_s or self.poll_interval_seconds
        job_timeout = job_timeout_s or self.job_timeout_seconds
        
        waited = 0
        
        while True:
            if stop_event and stop_event.is_set():
                logger.info(f"Polling cancelled for task {task_id}")
                return None
            
            try:
                result = self._poll_status(task_id, request_timeout_s)
                
                if result:
                    status = result.get("status")
                    
                    if status == "completed":
                        count = result.get("count", 0)
                        logger.info(f"Job {task_id} completed. Found {count} jobs.")
                        return result.get("data", [])
                    elif status == "failed":
                        error = result.get("error", "Unknown error")
                        logger.error(f"Job {task_id} failed: {error}")
                        return None
                
            except Exception as e:
                logger.warning(f"Polling error for {task_id}: {e}")
            
            if waited >= job_timeout:
                logger.warning(f"Timeout waiting for job {task_id}")
                return None
            
            if stop_event:
                stop_event.wait(poll_interval)
            else:
                import time
                time.sleep(poll_interval)
            
            waited += poll_interval
    
    def close(self):
        """Close the session and release resources."""
        self.session.close()
        logger.info("JobSpyClient session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
