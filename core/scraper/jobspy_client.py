"""JobSpy API Client with connection reuse and retry logic."""
import logging
import threading
import time
from typing import Optional, Dict, Any, List

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception,
    before_sleep_log
)

from core.config_loader import ScraperConfig

logger = logging.getLogger(__name__)

_FAILED_POLL_RESULT = object()


def _is_retryable_error(exc: Exception) -> bool:
    """
    Determine if an exception is retryable.
    
    Only retries on:
    - Timeouts
    - Server errors (5xx)
    - Connection errors without a response
    
    Does NOT retry on client errors (4xx).
    """
    if isinstance(exc, requests.Timeout):
        return True
    
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, 'response', None)
        if response is not None:
            return response.status_code >= 500
        return True
    
    if isinstance(exc, requests.RequestException):
        response = getattr(exc, 'response', None)
        if response is not None and response.status_code >= 400 and response.status_code < 500:
            return False
        return True
    
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
        api_token: Optional[str] = None,
        poll_interval_seconds: int = 10,
        job_timeout_seconds: int = 300,
        request_timeout_seconds: int = 30
    ):
        self.base_url = base_url.rstrip("/") if base_url else None
        self.poll_interval_seconds = poll_interval_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds
        
        self.session = requests.Session()
        if api_token:
            self.session.headers.update({"X-JobSpy-Token": api_token})
        
        logger.info(
            f"JobSpyClient initialized: configured={bool(self.base_url)}, "
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
        if not self.base_url:
            raise RuntimeError("JobSpy API URL is not configured")

        payload = scraper_cfg.model_dump(
            exclude_none=True,
            exclude={
                "description",
                "display_name",
                "enabled",
                "fetch_mode",
                "seed_url",
                "tags",
            },
        )
        site_types = payload.get("site_type") or ["unknown"]
        site_name = site_types[0] if site_types else "unknown"
        
        request_timeout = getattr(
            scraper_cfg, 
            "request_timeout", 
            self.request_timeout_seconds
        )
        
        logger.info(f"Submitting job for {site_name}")
        
        response = self.session.post(  # codeql[py/partial-ssrf] URL is config-driven (config.yaml), not user-supplied
            f"{self.base_url}/scrape",
            json=payload,
            timeout=request_timeout
        )
        response.raise_for_status()
        task_id = response.json().get("task_id")
        logger.info(f"Job submitted for {site_name}: task_id={task_id}")
        return task_id

    def check_health(
        self,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Check whether the JobSpy API is reachable without starting a scrape."""
        if not self.base_url:
            return {
                "available": False,
                "status": "not_configured",
                "endpoint": None,
                "status_code": None,
                "response_time_ms": 0,
                "error": "JobSpy API URL is not configured",
            }

        timeout = timeout_seconds or self.request_timeout_seconds
        endpoint = f"{self.base_url}/health"
        started = time.monotonic()

        try:
            response = self.session.get(  # codeql[py/partial-ssrf] URL is config-driven, not user input
                endpoint,
                timeout=timeout,
            )
            elapsed_ms = int((time.monotonic() - started) * 1000)
            available = 200 <= response.status_code < 400
            return {
                "available": available,
                "status": "available" if available else "unavailable",
                "endpoint": endpoint,
                "status_code": response.status_code,
                "response_time_ms": elapsed_ms,
                "error": None if available else f"HTTP {response.status_code}",
            }
        except requests.Timeout:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            return {
                "available": False,
                "status": "timeout",
                "endpoint": endpoint,
                "status_code": None,
                "response_time_ms": elapsed_ms,
                "error": "JobSpy health check timed out",
            }
        except requests.RequestException as exc:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            return {
                "available": False,
                "status": "unavailable",
                "endpoint": endpoint,
                "status_code": getattr(getattr(exc, "response", None), "status_code", None),
                "response_time_ms": elapsed_ms,
                "error": exc.__class__.__name__,
            }

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
        if not self.base_url:
            raise RuntimeError("JobSpy API URL is not configured")
        timeout = request_timeout_s or self.request_timeout_seconds
        response = self.session.get(  # codeql[py/partial-ssrf] URL is config-driven (config.yaml), not user-supplied
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

    @staticmethod
    def _is_cancelled(task_id: str, stop_event: Optional[threading.Event]) -> bool:
        if stop_event and stop_event.is_set():
            logger.info(f"Polling cancelled for task {task_id}")
            return True
        return False

    @staticmethod
    def _handle_poll_result(task_id: str, result: Optional[Dict[str, Any]]) -> object | List[Dict[str, Any]] | None:
        if not result:
            return None

        status = result.get("status")
        if status == "completed":
            count = result.get("count", 0)
            logger.info(f"Job {task_id} completed. Found {count} jobs.")
            return result.get("data", [])

        if status == "failed":
            error = result.get("error", "Unknown error")
            logger.error(f"Job {task_id} failed: {error}")
            return _FAILED_POLL_RESULT

        return None

    @staticmethod
    def _sleep_for_poll_interval(
        poll_interval: int,
        stop_event: Optional[threading.Event],
    ) -> None:
        if stop_event:
            stop_event.wait(poll_interval)
            return

        import time
        time.sleep(poll_interval)
    
    def wait_for_result(
        self,
        task_id: str,
        *,
        poll_interval_s: Optional[int] = None,
        job_timeout_s: Optional[int] = None,
        request_timeout_s: Optional[int] = None,
        stop_event: Optional[threading.Event] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Poll for job completion with cancellation support."""
        poll_interval = poll_interval_s or self.poll_interval_seconds
        job_timeout = job_timeout_s or self.job_timeout_seconds
        
        waited = 0
        
        while True:
            if self._is_cancelled(task_id, stop_event):
                return None
            
            try:
                poll_result = self._handle_poll_result(
                    task_id,
                    self._poll_status(task_id, request_timeout_s),
                )
                if poll_result is _FAILED_POLL_RESULT:
                    return None
                if poll_result is not None:
                    return poll_result
            except Exception as e:
                logger.warning(f"Polling error for {task_id}: {e}")
            
            if waited >= job_timeout:
                logger.warning(f"Timeout waiting for job {task_id}")
                return None
            
            self._sleep_for_poll_interval(poll_interval, stop_event)
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
