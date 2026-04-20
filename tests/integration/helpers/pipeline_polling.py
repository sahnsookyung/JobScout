"""Polling helpers for microservices pipeline E2E tests."""

from __future__ import annotations

import time
from typing import Callable, Optional

import requests


def _get_with_retry(
    url: str,
    *,
    timeout: float,
    attempts: int = 5,
    retry_delay_s: float = 0.5,
):
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return requests.get(url, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == attempts - 1:
                raise
            time.sleep(retry_delay_s)
    assert last_exc is not None
    raise last_exc


def _wait_for_terminal_state(
    *,
    url: str,
    label: str,
    timeout_s: float,
    poll_interval_s: float = 0.5,
    diagnostics: Optional[Callable[[], str]] = None,
) -> dict:
    deadline = time.time() + timeout_s
    last_payload = None
    last_status_code = None

    while time.time() < deadline:
        try:
            response = _get_with_retry(url, timeout=10)
        except requests.exceptions.RequestException:
            time.sleep(poll_interval_s)
            continue
        last_status_code = response.status_code
        if response.status_code == 200:
            try:
                last_payload = response.json()
            except ValueError:
                last_payload = {
                    "non_json_body": response.text[:500],
                }
            else:
                if last_payload.get("status") in {"completed", "failed", "cancelled"}:
                    return last_payload
        time.sleep(poll_interval_s)

    details = [
        f"{label} did not reach a terminal state within {timeout_s:.1f}s.",
        f"Last HTTP status: {last_status_code}",
        f"Last payload: {last_payload}",
    ]
    if diagnostics is not None:
        details.append(diagnostics())
    raise AssertionError("\n".join(details))


def wait_for_resume_terminal(
    base_url: str,
    task_id: str,
    *,
    timeout_s: float,
    poll_interval_s: float = 0.5,
    diagnostics: Optional[Callable[[], str]] = None,
) -> dict:
    return _wait_for_terminal_state(
        url=f"{base_url}/api/pipeline/resume-status/{task_id}",
        label=f"Resume task {task_id}",
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
        diagnostics=diagnostics,
    )


def wait_for_matching_terminal(
    base_url: str,
    task_id: str,
    *,
    timeout_s: float,
    poll_interval_s: float = 0.5,
    diagnostics: Optional[Callable[[], str]] = None,
) -> dict:
    return _wait_for_terminal_state(
        url=f"{base_url}/api/pipeline/status/{task_id}",
        label=f"Matching task {task_id}",
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
        diagnostics=diagnostics,
    )
