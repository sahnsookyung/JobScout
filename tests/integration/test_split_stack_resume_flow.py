"""Split-stack E2E coverage for resume upload -> extract/embed -> matching."""

from __future__ import annotations

import atexit
import json
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
import requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from database.models import (
    CandidatePreferences,
    JobMatch,
    MatchSelectionItem,
    MatchSelectionRun,
    NotificationTracker,
    ResumeEvidenceUnitEmbedding,
    ResumeSectionEmbedding,
    ResumeUpload,
    StructuredResume,
    UserNotificationChannel,
    UserNotificationSettings,
)
from tests.integration.helpers.compose_env import ensure_compose_env_file
from tests.integration.helpers.pipeline_polling import (
    wait_for_matching_terminal,
    wait_for_resume_terminal,
)
from tests.integration.helpers.seed_matching_jobs import (
    reset_split_stack_state,
    seed_matcher_ready_jobs,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"
VALID_RESUME_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "resumes" / "valid_resume.json"
FAIL_EMBEDDING_RESUME_FIXTURE = (
    PROJECT_ROOT / "tests" / "fixtures" / "resumes" / "fail_embedding_resume.json"
)
DEV_USER_ID = "00000000-0000-0000-0000-000000000001"
E2E_COMPOSE_PROJECT_NAME = "jobscout-e2e"
STARTUP_TIMEOUT_SECONDS = 180.0
UPLOAD_TIMEOUT_SECONDS = 150.0
MATCHING_TIMEOUT_SECONDS = 150.0
NOTIFICATION_TIMEOUT_SECONDS = 30.0
_ACTIVE_SPLIT_STACK_CLEANUP: dict[str, object] = {}


@dataclass(frozen=True)
class SplitStackContext:
    base_url: str
    database_url: str
    service_urls: dict[str, str]
    compose_args: tuple[str, ...]
    compose_env: dict[str, str]
    created_dotenv: bool = False


def _docker_available() -> bool:
    result = subprocess.run(
        ["docker", "version"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result.returncode == 0


def _compose_env() -> dict[str, str]:
    def reserve_port() -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return str(sock.getsockname()[1])

    env = os.environ.copy()
    env.update(
        {
            "POSTGRES_PORT": reserve_port(),
            "REDIS_PORT": reserve_port(),
            "JOBSPY_PORT": reserve_port(),
            "MAILPIT_SMTP_PORT": reserve_port(),
            "MAILPIT_UI_PORT": reserve_port(),
            "WEB_BACKEND_PORT": reserve_port(),
            "EXTRACTION_PORT": reserve_port(),
            "EMBEDDINGS_PORT": reserve_port(),
            "SCORER_MATCHER_PORT": reserve_port(),
            "ORCHESTRATOR_PORT": reserve_port(),
            "RESUME_ETL_WAIT_TIMEOUT_SECONDS": "120",
            "LISTENER_TIMEOUT_SECONDS": "120",
        }
    )
    return env


def _next_compose_env() -> dict[str, str]:
    return _compose_env()


def _compose_args(project_name: str) -> tuple[str, ...]:
    return (
        "docker",
        "compose",
        "-p",
        project_name,
        "-f",
        str(PROJECT_ROOT / "docker-compose.yml"),
        "-f",
        str(PROJECT_ROOT / "docker-compose.microservices.yml"),
        "-f",
        str(PROJECT_ROOT / "docker-compose.web.yml"),
        "-f",
        str(PROJECT_ROOT / "docker-compose.e2e.yml"),
    )


def _run_compose(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
    *args: str,
    check: bool = True,
    timeout: float = 1800,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*compose_args, *args],
        cwd=PROJECT_ROOT,
        env=compose_env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


def _compose_down(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
) -> None:
    _run_compose(
        compose_args,
        compose_env,
        "down",
        "-v",
        "--remove-orphans",
        check=False,
        timeout=600,
    )
    project_name = _compose_project_name(compose_args)
    if not _compose_project_container_ids(project_name):
        return

    _force_remove_compose_project_resources(project_name)


def _compose_project_name(compose_args: tuple[str, ...]) -> str:
    project_flag_index = compose_args.index("-p")
    return compose_args[project_flag_index + 1]


def _compose_project_container_ids(project_name: str) -> list[str]:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={project_name}",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _force_remove_compose_project_resources(project_name: str) -> None:
    container_ids = _compose_project_container_ids(project_name)
    if container_ids:
        subprocess.run(
            ["docker", "rm", "-f", "-v", *container_ids],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    subprocess.run(
        ["docker", "network", "rm", f"{project_name}_default"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _register_active_split_stack_cleanup(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
) -> None:
    _ACTIVE_SPLIT_STACK_CLEANUP["compose_args"] = compose_args
    _ACTIVE_SPLIT_STACK_CLEANUP["compose_env"] = compose_env


def _clear_active_split_stack_cleanup() -> None:
    _ACTIVE_SPLIT_STACK_CLEANUP.clear()


def _cleanup_active_split_stack() -> None:
    compose_args = _ACTIVE_SPLIT_STACK_CLEANUP.get("compose_args")
    compose_env = _ACTIVE_SPLIT_STACK_CLEANUP.get("compose_env")
    if not compose_args or not compose_env:
        return

    try:
        _compose_down(compose_args, compose_env)
    finally:
        _clear_active_split_stack_cleanup()


atexit.register(_cleanup_active_split_stack)


def _compose_up_args(services: tuple[str, ...], *, build_images: bool) -> tuple[str, ...]:
    build_flag = "--build" if build_images else "--no-build"
    return (
        "--profile",
        "split",
        "--profile",
        "web",
        "up",
        "-d",
        build_flag,
        *services,
    )


def _env_flag(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean environment value for {name}: {raw!r}")


def _compose_images_available(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
) -> bool:
    result = _run_compose(
        compose_args,
        compose_env,
        "config",
        "--images",
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        return False

    image_names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not image_names:
        return False

    inspect = subprocess.run(
        ["docker", "image", "inspect", *image_names],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return inspect.returncode == 0


def _resolve_build_images(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
) -> bool:
    override = _env_flag("JOBSCOUT_E2E_BUILD_IMAGES")
    if override is not None:
        return override
    skip_build = _env_flag("JOBSCOUT_E2E_SKIP_BUILD")
    if skip_build is not None:
        return not skip_build
    return not _compose_images_available(compose_args, compose_env)


def _compose_up_with_retries(
    compose_args: tuple[str, ...],
    services: tuple[str, ...],
    *,
    build_images: bool | None = None,
    attempts: int = 3,
) -> tuple[dict[str, str], subprocess.CompletedProcess[str]]:
    last_error = None
    for _ in range(attempts):
        compose_env = _next_compose_env()
        resolved_build_images = (
            build_images
            if build_images is not None
            else _resolve_build_images(compose_args, compose_env)
        )
        _compose_down(compose_args, compose_env)
        try:
            result = _run_compose(
                compose_args,
                compose_env,
                *_compose_up_args(services, build_images=resolved_build_images),
            )
            return compose_env, result
        except subprocess.CalledProcessError as exc:
            last_error = exc
            stderr = exc.stderr or ""
            _compose_down(compose_args, compose_env)
            if "port is already allocated" not in stderr.lower():
                raise
    if last_error is not None:
        raise last_error
    raise AssertionError("Failed to bring up compose stack")


def _wait_for_http_health(url: str, *, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return
            last_error = f"{url} returned {response.status_code}: {response.text}"
        except Exception as exc:  # noqa: BLE001 - include live diagnostics on timeout
            last_error = str(exc)
        time.sleep(1)
    raise AssertionError(f"Service healthcheck did not pass for {url}: {last_error}")


def _parse_ps_json(raw_output: str) -> list[dict]:
    raw_output = raw_output.strip()
    if not raw_output:
        return []
    if raw_output.startswith("["):
        return json.loads(raw_output)
    lines = [line for line in raw_output.splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _assert_db_migrate_succeeded(compose_args: tuple[str, ...], compose_env: dict[str, str]) -> None:
    result = _run_compose(compose_args, compose_env, "ps", "-a", "db-migrate", "--format", "json")
    rows = _parse_ps_json(result.stdout)
    assert rows, "db-migrate container was not found in compose status output"
    db_migrate = rows[0]
    exit_code = str(db_migrate.get("ExitCode", ""))
    state = str(db_migrate.get("State", ""))
    assert state.lower().startswith("exited"), f"db-migrate not exited as expected: {db_migrate}"
    assert exit_code == "0", f"db-migrate did not exit cleanly: {db_migrate}"


def _wait_for_compose_service_state(
    compose_args: tuple[str, ...],
    compose_env: dict[str, str],
    service_name: str,
    expected_state_prefix: str,
    *,
    timeout_s: float,
) -> None:
    deadline = time.time() + timeout_s
    last_rows: list[dict] = []
    expected = expected_state_prefix.lower()

    while time.time() < deadline:
        result = _run_compose(
            compose_args,
            compose_env,
            "ps",
            service_name,
            "--format",
            "json",
            check=False,
            timeout=120,
        )
        rows = _parse_ps_json(result.stdout)
        last_rows = rows
        if rows:
            state = str(rows[0].get("State", "")).lower()
            if state.startswith(expected):
                return
        time.sleep(1)

    raise AssertionError(
        f"Service {service_name} did not reach state '{expected_state_prefix}'. "
        f"Last compose status: {last_rows}"
    )


def _assert_shared_upload_dir_writable(
    compose_args: tuple[str, ...], compose_env: dict[str, str]
) -> None:
    command = (
        "from pathlib import Path; "
        "path = Path('/data/resume_uploads/.e2e-write-check'); "
        "path.write_text('ok', encoding='utf-8'); "
        "path.unlink()"
    )
    result = _run_compose(
        compose_args,
        compose_env,
        "exec",
        "-T",
        "web-backend",
        "python",
        "-c",
        command,
    )
    assert result.returncode == 0, result.stderr


def _stack_diagnostics(context: SplitStackContext) -> str:
    ps = _run_compose(
        context.compose_args,
        context.compose_env,
        "ps",
        "--format",
        "json",
        check=False,
        timeout=120,
    )
    logs = _run_compose(
        context.compose_args,
        context.compose_env,
        "logs",
        "--no-color",
        "db-migrate",
        "notification-worker",
        "web-backend",
        "orchestrator",
        "extraction",
        "embeddings",
        "scorer-matcher",
        "mock-llm",
        check=False,
        timeout=120,
    )
    health_urls = {
        "web-backend": f"{context.base_url}/health",
        "orchestrator": context.service_urls["orchestrator"],
        "extraction": context.service_urls["extraction"],
        "embeddings": context.service_urls["embeddings"],
        "scorer-matcher": context.service_urls["scorer-matcher"],
    }
    health = {}
    for name, url in health_urls.items():
        try:
            response = requests.get(url, timeout=5)
            health[name] = {"status_code": response.status_code, "body": response.text[:500]}
        except Exception as exc:  # noqa: BLE001
            health[name] = {"error": str(exc)}

    return "\n".join(
        [
            "=== compose ps ===",
            ps.stdout.strip(),
            "=== service health ===",
            json.dumps(health, indent=2, sort_keys=True),
            "=== compose logs ===",
            logs.stdout[-12000:],
        ]
    )


@pytest.fixture(scope="module")
def split_stack() -> SplitStackContext:
    if not _docker_available():
        pytest.skip("Docker is not available for split-stack E2E tests")

    project_name = E2E_COMPOSE_PROJECT_NAME
    compose_args = _compose_args(project_name)
    services = (
        "postgres",
        "redis",
        "mailpit",
        "db-migrate",
        "mock-llm",
        "extraction",
        "embeddings",
        "notification-worker",
        "scorer-matcher",
        "orchestrator",
        "web-backend",
    )

    compose_env: dict[str, str] | None = None
    created_dotenv = ensure_compose_env_file(PROJECT_ROOT)
    build_images = _env_flag("JOBSCOUT_E2E_BUILD_IMAGES")
    try:
        compose_env, _ = _compose_up_with_retries(
            compose_args,
            services,
            build_images=build_images,
        )
        _register_active_split_stack_cleanup(compose_args, compose_env)

        web_backend_url = f"http://localhost:{compose_env['WEB_BACKEND_PORT']}"
        extraction_url = f"http://localhost:{compose_env['EXTRACTION_PORT']}"
        embeddings_url = f"http://localhost:{compose_env['EMBEDDINGS_PORT']}"
        scorer_matcher_url = f"http://localhost:{compose_env['SCORER_MATCHER_PORT']}"
        orchestrator_url = f"http://localhost:{compose_env['ORCHESTRATOR_PORT']}"

        _wait_for_http_health(f"{web_backend_url}/health", timeout_s=STARTUP_TIMEOUT_SECONDS)
        _wait_for_http_health(f"{extraction_url}/health", timeout_s=STARTUP_TIMEOUT_SECONDS)
        _wait_for_http_health(f"{embeddings_url}/health", timeout_s=STARTUP_TIMEOUT_SECONDS)
        _wait_for_http_health(f"{scorer_matcher_url}/health", timeout_s=STARTUP_TIMEOUT_SECONDS)
        _wait_for_http_health(f"{orchestrator_url}/health", timeout_s=STARTUP_TIMEOUT_SECONDS)
        _wait_for_compose_service_state(
            compose_args,
            compose_env,
            "notification-worker",
            "running",
            timeout_s=STARTUP_TIMEOUT_SECONDS,
        )
        _assert_db_migrate_succeeded(compose_args, compose_env)
        _assert_shared_upload_dir_writable(compose_args, compose_env)

        yield SplitStackContext(
            base_url=web_backend_url,
            database_url=(
                f"postgresql://user:password@localhost:{compose_env['POSTGRES_PORT']}/jobscout"
            ),
            service_urls={
                "extraction": f"{extraction_url}/health",
                "embeddings": f"{embeddings_url}/health",
                "scorer-matcher": f"{scorer_matcher_url}/health",
                "orchestrator": f"{orchestrator_url}/health",
            },
            compose_args=compose_args,
            compose_env=compose_env,
            created_dotenv=created_dotenv,
        )
    finally:
        if compose_env is None:
            compose_env = _next_compose_env()
        _compose_down(compose_args, compose_env)
        _clear_active_split_stack_cleanup()
        if created_dotenv:
            DOTENV_PATH.unlink(missing_ok=True)


def _session_for(database_url: str):
    engine = create_engine(database_url)
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, session_local()


def _upload_resume(base_url: str, resume_path: Path) -> dict:
    with resume_path.open("rb") as handle:
        response = requests.post(
            f"{base_url}/api/pipeline/upload-resume",
            files={"file": (resume_path.name, handle.read(), "application/json")},
            timeout=30,
        )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True, payload
    assert payload["task_id"], payload
    assert payload["upload_id"], payload
    return payload


def _get_notification_settings(base_url: str) -> dict:
    response = requests.get(f"{base_url}/api/v1/notification-settings", timeout=15)
    assert response.status_code == 200, response.text
    return response.json()


def _update_notification_settings(base_url: str, payload: dict) -> dict:
    response = requests.put(
        f"{base_url}/api/v1/notification-settings",
        json=payload,
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _get_candidate_preferences(base_url: str) -> dict:
    response = requests.get(f"{base_url}/api/v1/candidate-preferences", timeout=15)
    assert response.status_code == 200, response.text
    return response.json()


def _update_candidate_preferences(base_url: str, payload: dict) -> dict:
    response = requests.put(
        f"{base_url}/api/v1/candidate-preferences",
        json=payload,
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _get_matches(
    base_url: str,
    *,
    status: str = "active",
    ranking_mode: str | None = None,
    show_hidden: bool = False,
    min_fit: float | None = None,
) -> dict:
    params: dict[str, object] = {
        "status": status,
        "show_hidden": str(show_hidden).lower(),
    }
    if ranking_mode is not None:
        params["ranking_mode"] = ranking_mode
    if min_fit is not None:
        params["min_fit"] = min_fit
    response = requests.get(
        f"{base_url}/api/matches",
        params=params,
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()

def _toggle_match_hidden(base_url: str, match_id: str) -> dict:
    response = requests.post(
        f"{base_url}/api/matches/{match_id}/hide",
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _get_match_explanation(base_url: str, match_id: str) -> dict:
    response = requests.get(
        f"{base_url}/api/matches/{match_id}/explanation",
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _send_notification_settings_test(base_url: str, channel_type: str) -> dict:
    response = requests.post(
        f"{base_url}/api/v1/notification-settings/test",
        json={"channel_type": channel_type},
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _wait_for_test_status(base_url: str, channel_type: str, expected_status: str) -> dict:
    deadline = time.time() + NOTIFICATION_TIMEOUT_SECONDS
    last_payload = None
    while time.time() < deadline:
        payload = _get_notification_settings(base_url)
        channel = payload["channels"][channel_type]
        last_payload = payload
        if channel["last_test_status"] == expected_status:
            return payload
        time.sleep(0.5)
    raise AssertionError(
        f"Timed out waiting for notification test status '{expected_status}'. "
        f"Last payload: {last_payload}"
    )


def _wait_for_automatic_notification_delivery(
    database_url: str,
    *,
    owner_id: str,
    channel_type: str,
) -> list[NotificationTracker]:
    deadline = time.time() + NOTIFICATION_TIMEOUT_SECONDS
    owner_uuid = uuid.UUID(owner_id)
    last_event_types: list[str] = []

    while time.time() < deadline:
        engine, session = _session_for(database_url)
        try:
            rows = session.execute(
                select(NotificationTracker).where(
                    NotificationTracker.owner_id == owner_uuid,
                    NotificationTracker.channel_type == channel_type,
                    NotificationTracker.sent_successfully.is_(True),
                    NotificationTracker.event_type.in_(
                        ["new_match_alert", "batch_complete"]
                    ),
                )
            ).scalars().all()
            last_event_types = [row.event_type for row in rows]
            has_batch_complete = "batch_complete" in last_event_types
            has_match_notification = "new_match_alert" in last_event_types
            if has_batch_complete and has_match_notification:
                return rows
        finally:
            session.close()
            engine.dispose()
        time.sleep(1)

    raise AssertionError(
        "Timed out waiting for automatic notification delivery. "
        f"Last successful event types: {last_event_types}"
    )


def _reset_notification_state(database_url: str) -> None:
    engine, session = _session_for(database_url)
    try:
        session.query(NotificationTracker).delete()
        session.query(UserNotificationChannel).delete()
        session.query(UserNotificationSettings).delete()
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


def test_resume_upload_completes_then_matching_completes(split_stack: SplitStackContext):
    reset_split_stack_state(split_stack.database_url)
    seeded_jobs = seed_matcher_ready_jobs(split_stack.database_url)

    upload_payload = _upload_resume(split_stack.base_url, VALID_RESUME_FIXTURE)
    diagnostics = lambda: _stack_diagnostics(split_stack)
    resume_state = wait_for_resume_terminal(
        split_stack.base_url,
        upload_payload["task_id"],
        timeout_s=UPLOAD_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )

    assert resume_state["status"] == "completed", resume_state

    eligibility = requests.get(
        f"{split_stack.base_url}/api/pipeline/resume-eligibility",
        timeout=15,
    )
    assert eligibility.status_code == 200, eligibility.text
    eligibility_payload = eligibility.json()
    assert eligibility_payload["can_run"] is True, eligibility_payload

    engine, session = _session_for(split_stack.database_url)
    try:
        upload = session.execute(
            select(ResumeUpload).where(ResumeUpload.id == upload_payload["upload_id"])
        ).scalar_one()
        fingerprint = upload.resume_fingerprint

        structured_resume = session.execute(
            select(StructuredResume).where(StructuredResume.resume_fingerprint == fingerprint)
        ).scalar_one_or_none()
        assert structured_resume is not None

        section_count = session.query(ResumeSectionEmbedding).filter(
            ResumeSectionEmbedding.resume_fingerprint == fingerprint
        ).count()
        evidence_count = session.query(ResumeEvidenceUnitEmbedding).filter(
            ResumeEvidenceUnitEmbedding.resume_fingerprint == fingerprint
        ).count()
        assert section_count >= 1
        assert evidence_count >= 1
    finally:
        session.close()
        engine.dispose()

    run_response = requests.post(
        f"{split_stack.base_url}/api/pipeline/run-matching",
        timeout=30,
    )
    assert run_response.status_code == 200, run_response.text
    run_payload = run_response.json()
    assert run_payload["success"] is True, run_payload
    assert run_payload["task_id"], run_payload

    matching_state = wait_for_matching_terminal(
        split_stack.base_url,
        run_payload["task_id"],
        timeout_s=MATCHING_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert matching_state["status"] == "completed", matching_state
    assert (matching_state.get("saved_count") or 0) >= 1, matching_state

    engine, session = _session_for(split_stack.database_url)
    try:
        matches = session.execute(
            select(JobMatch).where(JobMatch.resume_fingerprint == fingerprint)
        ).scalars().all()
        assert matches, "Expected at least one persisted match"
        matched_job_ids = {str(match.job_post_id) for match in matches}
        assert seeded_jobs.positive_job_id in matched_job_ids

        active_matches = [match for match in matches if match.status == "active"]
        selection_run = session.execute(
            select(MatchSelectionRun).where(
                MatchSelectionRun.resume_fingerprint == fingerprint,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
            )
        ).scalar_one()
        selection_items = session.execute(
            select(MatchSelectionItem).where(
                MatchSelectionItem.selection_run_id == selection_run.id
            )
        ).scalars().all()

        assert selection_run.policy_snapshot_json
        assert selection_run.selected_count == len(selection_items)
        assert selection_run.candidate_pool_size >= selection_run.selected_count
        assert selection_run.alert_candidate_count <= selection_run.selected_count
        assert [item.rank_position for item in selection_items] == list(
            range(1, len(selection_items) + 1)
        )
        assert {str(item.job_match_id) for item in selection_items} == {
            str(match.id) for match in active_matches
        }
    finally:
        session.close()
        engine.dispose()


def test_matching_flow_triggers_email_notifications(split_stack: SplitStackContext):
    reset_split_stack_state(split_stack.database_url)
    _reset_notification_state(split_stack.database_url)
    seed_matcher_ready_jobs(split_stack.database_url)

    updated_settings = _update_notification_settings(
        split_stack.base_url,
        {
            "notifications_enabled": True,
            "min_fit_for_alerts": 0,
            "notify_on_new_match": True,
            "notify_on_batch_complete": True,
            "channels": {
                "email": {
                    "enabled": True,
                }
            },
        },
    )
    assert updated_settings["channels"]["email"]["enabled"] is True

    upload_payload = _upload_resume(split_stack.base_url, VALID_RESUME_FIXTURE)
    diagnostics = lambda: _stack_diagnostics(split_stack)
    resume_state = wait_for_resume_terminal(
        split_stack.base_url,
        upload_payload["task_id"],
        timeout_s=UPLOAD_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert resume_state["status"] == "completed", resume_state

    run_response = requests.post(
        f"{split_stack.base_url}/api/pipeline/run-matching",
        timeout=30,
    )
    assert run_response.status_code == 200, run_response.text
    run_payload = run_response.json()
    assert run_payload["success"] is True, run_payload

    matching_state = wait_for_matching_terminal(
        split_stack.base_url,
        run_payload["task_id"],
        timeout_s=MATCHING_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert matching_state["status"] == "completed", matching_state
    assert (matching_state.get("saved_count") or 0) >= 1, matching_state
    assert (matching_state.get("notified_count") or 0) >= 1, matching_state

    delivered_notifications = _wait_for_automatic_notification_delivery(
        split_stack.database_url,
        owner_id=DEV_USER_ID,
        channel_type="email",
    )
    delivered_event_types = {row.event_type for row in delivered_notifications}
    assert "batch_complete" in delivered_event_types
    assert "new_match_alert" in delivered_event_types

    engine, session = _session_for(split_stack.database_url)
    try:
        matches = session.execute(select(JobMatch)).scalars().all()
        assert matches, "Expected persisted matches after matching completed"
        assert any(match.notified for match in matches)
        active_match = next(match for match in matches if match.status == "active")
        selection_run = session.execute(
            select(MatchSelectionRun).where(
                MatchSelectionRun.resume_fingerprint == active_match.resume_fingerprint,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
            )
        ).scalar_one()
        selection_items = session.execute(
            select(MatchSelectionItem).where(
                MatchSelectionItem.selection_run_id == selection_run.id
            )
        ).scalars().all()
        selection_match_ids = {str(item.job_match_id) for item in selection_items}

        notified_match_ids = {
            str(row.job_match_id)
            for row in delivered_notifications
            if row.event_type == "new_match_alert" and row.job_match_id is not None
        }
        assert notified_match_ids
        assert notified_match_ids.issubset(selection_match_ids)
    finally:
        session.close()
        engine.dispose()

    matches_payload = _get_matches(split_stack.base_url)
    assert matches_payload["success"] is True, matches_payload
    assert matches_payload["count"] >= 1, matches_payload
    assert any(match["match_id"] == str(active_match.id) for match in matches_payload["matches"])

    explanation_payload = _get_match_explanation(split_stack.base_url, str(active_match.id))
    assert explanation_payload["success"] is True, explanation_payload
    explanation = explanation_payload["explanation"]
    assert explanation is not None
    assert explanation["fit_scorer"]["name"] == "cross_encoder_semantic_fit"
    assert explanation["diagnostics"]["effective_fit_mode"] in {"cross_encoder", "threshold"}
    assert explanation["diagnostics"]["provider_route"] in {"local", "local_heuristic", "remote", "threshold"}
    assert explanation["retrieval"]["mode"] in {"dense", "hybrid"}

    fit_first_payload = _get_matches(split_stack.base_url, ranking_mode="fit_first")
    balanced_payload = _get_matches(split_stack.base_url, ranking_mode="balanced")
    preference_first_payload = _get_matches(
        split_stack.base_url,
        ranking_mode="preference_first",
    )
    active_ids = {
        match["match_id"] for match in matches_payload["matches"]
    }
    assert {match["match_id"] for match in fit_first_payload["matches"]} == active_ids
    assert {match["match_id"] for match in balanced_payload["matches"]} == active_ids
    assert {match["match_id"] for match in preference_first_payload["matches"]} == active_ids

    hide_payload = _toggle_match_hidden(split_stack.base_url, str(active_match.id))
    assert hide_payload["success"] is True, hide_payload
    assert hide_payload["is_hidden"] is True, hide_payload

    hidden_default_payload = _get_matches(split_stack.base_url)
    assert all(
        match["match_id"] != str(active_match.id)
        for match in hidden_default_payload["matches"]
    )

    hidden_included_payload = _get_matches(split_stack.base_url, show_hidden=True)
    assert any(
        match["match_id"] == str(active_match.id)
        for match in hidden_included_payload["matches"]
    )

    engine, session = _session_for(split_stack.database_url)
    try:
        current_selection_items = session.execute(
            select(MatchSelectionItem).where(
                MatchSelectionItem.selection_run_id == selection_run.id
            )
        ).scalars().all()
        assert any(str(item.job_match_id) == str(active_match.id) for item in current_selection_items)
    finally:
        session.close()
        engine.dispose()


def test_candidate_preferences_round_trip_updates_matching_behavior(
    split_stack: SplitStackContext,
):
    reset_split_stack_state(split_stack.database_url)
    seeded_jobs = seed_matcher_ready_jobs(split_stack.database_url)

    initial_preferences = _get_candidate_preferences(split_stack.base_url)
    assert initial_preferences["remote_mode"] == "any"
    assert initial_preferences["revision"] >= 0

    onsite_preferences = _update_candidate_preferences(
        split_stack.base_url,
        {
            "remote_mode": "onsite",
            "target_locations": ["On-site"],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "",
        },
    )
    assert onsite_preferences["remote_mode"] == "onsite"
    assert onsite_preferences["target_locations"] == ["On-site"]
    assert onsite_preferences["revision"] >= initial_preferences["revision"] + 1

    diagnostics = lambda: _stack_diagnostics(split_stack)
    upload_payload = _upload_resume(split_stack.base_url, VALID_RESUME_FIXTURE)
    resume_state = wait_for_resume_terminal(
        split_stack.base_url,
        upload_payload["task_id"],
        timeout_s=UPLOAD_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert resume_state["status"] == "completed", resume_state

    run_response = requests.post(
        f"{split_stack.base_url}/api/pipeline/run-matching",
        timeout=30,
    )
    assert run_response.status_code == 200, run_response.text
    run_payload = run_response.json()
    assert run_payload["success"] is True, run_payload

    matching_state = wait_for_matching_terminal(
        split_stack.base_url,
        run_payload["task_id"],
        timeout_s=MATCHING_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert matching_state["status"] == "completed", matching_state

    engine, session = _session_for(split_stack.database_url)
    try:
        upload = session.execute(
            select(ResumeUpload).where(ResumeUpload.id == upload_payload["upload_id"])
        ).scalar_one()
        fingerprint = upload.resume_fingerprint

        blocked_matches = session.execute(
            select(JobMatch).where(
                JobMatch.resume_fingerprint == fingerprint,
                JobMatch.status == "active",
            )
        ).scalars().all()
        blocked_job_ids = {str(match.job_post_id) for match in blocked_matches}
        assert seeded_jobs.positive_job_id not in blocked_job_ids

        stored_preferences = session.execute(
            select(CandidatePreferences).where(
                CandidatePreferences.owner_id == uuid.UUID(DEV_USER_ID)
            )
        ).scalar_one()
        assert stored_preferences.remote_mode == "onsite"
        assert list(stored_preferences.target_locations or []) == ["On-site"]
    finally:
        session.close()
        engine.dispose()

    remote_preferences = _update_candidate_preferences(
        split_stack.base_url,
        {
            "remote_mode": "remote",
            "target_locations": ["Remote"],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "Python backend FastAPI microservices mentorship",
        },
    )
    assert remote_preferences["remote_mode"] == "remote"
    assert remote_preferences["target_locations"] == ["Remote"]
    assert remote_preferences["soft_preferences"] == (
        "Python backend FastAPI microservices mentorship"
    )
    assert remote_preferences["revision"] >= onsite_preferences["revision"] + 1

    rerun_response = requests.post(
        f"{split_stack.base_url}/api/pipeline/run-matching",
        timeout=30,
    )
    assert rerun_response.status_code == 200, rerun_response.text
    rerun_payload = rerun_response.json()
    assert rerun_payload["success"] is True, rerun_payload

    rerun_state = wait_for_matching_terminal(
        split_stack.base_url,
        rerun_payload["task_id"],
        timeout_s=MATCHING_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert rerun_state["status"] == "completed", rerun_state
    assert (rerun_state.get("saved_count") or 0) >= 1, rerun_state

    engine, session = _session_for(split_stack.database_url)
    try:
        matches = session.execute(
            select(JobMatch).where(
                JobMatch.resume_fingerprint == fingerprint,
                JobMatch.status == "active",
            )
        ).scalars().all()
        assert matches, "Expected a persisted match after enabling remote preferences"

        matched_job_ids = {str(match.job_post_id) for match in matches}
        assert seeded_jobs.positive_job_id in matched_job_ids
        assert seeded_jobs.negative_job_id not in matched_job_ids

        positive_match = next(
            match for match in matches if str(match.job_post_id) == seeded_jobs.positive_job_id
        )
        preference_components = positive_match.preference_components or {}
        assert (positive_match.preference_score or 0) > 0, (
            f"Expected preference_score > 0, got {positive_match.preference_score}"
        )
        assert preference_components.get("preference_mode_used") == "semantic_rerank"
        assert "tech_stack_match" in (preference_components.get("preference_reason_codes") or [])
    finally:
        session.close()
        engine.dispose()


def test_preference_cross_encoder_reranking_emits_detail_codes(split_stack: SplitStackContext):
    reset_split_stack_state(split_stack.database_url)
    seeded_jobs = seed_matcher_ready_jobs(split_stack.database_url)

    _update_candidate_preferences(
        split_stack.base_url,
        {
            "remote_mode": "remote",
            "target_locations": ["Remote"],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "Python backend FastAPI microservices mentorship",
        },
    )

    diagnostics = lambda: _stack_diagnostics(split_stack)
    upload_payload = _upload_resume(split_stack.base_url, VALID_RESUME_FIXTURE)
    resume_state = wait_for_resume_terminal(
        split_stack.base_url,
        upload_payload["task_id"],
        timeout_s=UPLOAD_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert resume_state["status"] == "completed", resume_state

    run_response = requests.post(
        f"{split_stack.base_url}/api/pipeline/run-matching",
        timeout=30,
    )
    assert run_response.status_code == 200, run_response.text
    run_payload = run_response.json()
    assert run_payload["success"] is True, run_payload

    matching_state = wait_for_matching_terminal(
        split_stack.base_url,
        run_payload["task_id"],
        timeout_s=MATCHING_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )
    assert matching_state["status"] == "completed", matching_state
    assert (matching_state.get("saved_count") or 0) >= 1, matching_state

    engine, session = _session_for(split_stack.database_url)
    try:
        upload = session.execute(
            select(ResumeUpload).where(ResumeUpload.id == upload_payload["upload_id"])
        ).scalar_one()
        fingerprint = upload.resume_fingerprint

        matches = session.execute(
            select(JobMatch).where(
                JobMatch.resume_fingerprint == fingerprint,
                JobMatch.status == "active",
            )
        ).scalars().all()
        matched_job_ids = {str(match.job_post_id) for match in matches}
        assert seeded_jobs.positive_job_id in matched_job_ids, (
            f"Expected positive job {seeded_jobs.positive_job_id} in matches {matched_job_ids}"
        )

        positive_match = next(
            match for match in matches if str(match.job_post_id) == seeded_jobs.positive_job_id
        )
        preference_components = positive_match.preference_components or {}
        preference_reason_codes = preference_components.get("preference_reason_codes") or []

        assert (positive_match.preference_score or 0) > 0, (
            f"Expected preference_score > 0, got {positive_match.preference_score}; preference_components={preference_components}"
        )
        assert preference_components.get("preference_mode_used") == "semantic_rerank", preference_components
        assert "tech_stack_match" in preference_reason_codes, preference_reason_codes
        # CE path emits detail codes in "category:label|segment" format; LLM path does not.
        assert any("|" in code for code in preference_reason_codes), (
            f"Expected CE detail codes with '|' separator, got: {preference_reason_codes}"
        )
    finally:
        session.close()
        engine.dispose()


def test_resume_upload_failure_becomes_terminal_not_infinite_poll(split_stack: SplitStackContext):
    reset_split_stack_state(split_stack.database_url)

    upload_payload = _upload_resume(split_stack.base_url, FAIL_EMBEDDING_RESUME_FIXTURE)
    diagnostics = lambda: _stack_diagnostics(split_stack)
    resume_state = wait_for_resume_terminal(
        split_stack.base_url,
        upload_payload["task_id"],
        timeout_s=UPLOAD_TIMEOUT_SECONDS,
        diagnostics=diagnostics,
    )

    assert resume_state["status"] == "failed", resume_state
    assert resume_state.get("error"), resume_state


def test_notification_settings_round_trip_and_email_test_delivery(split_stack: SplitStackContext):
    reset_split_stack_state(split_stack.database_url)
    _reset_notification_state(split_stack.database_url)

    initial_settings = _get_notification_settings(split_stack.base_url)
    assert initial_settings["notifications_enabled"] is True
    assert initial_settings["channels"]["email"]["configured"] is True
    assert initial_settings["channels"]["email"]["enabled"] is False

    updated_settings = _update_notification_settings(
        split_stack.base_url,
        {
            "notifications_enabled": True,
            "min_fit_for_alerts": 88,
            "notify_on_new_match": False,
            "notify_on_batch_complete": True,
            "channels": {
                "email": {
                    "enabled": True,
                }
            },
        },
    )
    assert updated_settings["min_fit_for_alerts"] == 88
    assert updated_settings["notify_on_new_match"] is False
    assert updated_settings["channels"]["email"]["enabled"] is True
    assert updated_settings["revision"] >= initial_settings["revision"] + 1

    test_payload = _send_notification_settings_test(split_stack.base_url, "email")
    assert test_payload["success"] is True
    assert test_payload["notification_id"]

    terminal_settings = _wait_for_test_status(split_stack.base_url, "email", "sent")
    email_channel = terminal_settings["channels"]["email"]
    assert email_channel["last_test_status"] == "sent"
    assert email_channel["last_tested_at"] is not None
    assert email_channel["last_test_error"] is None

    engine, session = _session_for(split_stack.database_url)
    try:
        settings_channel = session.execute(
            select(UserNotificationChannel).where(
                UserNotificationChannel.owner_id == uuid.UUID(DEV_USER_ID),
                UserNotificationChannel.channel_type == "email",
            )
        ).scalar_one()
        assert settings_channel.enabled is True
        assert settings_channel.last_test_status == "sent"

        tracker_record = session.execute(
            select(NotificationTracker).where(
                NotificationTracker.owner_id == uuid.UUID(DEV_USER_ID),
                NotificationTracker.channel_type == "email",
                NotificationTracker.event_type == "settings_test",
            )
        ).scalar_one()
        assert tracker_record.sent_successfully is True
    finally:
        session.close()
        engine.dispose()
