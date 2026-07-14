import subprocess
from pathlib import Path

import pytest

from tests.integration.helpers.compose_env import ensure_compose_env_file
from tests.integration.test_microservices_resume_flow import (
    _ACTIVE_E2E_CLEANUP,
    _cleanup_active_e2e,
    _clear_active_e2e_cleanup,
    _compose_images_available,
    _compose_down,
    _compose_project_volume_names,
    _compose_up_with_retries,
    _compose_env,
    _compose_up_args,
    _env_flag,
    _register_active_e2e_cleanup,
    _resolve_build_images,
)


def test_ensure_compose_env_file_uses_existing_env(tmp_path: Path) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("EXISTING=1\n", encoding="utf-8")

    created = ensure_compose_env_file(tmp_path)

    assert created is False
    assert dotenv_path.read_text(encoding="utf-8") == "EXISTING=1\n"


def test_ensure_compose_env_file_ignores_example_values_for_e2e(tmp_path: Path) -> None:
    dotenv_example_path = tmp_path / ".env.example"
    dotenv_example_path.write_text("FROM_EXAMPLE=1\n", encoding="utf-8")

    created = ensure_compose_env_file(tmp_path)

    assert created is True
    assert (tmp_path / ".env").read_text(encoding="utf-8") == ""


def test_ensure_compose_env_file_creates_empty_file_when_no_example(tmp_path: Path) -> None:
    created = ensure_compose_env_file(tmp_path)

    assert created is True
    assert (tmp_path / ".env").read_text(encoding="utf-8") == ""


def test_compose_up_args_include_build_by_default() -> None:
    args = _compose_up_args(("web-backend",), build_images=True)

    assert "--build" in args
    assert "--no-build" not in args


def test_compose_up_args_support_skip_build_mode() -> None:
    args = _compose_up_args(("web-backend",), build_images=False)

    assert "--no-build" in args
    assert "--build" not in args


def test_compose_env_reserves_mailpit_ports() -> None:
    env = _compose_env()

    assert env["MAILPIT_SMTP_PORT"].isdigit()
    assert env["MAILPIT_UI_PORT"].isdigit()
    assert env["COMPOSE_PROJECT_NAME"] == "jobscout-e2e"
    assert env["JOBSPY_API_TOKEN"] == "jobscout-e2e-internal-token"
    assert env["WEB_BACKEND_CONTAINER_NAME"] == "jobscout-e2e-web-backend"


def test_env_flag_parses_bool_values(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_E2E_BUILD_IMAGES", "true")
    assert _env_flag("JOBSCOUT_E2E_BUILD_IMAGES") is True

    monkeypatch.setenv("JOBSCOUT_E2E_BUILD_IMAGES", "0")
    assert _env_flag("JOBSCOUT_E2E_BUILD_IMAGES") is False


def test_env_flag_rejects_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_E2E_BUILD_IMAGES", "maybe")

    with pytest.raises(ValueError):
        _env_flag("JOBSCOUT_E2E_BUILD_IMAGES")


def test_compose_images_available_returns_true_when_all_images_exist(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}

    class _Result:
        def __init__(self, *, returncode: int = 0, stdout: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._run_compose",
        lambda *args, **kwargs: _Result(stdout="svc-a:latest\nsvc-b:latest\n"),
    )
    commands: list[list[str]] = []

    def fake_run(command, *args, **kwargs):
        commands.append(command)
        return _Result(returncode=0)

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow.subprocess.run",
        fake_run,
    )

    assert _compose_images_available(compose_args, compose_env) is True
    assert any(command[:3] == ["docker", "image", "inspect"] for command in commands)


def test_compose_images_available_returns_false_when_runtime_image_is_stale(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}

    class _Result:
        def __init__(self, *, returncode: int = 0, stdout: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._run_compose",
        lambda *args, **kwargs: _Result(stdout="jobscout-orchestrator:latest\n"),
    )

    def fake_run(command, *args, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return _Result(returncode=0)
        if command[:4] == ["docker", "run", "--rm", "--entrypoint"]:
            return _Result(returncode=1)
        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow.subprocess.run",
        fake_run,
    )

    assert _compose_images_available(compose_args, compose_env) is False


def test_resolve_build_images_prefers_cached_images(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    monkeypatch.delenv("JOBSCOUT_E2E_BUILD_IMAGES", raising=False)
    monkeypatch.delenv("JOBSCOUT_E2E_SKIP_BUILD", raising=False)
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_images_available",
        lambda *args, **kwargs: True,
    )

    assert _resolve_build_images(compose_args, compose_env) is False


def test_resolve_build_images_honors_skip_build_env_when_images_exist(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    monkeypatch.delenv("JOBSCOUT_E2E_BUILD_IMAGES", raising=False)
    monkeypatch.setenv("JOBSCOUT_E2E_SKIP_BUILD", "1")
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_images_available",
        lambda *args, **kwargs: True,
    )

    assert _resolve_build_images(compose_args, compose_env) is False

def test_resolve_build_images_builds_when_skip_build_images_are_missing(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    monkeypatch.delenv("JOBSCOUT_E2E_BUILD_IMAGES", raising=False)
    monkeypatch.setenv("JOBSCOUT_E2E_SKIP_BUILD", "1")
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_images_available",
        lambda *args, **kwargs: False,
    )

    assert _resolve_build_images(compose_args, compose_env) is True


def test_resolve_build_images_prefers_explicit_build_override(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    monkeypatch.setenv("JOBSCOUT_E2E_BUILD_IMAGES", "1")
    monkeypatch.setenv("JOBSCOUT_E2E_SKIP_BUILD", "1")
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_images_available",
        lambda *args, **kwargs: True,
    )

    assert _resolve_build_images(compose_args, compose_env) is True


def test_active_e2e_cleanup_runs_down_and_clears_state(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}

    def fake_compose_down(args: tuple[str, ...], env: dict[str, str]) -> None:
        calls.append((args, env))

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_down",
        fake_compose_down,
    )
    _clear_active_e2e_cleanup()
    _register_active_e2e_cleanup(compose_args, compose_env)

    _cleanup_active_e2e()

    assert calls == [(compose_args, compose_env)]
    assert _ACTIVE_E2E_CLEANUP == {}


def test_compose_down_force_removes_leftover_project_containers(monkeypatch) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    commands: list[list[str]] = []

    class _Result:
        def __init__(self, returncode: int = 0) -> None:
            self.returncode = returncode

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._run_compose",
        lambda *args, **kwargs: _Result(),
    )
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_project_container_ids",
        lambda project_name: ["abc123"] if not commands else [],
    )
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_project_volume_names",
        lambda project_name: ["jobscout-e2e_resume_uploads"],
    )

    def fake_run(command: list[str], **kwargs) -> _Result:
        commands.append(command)
        return _Result()

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow.subprocess.run",
        fake_run,
    )

    _compose_down(compose_args, compose_env)

    assert commands == [
        ["docker", "rm", "-f", "-v", "abc123"],
        ["docker", "volume", "rm", "jobscout-e2e_resume_uploads"],
        ["docker", "network", "rm", "jobscout-e2e_default"],
    ]


def test_compose_project_volume_names_uses_project_label(monkeypatch) -> None:
    commands: list[list[str]] = []

    class _Result:
        returncode = 0
        stdout = "jobscout-e2e_resume_uploads\n"

    def fake_run(command: list[str], **kwargs) -> _Result:
        commands.append(command)
        return _Result()

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow.subprocess.run",
        fake_run,
    )

    assert _compose_project_volume_names("jobscout-e2e") == [
        "jobscout-e2e_resume_uploads"
    ]
    assert commands == [
        [
            "docker",
            "volume",
            "ls",
            "--filter",
            "label=com.docker.compose.project=jobscout-e2e",
            "--format",
            "{{.Name}}",
        ]
    ]


def test_compose_up_with_retries_prints_diagnostics_before_raising(monkeypatch, capsys) -> None:
    compose_args = ("docker", "compose", "-p", "jobscout-e2e")
    compose_env = {"WEB_BACKEND_PORT": "12345"}
    seen_commands: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._next_compose_env",
        lambda: compose_env,
    )

    def fake_run_compose(
        args: tuple[str, ...],
        env: dict[str, str],
        *command: str,
        **kwargs,
    ):
        del args, env, kwargs
        seen_commands.append(tuple(command))
        if command[:3] == ("ps", "-a", "--format"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"Name":"jobscout-e2e-db-migrate-1","State":"exited","ExitCode":1}\n',
                stderr="",
            )
        if command[:2] == ("logs", "--no-color"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="db-migrate | Traceback: boom\n",
                stderr="",
            )
        raise subprocess.CalledProcessError(
            1,
            command,
            output="",
            stderr='service "db-migrate" didn\'t complete successfully: exit 1',
        )

    compose_down_calls: list[dict[str, str]] = []
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._run_compose",
        fake_run_compose,
    )
    monkeypatch.setattr(
        "tests.integration.test_microservices_resume_flow._compose_down",
        lambda args, env: compose_down_calls.append(env),
    )

    with pytest.raises(AssertionError) as exc_info:
        _compose_up_with_retries(
            compose_args,
            ("db-migrate",),
            build_images=False,
            attempts=1,
        )

    captured = capsys.readouterr()
    assert "=== compose ps ===" in captured.err
    assert "Traceback: boom" in captured.err
    assert "service \"db-migrate\" didn't complete successfully" in str(exc_info.value)
    assert compose_down_calls == [compose_env, compose_env]
    assert ("ps", "-a", "--format", "json") in seen_commands
    assert ("logs", "--no-color", "db-migrate", "postgres") in seen_commands
