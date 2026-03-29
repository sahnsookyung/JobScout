from pathlib import Path

from tests.integration.helpers.compose_env import ensure_compose_env_file
from tests.integration.test_split_stack_resume_flow import _compose_env, _compose_up_args


def test_ensure_compose_env_file_uses_existing_env(tmp_path: Path) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("EXISTING=1\n", encoding="utf-8")

    created = ensure_compose_env_file(tmp_path)

    assert created is False
    assert dotenv_path.read_text(encoding="utf-8") == "EXISTING=1\n"


def test_ensure_compose_env_file_copies_example(tmp_path: Path) -> None:
    dotenv_example_path = tmp_path / ".env.example"
    dotenv_example_path.write_text("FROM_EXAMPLE=1\n", encoding="utf-8")

    created = ensure_compose_env_file(tmp_path)

    assert created is True
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FROM_EXAMPLE=1\n"


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
