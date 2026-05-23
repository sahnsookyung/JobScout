from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.security
def test_resume_variants_do_not_add_heavy_renderer_or_local_model_dependencies() -> None:
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()
    forbidden = ["playwright", "chromium", "browserless", "libreoffice", "ollama"]

    for dependency in forbidden:
        assert dependency not in pyproject


def test_resume_variants_do_not_add_docker_compose_service_or_binary_volume() -> None:
    compose_text = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in PROJECT_ROOT.glob("docker-compose*.yml")
    )

    assert "resume-variant" not in compose_text
    assert "resume_variant" not in compose_text
