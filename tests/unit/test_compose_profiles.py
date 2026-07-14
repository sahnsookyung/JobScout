from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]


class _ComposeLoader(yaml.SafeLoader):
    """Safe YAML loader with Compose's standard reset override tag."""


def _construct_reset_tag(loader: yaml.SafeLoader, node: yaml.Node) -> None:
    loader.construct_scalar(node)
    return None


_ComposeLoader.add_constructor("!reset", _construct_reset_tag)


def _load_compose(filename: str) -> dict:
    with open(REPO_ROOT / filename, "r", encoding="utf-8") as f:
        return yaml.load(f, Loader=_ComposeLoader)


def test_root_compose_uses_standardized_project_name() -> None:
    compose = _load_compose("docker-compose.yml")

    assert compose["name"] == "${COMPOSE_PROJECT_NAME:-jobscout}"


def test_microservices_have_no_profiles() -> None:
    compose = _load_compose("docker-compose.microservices.yml")
    services = compose["services"]

    for service_name in ("extraction", "embeddings", "scorer-matcher", "orchestrator", "notification-worker"):
        assert "profiles" not in services[service_name]


def test_orchestrator_has_downstream_service_urls() -> None:
    compose = _load_compose("docker-compose.microservices.yml")
    env = compose["services"]["orchestrator"]["environment"]

    assert any(str(entry).startswith("EXTRACTION_URL=") for entry in env)
    assert any(str(entry).startswith("EMBEDDINGS_URL=") for entry in env)


def test_microservices_all_depend_on_redis_healthy() -> None:
    compose = _load_compose("docker-compose.microservices.yml")
    services = compose["services"]

    for service in ("extraction", "embeddings", "scorer-matcher", "orchestrator"):
        depends_on = services[service].get("depends_on", {})
        assert "redis" in depends_on, f"{service} missing redis in depends_on"
        assert depends_on["redis"]["condition"] == "service_healthy", (
            f"{service} should wait for redis service_healthy, not just started"
        )


def test_e2e_scorer_bootstrap_skips_huggingface_download() -> None:
    compose = _load_compose("docker-compose.e2e.yml")
    service = compose["services"]["scorer-model-bootstrap"]

    command = "\n".join(str(part) for part in service["command"])

    assert "snapshot_download" not in command
    assert "skipped model download for E2E heuristic reranker" in command


def test_e2e_services_share_one_bounded_runtime_build() -> None:
    compose = _load_compose("docker-compose.e2e.yml")
    services = compose["services"]
    shared_image = "jobscout-e2e-runtime:latest"

    # Compose resolves YAML anchors before it merges override files, so a reset
    # inherited from this anchor would be overwritten by each base service.
    assert "build" not in compose["x-e2e-runtime"]
    assert services["db-migrate"]["image"] == shared_image
    assert services["db-migrate"]["build"]["dockerfile"] == "Dockerfile"
    for service_name in (
        "mock-llm",
        "web-backend",
        "extraction",
        "embeddings",
        "scorer-matcher",
        "scorer-model-bootstrap",
        "notification-worker",
        "orchestrator",
    ):
        service = services[service_name]
        assert service["image"] == shared_image
        assert service["build"] is None
        assert service["pull_policy"] == "never"
        assert service["command"]


def test_web_services_support_web_profile() -> None:
    compose = _load_compose("docker-compose.web.yml")
    services = compose["services"]

    for service_name in ("web-backend", "web-frontend"):
        profiles = services[service_name]["profiles"]
        assert "web" in profiles


def test_dev_overlay_only_overrides_existing_services() -> None:
    compose = _load_compose("docker-compose.dev.yml")
    services = compose["services"]

    assert set(services) == {
        "embeddings",
        "extraction",
        "orchestrator",
        "scorer-matcher",
        "web-backend",
    }
