from pathlib import Path


SERVICE_DIRS = [
    Path("/Users/sookyungahn/repos/JobScout/services/extraction"),
    Path("/Users/sookyungahn/repos/JobScout/services/embeddings"),
    Path("/Users/sookyungahn/repos/JobScout/services/scorer_matcher"),
    Path("/Users/sookyungahn/repos/JobScout/services/orchestrator"),
]


def test_microservices_do_not_import_web_backend_package() -> None:
    violations: list[str] = []

    for service_dir in SERVICE_DIRS:
        for py_file in service_dir.rglob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            if "web.backend" in text:
                violations.append(str(py_file))

    assert violations == []
