from datetime import datetime, timedelta, timezone

from scripts.cleanup_docker_artifacts import (
    ImageRecord,
    _build_cleanup_plan,
    _prune_build_cache,
)


def _image(
    image_id: str,
    created_at: datetime,
    *repo_tags: str,
) -> ImageRecord:
    return ImageRecord(
        image_id=image_id,
        created_at=created_at,
        repo_tags=tuple(repo_tags),
    )


def test_cleanup_plan_removes_only_old_superseded_jobscout_images() -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    old = now - timedelta(days=14)
    recent = now - timedelta(days=1)
    images = [
        _image(
            "sha256:old",
            old,
            "ghcr.io/sahnsookyung/jobscout-orchestrator:old-sha",
        ),
        _image(
            "sha256:new",
            recent,
            "ghcr.io/sahnsookyung/jobscout-orchestrator:new-sha",
            "ghcr.io/sahnsookyung/jobscout-orchestrator:latest",
        ),
        _image("sha256:other", old, "postgres:17"),
    ]

    plan = _build_cleanup_plan(images, set(), cutoff=cutoff)

    assert plan.image_references == (
        "ghcr.io/sahnsookyung/jobscout-orchestrator:old-sha",
    )
    assert plan.managed_image_count == 2
    assert plan.managed_repository_count == 1


def test_cleanup_plan_preserves_container_images_and_latest_tags() -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    old = now - timedelta(days=14)
    images = [
        _image(
            "sha256:used",
            old,
            "ghcr.io/sahnsookyung/jobscout-extraction:used-sha",
        ),
        _image(
            "sha256:latest",
            old,
            "ghcr.io/sahnsookyung/jobscout-extraction:latest",
        ),
    ]

    plan = _build_cleanup_plan(
        images,
        {"sha256:used"},
        cutoff=cutoff,
    )

    assert plan.image_references == ()


def test_cleanup_plan_preserves_newest_image_per_repository_when_all_are_old() -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    older = now - timedelta(days=21)
    newest = now - timedelta(days=14)
    images = [
        _image("sha256:older", older, "jobscout-e2e-web-frontend:previous"),
        _image("sha256:newest", newest, "jobscout-e2e-web-frontend:current"),
    ]

    plan = _build_cleanup_plan(images, set(), cutoff=cutoff)

    assert plan.image_references == ("jobscout-e2e-web-frontend:previous",)


def test_build_cache_cleanup_is_age_limited_and_includes_all_unused_cache(
    monkeypatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(arguments, *, check=True):
        del check
        commands.append(list(arguments))

        class _Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Result()

    monkeypatch.setattr("scripts.cleanup_docker_artifacts._run_docker", fake_run)

    assert _prune_build_cache(168) is None
    assert commands == [
        ["builder", "prune", "--all", "--force", "--filter", "until=168h"]
    ]
