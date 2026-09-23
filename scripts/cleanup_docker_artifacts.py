"""Remove stale JobScout images and old Docker build cache safely.

The cleanup is intentionally conservative:

- only JobScout image references are eligible;
- images referenced by any container are preserved;
- the newest image for every JobScout repository is preserved;
- ``latest`` tags and Docker volumes are never removed;
- build cache is pruned by age, not wholesale.

Run without ``--apply`` to preview the cleanup plan.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

DEFAULT_MAX_AGE_HOURS = 168
JOBSCOUT_REPOSITORY_PREFIXES = (
    "ghcr.io/sahnsookyung/jobscout-",
    "jobscout-",
)


@dataclass(frozen=True)
class ImageRecord:
    image_id: str
    created_at: datetime
    repo_tags: tuple[str, ...]


@dataclass(frozen=True)
class CleanupPlan:
    image_references: tuple[str, ...]
    managed_image_count: int
    managed_repository_count: int


def _run_docker(
    arguments: Sequence[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "unknown Docker error"
        raise RuntimeError(f"Docker command failed: {message}")
    return result


def _load_images() -> list[ImageRecord]:
    image_ids_result = _run_docker(["image", "ls", "-aq", "--no-trunc"])
    image_ids = sorted(set(image_ids_result.stdout.split()))
    if not image_ids:
        return []

    inspect_result = _run_docker(["image", "inspect", *image_ids])
    payload = json.loads(inspect_result.stdout)
    return [
        ImageRecord(
            image_id=item["Id"],
            created_at=datetime.fromisoformat(item["Created"].replace("Z", "+00:00")),
            repo_tags=tuple(item.get("RepoTags") or ()),
        )
        for item in payload
    ]


def _load_container_image_ids() -> set[str]:
    container_ids_result = _run_docker(["container", "ls", "-aq", "--no-trunc"])
    container_ids = sorted(set(container_ids_result.stdout.split()))
    if not container_ids:
        return set()

    inspect_result = _run_docker(["container", "inspect", *container_ids])
    payload = json.loads(inspect_result.stdout)
    return {item["Image"] for item in payload}


def _split_reference(reference: str) -> tuple[str, str]:
    repository, separator, tag = reference.rpartition(":")
    if not separator or "/" in tag:
        return reference, ""
    return repository, tag


def _is_jobscout_repository(repository: str) -> bool:
    return repository.startswith(JOBSCOUT_REPOSITORY_PREFIXES)


def _build_cleanup_plan(
    images: Sequence[ImageRecord],
    container_image_ids: set[str],
    *,
    cutoff: datetime,
) -> CleanupPlan:
    managed_by_repository: dict[str, list[ImageRecord]] = {}
    managed_image_ids: set[str] = set()

    for image in images:
        for reference in image.repo_tags:
            repository, _ = _split_reference(reference)
            if not _is_jobscout_repository(repository):
                continue
            managed_by_repository.setdefault(repository, []).append(image)
            managed_image_ids.add(image.image_id)

    newest_image_by_repository = {
        repository: max(
            repository_images,
            key=lambda image: (image.created_at, image.image_id),
        ).image_id
        for repository, repository_images in managed_by_repository.items()
    }

    removable_references: set[str] = set()
    for image in images:
        if image.created_at >= cutoff or image.image_id in container_image_ids:
            continue

        for reference in image.repo_tags:
            repository, tag = _split_reference(reference)
            if not _is_jobscout_repository(repository):
                continue
            if tag == "latest":
                continue
            if newest_image_by_repository.get(repository) == image.image_id:
                continue
            removable_references.add(reference)

    return CleanupPlan(
        image_references=tuple(sorted(removable_references)),
        managed_image_count=len(managed_image_ids),
        managed_repository_count=len(managed_by_repository),
    )


def _remove_images(image_references: Sequence[str]) -> list[str]:
    failures: list[str] = []
    for reference in image_references:
        result = _run_docker(["image", "rm", reference], check=False)
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip() or "unknown Docker error"
            failures.append(f"{reference}: {message}")
    return failures


def _prune_build_cache(max_age_hours: int) -> Optional[str]:
    result = _run_docker(
        [
            "builder",
            "prune",
            "--all",
            "--force",
            "--filter",
            f"until={max_age_hours}h",
        ],
        check=False,
    )
    if result.returncode == 0:
        return None
    return result.stderr.strip() or result.stdout.strip() or "unknown Docker error"


def _parse_args(arguments: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove stale JobScout Docker images and age-expired build cache.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the cleanup. Without this flag, only print the plan.",
    )
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Retain images and build cache newer than this (default: {DEFAULT_MAX_AGE_HOURS}).",
    )
    parser.add_argument(
        "--skip-build-cache",
        action="store_true",
        help="Do not prune age-expired Docker build cache.",
    )
    parsed = parser.parse_args(arguments)
    if parsed.max_age_hours <= 0:
        parser.error("--max-age-hours must be greater than zero")
    return parsed


def main(arguments: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(arguments)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.max_age_hours)

    try:
        images = _load_images()
        container_image_ids = _load_container_image_ids()
    except (json.JSONDecodeError, OSError, RuntimeError, ValueError) as exc:
        print(f"Docker cleanup could not inspect the daemon: {exc}", file=sys.stderr)
        return 2

    plan = _build_cleanup_plan(
        images,
        container_image_ids,
        cutoff=cutoff,
    )
    mode = "Applying" if args.apply else "Previewing"
    print(
        f"{mode} JobScout Docker cleanup: "
        f"{plan.managed_image_count} images across "
        f"{plan.managed_repository_count} repositories."
    )

    if plan.image_references:
        action = "Removing" if args.apply else "Would remove"
        print(f"{action} {len(plan.image_references)} stale image references:")
        for reference in plan.image_references:
            print(f"  {reference}")
    else:
        print("No stale JobScout image references are eligible for removal.")

    if not args.skip_build_cache:
        action = "Pruning" if args.apply else "Would prune"
        print(f"{action} unused build cache older than {args.max_age_hours} hours.")

    if not args.apply:
        print("Re-run with --apply to perform this cleanup.")
        return 0

    failures = _remove_images(plan.image_references)
    cache_failure = None
    if not args.skip_build_cache:
        cache_failure = _prune_build_cache(args.max_age_hours)

    if failures or cache_failure:
        for failure in failures:
            print(f"Image cleanup warning: {failure}", file=sys.stderr)
        if cache_failure:
            print(f"Build-cache cleanup warning: {cache_failure}", file=sys.stderr)
        return 1

    print("JobScout Docker cleanup completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
