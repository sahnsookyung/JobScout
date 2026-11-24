#!/usr/bin/env python3
"""Import only the sealed JobScout commit objects; never create or update refs."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.request


REPOSITORY = "sahnsookyung/JobScout"
REF = "refs/heads/automation/dependabot-import-2026-11-23"
API_URL = "https://api.github.com/repos/sahnsookyung/JobScout/git/commits"
ORIGINAL_PRS = frozenset(range(432, 447))
MERGE_COUNT = 15
COMMIT_COUNT = 16
EXTERNAL_PARENT_COUNT = 15
REVIEW_DATE = "2026-11-24"
MAX_MANIFEST_BYTES = 512 * 1024
MAX_POLICY_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
SHA_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
DATE_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
POLICY_FIELDS = {
    "schema_version", "manifest_sha256", "ordered_shas", "base_sha", "final_sha",
    "allowed_timestamps", "external_parents", "repository", "ref",
}
COMMIT_FIELDS = {"expected_sha", "tree", "parents", "message", "author", "committer"}
IDENTITY_FIELDS = {"name", "email", "date"}


class ImportRejected(Exception):
    """A fixed, non-sensitive diagnostic safe to print."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ImportRejected(message)


def exact_keys(value: object, fields: set[str], context: str) -> None:
    require(type(value) is dict and set(value) == fields, context + " has invalid fields")


def sha(value: object) -> bool:
    return type(value) is str and SHA_PATTERN.fullmatch(value) is not None


def timestamp(value: object) -> int:
    require(type(value) is str and DATE_PATTERN.fullmatch(value) is not None, "invalid UTC date")
    try:
        instant = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        raise ImportRejected("invalid UTC date") from None
    return int((instant - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)).total_seconds())


def text_value(value: object, limit: int, context: str) -> None:
    require(type(value) is str and bool(value), context + " must be nonempty text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise ImportRejected(context + " is not valid UTF-8") from None
    require(len(encoded) <= limit, context + " exceeds its byte limit")


def validate_identity(value: object, expected_date: str) -> None:
    exact_keys(value, IDENTITY_FIELDS, "identity")
    text_value(value["name"], 200, "identity name")
    text_value(value["email"], 254, "identity email")
    require(value["name"] == value["name"].strip()
            and not any(ord(c) < 32 or ord(c) == 127 or c in "<>" for c in value["name"]),
            "invalid identity name")
    require(re.fullmatch(r"[^\s<>@]+@[^\s<>@]+", value["email"]) is not None
            and not any(ord(c) < 32 or ord(c) == 127 for c in value["email"]),
            "invalid identity email")
    timestamp(value["date"])
    require(value["date"] == expected_date, "identity date differs from the sealed policy")


def commit_sha(commit: dict) -> str:
    lines = ["tree " + commit["tree"]]
    lines.extend("parent " + parent for parent in commit["parents"])
    for role in ("author", "committer"):
        person = commit[role]
        lines.append(f"{role} {person['name']} <{person['email']}> {timestamp(person['date'])} +0000")
    body = ("\n".join(lines) + "\n\n" + commit["message"]).encode("utf-8")
    return hashlib.sha1(b"commit " + str(len(body)).encode("ascii") + b"\0" + body).hexdigest()


def no_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def reject_json_constant(value: str) -> None:
    raise ImportRejected("non-finite JSON number")


def decode_json(raw: bytes, limit: int, context: str) -> object:
    require(type(raw) is bytes and 0 < len(raw) <= limit, context + " exceeds its byte limit or is empty")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=no_duplicate_keys,
                          parse_constant=reject_json_constant)
    except (UnicodeError, ValueError, RecursionError):
        raise ImportRejected(context + " is not valid JSON") from None


def read_bounded(path: Path, limit: int) -> bytes:
    try:
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
    except OSError:
        raise ImportRejected("could not read an input file") from None
    require(0 < len(raw) <= limit, "input file exceeds its byte limit or is empty")
    return raw


def trailers(message: str, merge: bool, expected_date: str) -> int | None:
    required = {"Original-PR", "Dependabot-Submitted-At", "Review-Date"} if merge else {"Review-Date"}
    parts = message.rstrip("\n").rsplit("\n\n", 1)
    require(len(parts) == 2, "commit is missing review trailers")
    fields = {}
    for line in parts[1].split("\n"):
        key, separator, value = line.partition(": ")
        require(bool(separator) and key in required and key not in fields, "invalid or duplicate review trailer")
        fields[key] = value
    require(set(fields) == required, "commit is missing review trailers")
    for key in required:
        require(sum(line.startswith(key + ":") for line in message.splitlines()) == 1,
                "duplicate review trailer")
    if not merge:
        require(fields["Review-Date"] == REVIEW_DATE, "invalid corrective review date")
        return None
    match = re.fullmatch(r"https://github\.com/sahnsookyung/JobScout/pull/([1-9][0-9]*)", fields["Original-PR"])
    require(match is not None, "invalid original PR trailer")
    number = int(match.group(1))
    require(re.fullmatch(rf"Merge Dependabot PR #{number}: .+", message.splitlines()[0]) is not None,
            "merge title and original PR do not match")
    require(fields["Dependabot-Submitted-At"] == expected_date, "submission date differs from commit date")
    review_date = REVIEW_DATE
    require(fields["Review-Date"] == review_date, "invalid merge review date")
    return number


def validate(manifest_raw: bytes, policy_raw: bytes, policy_sha256: str) -> list[dict]:
    require(type(policy_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", policy_sha256) is not None,
            "invalid policy seal")
    require(hashlib.sha256(policy_raw).hexdigest() == policy_sha256, "policy seal does not match")
    policy = decode_json(policy_raw, MAX_POLICY_BYTES, "policy")
    exact_keys(policy, POLICY_FIELDS, "policy")
    require(type(policy["schema_version"]) is int and policy["schema_version"] == 1, "unsupported policy schema")
    require(policy["repository"] == REPOSITORY and policy["ref"] == REF, "repository or ref is outside the approved scope")
    require(type(policy["manifest_sha256"]) is str
            and hashlib.sha256(manifest_raw).hexdigest() == policy["manifest_sha256"], "manifest seal does not match")
    ordered = policy["ordered_shas"]
    external = policy["external_parents"]
    dates = policy["allowed_timestamps"]
    require(type(ordered) is list and len(ordered) == COMMIT_COUNT and all(sha(x) for x in ordered),
            "invalid ordered commit list")
    require(len(set(ordered)) == COMMIT_COUNT, "duplicate commit SHA")
    require(sha(policy["base_sha"]) and policy["base_sha"] not in ordered, "invalid base SHA")
    require(policy["final_sha"] == ordered[-1], "final SHA differs from the ordered history")
    require(type(external) is list and len(external) == EXTERNAL_PARENT_COUNT and all(sha(x) for x in external),
            "invalid external parent list")
    require(len(set(external)) == EXTERNAL_PARENT_COUNT
            and not set(external).intersection(ordered + [policy["base_sha"]]), "duplicate or internal external parent")
    require(type(dates) is dict and set(dates) == set(ordered), "date policy does not cover exactly the ordered commits")
    for value in dates.values():
        timestamp(value)
    commits = decode_json(manifest_raw, MAX_MANIFEST_BYTES, "manifest")
    require(type(commits) is list and len(commits) == COMMIT_COUNT, "invalid manifest commit count")
    previous = policy["base_sha"]
    seen_external, seen_prs, original_dates, corrective_dates = set(), set(), set(), set()
    for index, commit in enumerate(commits):
        exact_keys(commit, COMMIT_FIELDS, "commit")
        require(commit["expected_sha"] == ordered[index] and sha(commit["tree"]), "commit SHA order or tree is invalid")
        parents = commit["parents"]
        merge = index < MERGE_COUNT
        require(type(parents) is list and len(parents) == (2 if merge else 1)
                and all(sha(x) for x in parents), "invalid parent count or SHA")
        require(parents[0] == previous, "first-parent chain differs from the sealed history")
        if merge:
            require(parents[1] in external and parents[1] not in seen_external, "unapproved or duplicate original PR parent")
            seen_external.add(parents[1])
        expected_date = dates[commit["expected_sha"]]
        validate_identity(commit["author"], expected_date)
        validate_identity(commit["committer"], expected_date)
        text_value(commit["message"], 8192, "commit message")
        require(commit["message"].endswith("\n") and "\r" not in commit["message"]
                and not any(ord(c) < 32 and c not in "\n\t" for c in commit["message"]), "invalid commit message encoding or final LF")
        number = trailers(commit["message"], merge, expected_date)
        if merge:
            require(number not in seen_prs, "duplicate original PR")
            require(number == 432 + index and number in ORIGINAL_PRS,
                    "PR is outside the approved history segment")
            seen_prs.add(number)
            original_dates.add(expected_date)
        else:
            corrective_dates.add(expected_date)
        require(commit_sha(commit) == commit["expected_sha"], "raw Git commit SHA does not match")
        previous = commit["expected_sha"]
    require(seen_external == set(external) and ORIGINAL_PRS == seen_prs, "original PR ancestry is incomplete")
    require(corrective_dates.issubset(original_dates), "corrective date is not an original Dependabot submission date")
    return commits


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ImportRejected("API redirects are forbidden")


def creation_token() -> str:
    required = {
        "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push", "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_REF": REF, "GITHUB_SERVER_URL": "https://github.com", "GITHUB_API_URL": "https://api.github.com",
    }
    require(all(os.environ.get(key) == value for key, value in required.items()), "GitHub execution context is outside the approved scope")
    token = os.environ.get("GITHUB_TOKEN", "")
    require(re.fullmatch(r"[A-Za-z0-9_.-]{20,4096}", token) is not None, "missing or invalid environment token")
    return token


def post_commit(commit: dict, token: str) -> None:
    payload = {key: commit[key] for key in ("tree", "parents", "message", "author", "committer")}
    request = urllib.request.Request(API_URL, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), method="POST",
                                     headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                              "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28",
                                              "User-Agent": "JobScout-sealed-commit-import"})
    try:
        with urllib.request.build_opener(RejectRedirects()).open(request, timeout=30) as response:
            require(response.status == 201 and response.geturl() == API_URL, "unexpected API status or response URL")
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except ImportRejected:
        raise
    except Exception:
        raise ImportRejected("commit-object API request failed; no further objects were requested") from None
    result = decode_json(raw, MAX_RESPONSE_BYTES, "API response")
    require(type(result) is dict and result.get("sha") == commit["expected_sha"], "API returned an unexpected commit SHA")
    require(type(result.get("tree")) is dict and result["tree"].get("sha") == commit["tree"], "API returned an unexpected tree")
    actual_parents = result.get("parents")
    require(type(actual_parents) is list and all(type(parent) is dict for parent in actual_parents)
            and [parent.get("sha") for parent in actual_parents] == commit["parents"], "API returned unexpected parents")
    # GitHub's JSON presentation may omit one final LF; the raw object SHA must still match above.
    expected_message = commit["message"]
    require(result.get("message") == expected_message
            or (expected_message.endswith("\n") and result.get("message") == expected_message[:-1]),
            "API returned an unexpected commit message")
    for role in ("author", "committer"):
        require(type(result.get(role)) is dict
                and all(result[role].get(key) == commit[role][key] for key in IDENTITY_FIELDS), "API returned an unexpected identity or date")


def run(manifest: Path, policy: Path, policy_sha256: str, create_objects: bool = False) -> int:
    commits = validate(read_bounded(manifest, MAX_MANIFEST_BYTES), read_bounded(policy, MAX_POLICY_BYTES), policy_sha256)
    if create_objects:
        token = creation_token()
        for commit in commits:
            post_commit(commit, token)
        print(f"Created and verified {len(commits)} commit objects. No refs were updated.")
    else:
        print(f"Validated {len(commits)} commit objects. No network requests were made.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--policy-sha256", required=True, help="independently reviewed policy SHA-256; never compute it at runtime")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--validate-only", action="store_true", help="validate without network access (default)")
    mode.add_argument("--create-objects", action="store_true", help="explicitly create only the sealed commit objects")
    args = parser.parse_args(argv)
    try:
        return run(args.manifest, args.policy, args.policy_sha256, args.create_objects)
    except ImportRejected as error:
        print("Import rejected: " + str(error), file=sys.stderr)
        return 1
    except Exception:
        print("Import rejected: unexpected local failure", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
