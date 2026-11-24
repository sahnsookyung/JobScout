"""Offline tests: no real HTTP requests, git writes, credentials, or refs."""

import contextlib
import copy
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.request

import import_commits as subject


TOKEN = "offline_test_secret_token_123456789"
CONTEXT = {
    "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push",
    "GITHUB_REPOSITORY": subject.REPOSITORY, "GITHUB_REF": subject.REF,
    "GITHUB_SERVER_URL": "https://github.com", "GITHUB_API_URL": "https://api.github.com",
    "GITHUB_TOKEN": TOKEN,
}


def fixture():
    commits, external = [], []
    previous = "a" * 40
    original = sorted(subject.ORIGINAL_PRS)
    sequence = original + [None]
    start = dt.datetime(2026, 11, 16, tzinfo=dt.timezone.utc)
    for index, number in enumerate(sequence):
        date = (start + dt.timedelta(minutes=index if number else 0)).strftime("%Y-%m-%dT%H:%M:%SZ")
        parents = [previous]
        if number:
            parent = hashlib.sha1(f"original PR {number}".encode()).hexdigest()
            parents.append(parent)
            external.append(parent)
            review = subject.REVIEW_DATE
            message = (f"Merge Dependabot PR #{number}: dependency update\n\n"
                       f"Original-PR: https://github.com/{subject.REPOSITORY}/pull/{number}\n"
                       f"Dependabot-Submitted-At: {date}\nReview-Date: {review}\n")
        else:
            message = f"Corrective change {index}\n\nReview-Date: {subject.REVIEW_DATE}\n"
        person = {"name": "Example Author", "email": "author@example.com", "date": date}
        commit = {"tree": "b" * 40, "parents": parents, "message": message,
                  "author": person, "committer": dict(person)}
        commit["expected_sha"] = subject.commit_sha(commit)
        commits.append(commit)
        previous = commit["expected_sha"]
    policy = {
        "schema_version": 1, "manifest_sha256": "", "repository": subject.REPOSITORY, "ref": subject.REF,
        "base_sha": "a" * 40, "final_sha": commits[-1]["expected_sha"],
        "ordered_shas": [c["expected_sha"] for c in commits], "external_parents": external,
        "allowed_timestamps": {c["expected_sha"]: c["author"]["date"] for c in commits},
    }
    return commits, policy


def seal(commits, policy):
    raw = json.dumps(commits, ensure_ascii=False, indent=2).encode() + b"\n"
    policy = copy.deepcopy(policy)
    policy["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
    policy_raw = json.dumps(policy, sort_keys=True, indent=2).encode() + b"\n"
    return raw, policy_raw, hashlib.sha256(policy_raw).hexdigest()


class Response:
    status = 201

    def __init__(self, commit):
        self.url = subject.API_URL
        self.data = {"sha": commit["expected_sha"], "tree": {"sha": commit["tree"]},
                     "parents": [{"sha": p} for p in commit["parents"]],
                     "message": commit["message"], "author": commit["author"], "committer": commit["committer"]}

    def read(self, limit):
        return json.dumps(self.data).encode()[:limit]

    def geturl(self):
        return self.url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.commits, self.policy = fixture()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest_path = Path(self.temp.name) / "manifest.json"
        self.policy_path = Path(self.temp.name) / "policy.json"
        self.network = mock.patch.object(urllib.request.OpenerDirector, "open", side_effect=AssertionError("real network forbidden"))
        self.network_mock = self.network.start()
        self.addCleanup(self.network.stop)
        self.write_inputs()

    def write_inputs(self):
        self.raw, self.policy_raw, self.digest = seal(self.commits, self.policy)
        self.manifest_path.write_bytes(self.raw)
        self.policy_path.write_bytes(self.policy_raw)

    def cli_args(self):
        return ["--manifest", str(self.manifest_path), "--policy", str(self.policy_path), "--policy-sha256", self.digest]

    def rejected_without_requests(self):
        self.write_inputs()
        with mock.patch.dict(os.environ, CONTEXT, clear=True), mock.patch.object(subject, "post_commit") as post:
            with self.assertRaises(subject.ImportRejected):
                subject.run(self.manifest_path, self.policy_path, self.digest, create_objects=True)
            post.assert_not_called()
        self.network_mock.assert_not_called()

    def test_known_native_git_commit_hash_and_final_lf(self):
        commit = {"tree": "1" * 40, "parents": ["2" * 40], "message": "Preserve final LF\n",
                  "author": {"name": "Example Author", "email": "author@example.com", "date": "1970-01-01T00:00:00Z"},
                  "committer": {"name": "Example Committer", "email": "committer@example.com", "date": "1970-01-01T00:00:00Z"}}
        # Independently obtained with git hash-object -t commit --stdin.
        self.assertEqual(subject.commit_sha(commit), "af6625c27ea6715584dd8c03c01ead7a8d51e01e")
        commit["message"] = commit["message"].rstrip("\n")
        self.assertNotEqual(subject.commit_sha(commit), "af6625c27ea6715584dd8c03c01ead7a8d51e01e")

    def test_default_and_explicit_validation_never_require_token_or_network(self):
        for flag in ([], ["--validate-only"]):
            with self.subTest(flag=flag), mock.patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(subject.main(self.cli_args() + flag), 0)
        self.network_mock.assert_not_called()

    def test_both_modes_are_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            subject.main(self.cli_args() + ["--validate-only", "--create-objects"])
        self.network_mock.assert_not_called()

    def test_policy_seal_binds_policy_bytes(self):
        with self.assertRaises(subject.ImportRejected):
            subject.validate(self.raw, self.policy_raw + b" ", self.digest)

    def test_manifest_seal_binds_manifest_bytes(self):
        with self.assertRaises(subject.ImportRejected):
            subject.validate(self.raw + b" ", self.policy_raw, self.digest)

    def test_entire_manifest_validated_before_first_request(self):
        self.commits[-1]["author"]["date"] = "2026-11-24T00:00:00Z"
        self.rejected_without_requests()

    def test_no_arbitrary_fields_or_missing_dates(self):
        for mutation in (lambda: self.commits[-1].update(signature="unapproved"),
                         lambda: self.commits[-1]["committer"].pop("date"),
                         lambda: self.policy.update(endpoint="https://example.com")):
            with self.subTest(mutation=mutation):
                self.commits, self.policy = fixture()
                mutation()
                self.rejected_without_requests()

    def test_wrong_scope_count_or_order(self):
        mutations = [lambda: self.policy.update(repository="other/repo"),
                     lambda: self.policy.update(ref="refs/heads/main"),
                     lambda: self.commits.pop(),
                     lambda: self.policy["ordered_shas"].reverse(),
                     lambda: self.policy.update(final_sha="c" * 40),
                     lambda: self.policy["ordered_shas"].__setitem__(1, self.policy["ordered_shas"][0])]
        for mutation in mutations:
            self.commits, self.policy = fixture()
            mutation()
            with self.subTest(mutation=mutation):
                self.rejected_without_requests()

    def test_parent_ancestry_cannot_be_changed_or_reused(self):
        mutations = [lambda: self.commits[-1]["parents"].__setitem__(0, "f" * 40),
                     lambda: self.commits[-2]["parents"].__setitem__(1, "f" * 40),
                     lambda: self.commits[-2]["parents"].__setitem__(1, self.commits[0]["parents"][1]),
                     lambda: self.commits[15]["parents"].append("f" * 40)]
        for mutation in mutations:
            self.commits, self.policy = fixture()
            mutation()
            with self.subTest(mutation=mutation):
                self.rejected_without_requests()

    def test_dates_names_and_message_are_strict(self):
        mutations = [lambda: self.commits[-1]["author"].update(date="2026-11-16T00:38:00+00:00"),
                     lambda: self.commits[-1]["author"].update(name="Name\ncommitter injected"),
                     lambda: self.commits[-1]["author"].update(email="invalid email"),
                     lambda: self.commits[-1].update(tree="B" * 40),
                     lambda: self.commits[-1].update(message=self.commits[-1]["message"].rstrip("\n")),
                     lambda: self.commits[-1].update(message=self.commits[-1]["message"].replace("2026-11-24", "2026-11-23")),
                     lambda: self.commits[-1].update(message=self.commits[-1]["message"] + "Review-Date: 2026-11-24\n")]
        for mutation in mutations:
            self.commits, self.policy = fixture()
            mutation()
            with self.subTest(mutation=mutation):
                self.rejected_without_requests()

    def test_raw_sha_rejects_tree_change_even_with_resealed_json(self):
        self.commits[-1]["tree"] = "c" * 40
        self.rejected_without_requests()

    def test_duplicate_keys_nonfinite_and_oversized_inputs_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b" " * 101):
            with self.subTest(raw=raw[:20]), self.assertRaises(subject.ImportRejected):
                subject.decode_json(raw, 100, "test")

    def test_wrong_context_or_missing_token_never_posts(self):
        for key in CONTEXT:
            env = dict(CONTEXT)
            env.pop(key)
            with self.subTest(key=key), mock.patch.dict(os.environ, env, clear=True), mock.patch.object(subject, "post_commit") as post:
                with self.assertRaises(subject.ImportRejected):
                    subject.run(self.manifest_path, self.policy_path, self.digest, create_objects=True)
                post.assert_not_called()

    def test_only_fixed_post_endpoint_payload_and_context_are_used(self):
        requests = []

        def answer(request, timeout):
            index = len(requests)
            requests.append(request)
            self.assertEqual(request.full_url, subject.API_URL)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_header("Authorization"), "Bearer " + TOKEN)
            self.assertEqual(json.loads(request.data), {k: self.commits[index][k] for k in ("tree", "parents", "message", "author", "committer")})
            return Response(self.commits[index])

        opener = mock.Mock()
        opener.open.side_effect = answer
        with mock.patch.dict(os.environ, CONTEXT, clear=True), mock.patch.object(subject.urllib.request, "build_opener", return_value=opener) as build, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(subject.main(self.cli_args() + ["--create-objects"]), 0)
        self.assertEqual(len(requests), 16)
        self.assertTrue(all(isinstance(call.args[0], subject.RejectRedirects) for call in build.call_args_list))

    def test_response_mismatch_stops_before_next_object(self):
        responses = [Response(c) for c in self.commits]
        responses[2].data["committer"] = dict(responses[2].data["committer"], date="2026-11-24T00:00:00Z")
        opener = mock.Mock()
        opener.open.side_effect = responses
        with mock.patch.dict(os.environ, CONTEXT, clear=True), mock.patch.object(subject.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(subject.ImportRejected):
                subject.run(self.manifest_path, self.policy_path, self.digest, create_objects=True)
        self.assertEqual(opener.open.call_count, 3)

    def test_response_sha_tree_parent_date_and_message_checked(self):
        changes = [lambda r: r.data.update(sha="f" * 40), lambda r: r.data["tree"].update(sha="f" * 40),
                   lambda r: r.data["parents"].reverse(), lambda r: r.data.update(message="changed"),
                   lambda r: r.data.update(author=dict(r.data["author"], date="2026-11-24T00:00:00Z")),
                   lambda r: setattr(r, "status", 200), lambda r: setattr(r, "url", "https://example.com")]
        for change in changes:
            response = Response(copy.deepcopy(self.commits[0]))
            change(response)
            opener = mock.Mock()
            opener.open.return_value = response
            with self.subTest(change=change), mock.patch.object(subject.urllib.request, "build_opener", return_value=opener), self.assertRaises(subject.ImportRejected):
                subject.post_commit(self.commits[0], TOKEN)

    def test_github_json_may_omit_one_final_lf_with_the_same_object_sha(self):
        commit = self.commits[0]
        response = Response(copy.deepcopy(commit))
        response.data["message"] = commit["message"][:-1]
        self.assertEqual(response.data["sha"], commit["expected_sha"])
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(subject.urllib.request, "build_opener", return_value=opener):
            subject.post_commit(commit, TOKEN)
        request = opener.open.call_args.args[0]
        self.assertEqual(json.loads(request.data)["message"], commit["message"])
        self.assertTrue(json.loads(request.data)["message"].endswith("\n"))

    def test_response_message_rejects_extra_lf_or_mutated_text(self):
        commit = self.commits[0]
        messages = [commit["message"] + "\n", commit["message"][:-2],
                    commit["message"].replace("dependency update", "mutated text")[:-1]]
        for message in messages:
            response = Response(copy.deepcopy(commit))
            response.data["message"] = message
            opener = mock.Mock()
            opener.open.return_value = response
            with self.subTest(message=message), mock.patch.object(subject.urllib.request, "build_opener", return_value=opener), self.assertRaises(subject.ImportRejected):
                subject.post_commit(commit, TOKEN)

    def test_response_message_cannot_strip_multiple_final_lfs(self):
        commit = copy.deepcopy(self.commits[0])
        commit["message"] += "\n"
        commit["expected_sha"] = subject.commit_sha(commit)
        response = Response(copy.deepcopy(commit))
        response.data["message"] = commit["message"][:-2]
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(subject.urllib.request, "build_opener", return_value=opener), self.assertRaises(subject.ImportRejected):
            subject.post_commit(commit, TOKEN)

    def test_redirects_are_rejected_without_followup_request(self):
        request = urllib.request.Request(subject.API_URL, data=b"{}", method="POST")
        handler = subject.RejectRedirects()
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status), self.assertRaises(subject.ImportRejected):
                handler.redirect_request(request, None, status, "redirect", {}, "https://example.com")
        self.network_mock.assert_not_called()

    def test_errors_do_not_print_token_or_remote_response(self):
        opener = mock.Mock()
        opener.open.side_effect = RuntimeError(TOKEN + " secret response")
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, CONTEXT, clear=True), mock.patch.object(subject.urllib.request, "build_opener", return_value=opener), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(subject.main(self.cli_args() + ["--create-objects"]), 1)
        self.assertNotIn(TOKEN, stdout.getvalue() + stderr.getvalue())
        self.assertNotIn("secret response", stdout.getvalue() + stderr.getvalue())
        self.assertEqual(opener.open.call_count, 1)


if __name__ == "__main__":
    unittest.main()
