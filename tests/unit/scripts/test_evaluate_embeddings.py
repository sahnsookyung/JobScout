import copy
import importlib.util
import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/evaluate_embeddings.py"
SPEC = importlib.util.spec_from_file_location("embedding_benchmark", SCRIPT)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_fixture_is_family_disjoint_and_has_all_language_directions() -> None:
    fixture = benchmark.load_fixture(benchmark.DEFAULT_FIXTURE)
    cases = benchmark.expand_cases(fixture)
    assert len(cases) == 112
    assert len({c["id"] for c in cases}) == 112
    assert len([c for c in cases if c["split"] == "holdout"]) == 80
    splits = {split: {c["family"] for c in cases if c["split"] == split} for split in ("calibration", "holdout")}
    assert not splits["calibration"] & splits["holdout"]
    for family in fixture["families"]:
        assert {c["language"] for c in cases if c["family"] == family["id"]} == {"en-en", "ja-ja", "en-ja", "ja-en"}


@pytest.mark.parametrize("mutation", ["duplicate", "bad_grade", "missing_language", "no_positive"])
def test_invalid_labels_fail_before_provider_calls(tmp_path: Path, mutation: str) -> None:
    fixture = benchmark.load_fixture(benchmark.DEFAULT_FIXTURE)
    family = fixture["families"][0]
    if mutation == "duplicate":
        fixture["families"].append(copy.deepcopy(family))
    elif mutation == "bad_grade":
        family["candidates"][0]["grade"] = True
    elif mutation == "missing_language":
        del family["query"]["ja"]
    else:
        family["candidates"][0]["grade"] = 0
    path = tmp_path / "fixture.json"
    path.write_text(json.dumps(fixture))
    with pytest.raises(ValueError):
        benchmark.load_fixture(path)


@pytest.mark.parametrize("vector,dimensions", [([0, 0], 2), ([float('nan'), 1], 2), ([float('inf'), 1], 2), ([1], 2), ([True, 1], 2)])
def test_invalid_vectors_fail_closed(vector: list, dimensions: int) -> None:
    with pytest.raises(ValueError):
        benchmark.normalize(vector, dimensions)


def test_prefix_normalization_is_not_full_vector_normalization() -> None:
    assert benchmark.normalize([3, 4, 12], 2) == pytest.approx([.6, .8])


def test_response_is_reordered_by_index_and_never_assumed_ordered() -> None:
    response = SimpleNamespace(data=[SimpleNamespace(index=1, embedding=[0, 1]), SimpleNamespace(index=0, embedding=[1, 0])])
    assert benchmark.validate_response(response, 2, 2) == [[1, 0], [0, 1]]
    response.data[0].index = 0
    with pytest.raises(ValueError, match="indices"):
        benchmark.validate_response(response, 2, 2)


@pytest.mark.parametrize("data", [[], [SimpleNamespace(index=0, embedding=[1])], [SimpleNamespace(index=2, embedding=[1, 0])]])
def test_missing_or_malformed_response_is_not_a_quality_score(data: list) -> None:
    with pytest.raises(ValueError):
        benchmark.validate_response(SimpleNamespace(data=data), 1, 2)


def test_cache_separates_roles_models_and_dimensions() -> None:
    model = benchmark.MODELS["nemotron-1b"]
    original = benchmark.cache_key(model, "query", "same text")
    assert original != benchmark.cache_key(model, "passage", "same text")
    assert original != benchmark.cache_key({**model, "native": 4096}, "query", "same text")
    assert original != benchmark.cache_key(benchmark.MODELS["nemotron-8b"], "query", "same text")


def test_hand_calculated_ranking_distinguishes_partial_from_full() -> None:
    case = {"id": "a:en-en", "family": "a", "task": "evidence", "split": "holdout", "slice": "negation", "language": "en-en", "query": "test",
            "candidates": [{"id": str(i), "grade": grade, "text": str(i)} for i, grade in enumerate([2, 1, 0])]}
    row = benchmark.rank_case(case, [1, 0], [[.6, .8], [.8, .6], [0, 1]])
    assert row["hit1"] == 0
    assert row["mrr"] == .5
    assert row["recall3"] == 1
    assert row["margin"] == pytest.approx(-.2)
    assert row["ndcg3"] == pytest.approx((1+3/math.log2(3))/(3+1/math.log2(3)))


def test_threshold_never_looks_at_holdout_and_no_match_has_separate_denominator() -> None:
    rows = [{"split": "calibration", "task": "evidence", "ranked": [{"grade": 0, "score": .4}, {"grade": 2, "score": .8}]},
            {"split": "holdout", "task": "evidence", "ranked": [{"grade": 0, "score": .99}]}]
    assert benchmark.calibrate_threshold(rows) == math.nextafter(.4, math.inf)
    nomatch = {"family": "n", "has_full": False, "task": "evidence", "top_grade": 0, "ranked": [{"grade": 0, "score": .99}]}
    summary = benchmark.summarize([nomatch], .5)
    assert summary["hit1"] is None
    assert summary["no_match_false_accepts"] == 1
    assert summary["false_accepts"] == 1


def test_bootstrap_uses_families_not_language_variants() -> None:
    left = [{"id": f"{family}:{lang}", "family": family, "split": "holdout", "has_full": True, "hit1": family == "a"}
            for family in ("a", "b") for lang in range(4)]
    right = [{**row, "hit1": 0} for row in left]
    comparison = benchmark.paired_bootstrap(left, right, iterations=200)
    assert comparison["family_count"] == 2
    assert comparison["family_mean_hit1_delta"] == .5
    assert comparison["case_wins"] == 4
    assert comparison["ci95"] == [0, 1]


def test_failed_collection_records_metadata_without_caching_invalid_vectors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(benchmark.time, "sleep", lambda _: None)
    monkeypatch.setattr(benchmark, "MODELS", {"test": {"id": "test", "native": 2, "dimensions": [2]}})
    client = SimpleNamespace(embeddings=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(data=[])))
    with pytest.raises(RuntimeError, match="ValueError"):
        benchmark.collect_vectors([{"query": "synthetic", "candidates": [{"text": "synthetic passage"}]}], ["test"], tmp_path, client)
    assert not list(tmp_path.glob("vectors/*.json"))
    events = json.loads(next(tmp_path.glob("run-*.json")).read_text())["requests"]
    assert events[0]["status"] == "error"
    assert "body" not in events[0]


def test_offline_scoring_and_review_escape_fixture_text(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = benchmark.load_fixture(benchmark.DEFAULT_FIXTURE)
    fixture["families"][0]["query"]["en"] = '<script>alert("unsafe")</script>'
    monkeypatch.setattr(benchmark, "MODELS", {"test": {"id": "test", "native": 2, "dimensions": [2]}})
    for case in benchmark.expand_cases(fixture):
        benchmark.save_json(tmp_path / "vectors" / (benchmark.cache_key(benchmark.MODELS["test"], "query", case["query"])+".json"), [1, 0])
        for candidate in case["candidates"]:
            vector = [1, 0] if candidate["grade"] == 2 else [.5, 1]
            benchmark.save_json(tmp_path / "vectors" / (benchmark.cache_key(benchmark.MODELS["test"], "passage", candidate["text"])+".json"), vector)
    report = benchmark.evaluate(fixture, ["test"], tmp_path)
    assert report["configurations"]["test/2"]["holdout"]["hit1"] == 1
    assert report["configurations"]["test/2"]["holdout"]["no_match_false_accepts"] == 0
    benchmark.write_html(report, fixture, tmp_path / "report.html")
    output = (tmp_path / "report.html").read_text()
    assert '<script>alert("unsafe")</script>' not in output
    assert '&lt;script&gt;alert' in output
    assert len(report["priority_review_families"]) <= 16


def test_collection_resumes_vectors_and_validates_prefixes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(benchmark.time, "sleep", lambda _: None)
    monkeypatch.setattr(benchmark, "MODELS", {"test": {"id": "test", "native": 2, "dimensions": [1, 2]}})

    def create(**kwargs: dict) -> SimpleNamespace:
        return SimpleNamespace(data=[SimpleNamespace(index=i, embedding=[1, 0][:kwargs['dimensions']]) for i in range(len(kwargs['input']))], usage=SimpleNamespace(total_tokens=1))

    client = SimpleNamespace(embeddings=SimpleNamespace(create=create))
    cases = [{"query": "synthetic query", "candidates": [{"text": "synthetic passage"}]}]
    first = benchmark.collect_vectors(cases, ["test"], tmp_path, client, repeats=1)
    second = benchmark.collect_vectors(cases, ["test"], tmp_path, client, repeats=1)
    assert len(first["requests"]) == 5
    assert len(second["requests"]) == 3
    assert second["cache_hits"] == 2
    assert all(r['phase'] != 'corpus' for r in second['requests'])
    benchmark.verify_collection(second, ['test'])
    del second['completed_at']
    with pytest.raises(ValueError, match='incomplete'):
        benchmark.verify_collection(second, ['test'])


@pytest.mark.parametrize("cosine", [0.9, float('nan'), None])
def test_offline_report_requires_successful_prefix_validation(cosine: float, monkeypatch: pytest.MonkeyPatch) -> None:
    model = {"id": "test", "native": 2, "dimensions": [1, 2]}
    monkeypatch.setattr(benchmark, 'MODELS', {'test': model})
    checks = [] if cosine is None else [{'model': 'test', 'dimensions': 1, 'role': role, 'min_cosine': cosine} for role in ['query', 'passage']]
    manifest = {'completed_at': 'timestamp', 'models': {'test': model}, 'prefix_checks': checks}
    with pytest.raises(ValueError, match='prefix'):
        benchmark.verify_collection(manifest, ['test'])
