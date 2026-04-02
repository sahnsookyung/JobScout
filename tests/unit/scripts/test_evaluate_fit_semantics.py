import json
import importlib.util
from pathlib import Path
from types import SimpleNamespace

from core.config_loader import MatcherConfig, ScorerConfig
from core.scorer.semantic_fit import ThresholdSemanticFitScorer


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "evaluate_fit_semantics.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location("evaluate_fit_semantics_script", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_pair_preliminary_builds_expected_requirement_candidates():
    script = _load_script_module()
    case = {
        "name": "python_backend_match",
        "job_title": "Python Platform Engineer",
        "job_company": "Acme Cloud",
        "job_summary": "Python APIs and backend services",
        "requirement_text": "Python backend API development",
        "req_type": "required",
        "evidence_text": "Built Python backend APIs for internal services",
        "evidence_section": "experience",
        "original_similarity": 0.31,
    }

    preliminary = script._pair_preliminary(case)

    assert preliminary.job.title == "Python Platform Engineer"
    assert preliminary.resume_fingerprint == "fixture-python_backend_match"
    assert len(preliminary.missing_requirements) == 1
    candidate = preliminary.missing_requirements[0].evidence_candidates[0]
    assert candidate.rank == 1
    assert candidate.evidence.source_section == "experience"


def test_evaluate_pair_cases_reports_threshold_baseline_failures():
    script = _load_script_module()
    cases = [
        {
            "name": "java_python_mismatch",
            "job_title": "Java Backend Engineer",
            "job_company": "Acme Cloud",
            "job_summary": "Backend role focused on Java microservices.",
            "requirement_text": "Strong Java programming experience",
            "req_type": "required",
            "evidence_text": "Built Python FastAPI services and internal APIs",
            "evidence_section": "experience",
            "original_similarity": 0.82,
            "expected_verdict": "missing",
        }
    ]

    summary = script._evaluate_pair_cases(
        cases,
        scorer=ThresholdSemanticFitScorer(),
        scorer_config=ScorerConfig(),
    )

    assert summary["total"] == 1
    assert summary["passed"] == 0
    assert summary["results"][0]["actual_verdict"] == "covered"


def test_evaluate_retrieval_cases_reports_fused_top_result():
    script = _load_script_module()
    cases = [
        {
            "name": "lexical_fastapi_rescue",
            "dense_candidates": [
                {"job_id": "dense-generalist", "dense_score": 0.95},
                {"job_id": "exact-fastapi", "dense_score": 0.70},
            ],
            "lexical_candidates": [
                {"job_id": "exact-fastapi", "lexical_score": 0.99, "dense_similarity": 0.70},
            ],
            "expected_top_job_id": "exact-fastapi",
        }
    ]

    summary = script._evaluate_retrieval_cases(
        cases,
        matcher_config=MatcherConfig(hybrid_retrieval_enabled=True),
    )

    assert summary["passed"] == 1
    assert summary["results"][0]["actual_top_job_id"] == "exact-fastapi"


def test_main_prints_json_report(monkeypatch, capsys, tmp_path):
    script = _load_script_module()
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(json.dumps({"pair_cases": [], "retrieval_cases": []}), encoding="utf-8")

    monkeypatch.setattr(
        script,
        "_parse_args",
        lambda: SimpleNamespace(
            fixture=fixture_path,
            mode="threshold",
            config=Path(__file__).resolve().parents[3] / "config.yaml",
            allow_failures=False,
        ),
    )

    exit_code = script.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["mode"] == "threshold"
    assert payload["pair_summary"]["total"] == 0
    assert payload["all_passed"] is True

def test_main_returns_non_zero_when_cases_fail_by_default(monkeypatch, tmp_path):
    script = _load_script_module()
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(
        json.dumps(
            {
                "pair_cases": [
                    {
                        "name": "java_python_mismatch",
                        "job_title": "Java Backend Engineer",
                        "job_company": "Acme Cloud",
                        "job_summary": "Backend role focused on Java microservices.",
                        "requirement_text": "Strong Java programming experience",
                        "req_type": "required",
                        "evidence_text": "Built Python FastAPI services and internal APIs",
                        "evidence_section": "experience",
                        "original_similarity": 0.82,
                        "expected_verdict": "missing",
                    }
                ],
                "retrieval_cases": [],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        script,
        "_parse_args",
        lambda: SimpleNamespace(
            fixture=fixture_path,
            mode="threshold",
            config=Path(__file__).resolve().parents[3] / "config.yaml",
            allow_failures=False,
        ),
    )

    assert script.main() == 1

def test_main_allows_failures_when_requested(monkeypatch, tmp_path):
    script = _load_script_module()
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(
        json.dumps(
            {
                "pair_cases": [
                    {
                        "name": "java_python_mismatch",
                        "job_title": "Java Backend Engineer",
                        "job_company": "Acme Cloud",
                        "job_summary": "Backend role focused on Java microservices.",
                        "requirement_text": "Strong Java programming experience",
                        "req_type": "required",
                        "evidence_text": "Built Python FastAPI services and internal APIs",
                        "evidence_section": "experience",
                        "original_similarity": 0.82,
                        "expected_verdict": "missing",
                    }
                ],
                "retrieval_cases": [],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        script,
        "_parse_args",
        lambda: SimpleNamespace(
            fixture=fixture_path,
            mode="threshold",
            config=Path(__file__).resolve().parents[3] / "config.yaml",
            allow_failures=True,
        ),
    )

    assert script.main() == 0
