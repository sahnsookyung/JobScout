#!/usr/bin/env python3
"""Opt-in NVIDIA embedding comparison; offline scoring requires no credentials.

Only the fixture is sent to the provider. This script never reads the application
database or changes runtime configuration. See docs/plans/benchmarking/embedding-benchmark.md.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import platform
import random
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = ROOT / "tests/fixtures/evaluations/embedding_cases.json"
ENDPOINT = "https://integrate.api.nvidia.com/v1"
MODELS = {
    "nemotron-1b": {
        "id": "nvidia/nemotron-3.5-embed-1b", "parameters_b": 1.55,
        "native": 2048, "dimensions": [256, 512, 1024, 2048],
    },
    "nemotron-8b": {
        "id": "nvidia/nemotron-3.5-embed-8b", "parameters_b": 8.38,
        "native": 4096, "dimensions": [256, 512, 1024, 2048, 4096],
    },
}
SEED = 20261030


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def load_fixture(path: Path) -> Dict[str, Any]:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    if fixture.get("version") != 1 or not fixture.get("families"):
        raise ValueError("Expected a nonempty version 1 fixture")
    seen = set()
    for family in fixture["families"]:
        fid = family["id"]
        if fid in seen or family["split"] not in {"calibration", "holdout"}:
            raise ValueError("Duplicate family or invalid split")
        seen.add(fid)
        if family["task"] not in {"retrieval", "evidence"} or not family["slice"]:
            raise ValueError("Invalid task or slice")
        candidates = family["candidates"]
        if len(candidates) < 3 or len({c["id"] for c in candidates}) != len(candidates):
            raise ValueError("Need at least three uniquely identified candidates")
        for text in [family["query"]] + [c["text"] for c in candidates]:
            if set(text) != {"en", "ja"} or not all(isinstance(t, str) and t.strip() for t in text.values()):
                raise ValueError("Every text needs nonempty en and ja translations")
        if any(type(c["grade"]) is not int or c["grade"] not in (0, 1, 2) or not c["reason"] for c in candidates):
            raise ValueError("Invalid grade or missing label rationale")
        has_full = any(c["grade"] == 2 for c in candidates)
        if (family["slice"] == "no_match") == has_full:
            raise ValueError("No-match cases must have no full match; other cases need one")
    if {f["split"] for f in fixture["families"]} != {"calibration", "holdout"}:
        raise ValueError("Both family-disjoint splits are required")
    return fixture


def expand_cases(fixture: Dict[str, Any]) -> List[Dict[str, Any]]:
    cases = []
    for family in fixture["families"]:
        for qlang, plang in [("en", "en"), ("ja", "ja"), ("en", "ja"), ("ja", "en")]:
            cases.append({
                **{k: family[k] for k in ("id", "split", "task", "slice")},
                "family": family["id"], "id": f"{family['id']}:{qlang}-{plang}",
                "language": f"{qlang}-{plang}", "query": family["query"][qlang],
                "candidates": [{**c, "text": c["text"][plang]} for c in family["candidates"]],
            })
    return cases


def normalize(vector: List[float], dimensions: int) -> List[float]:
    if dimensions < 1 or len(vector) < dimensions:
        raise ValueError("Invalid vector dimension")
    values = vector[:dimensions]
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in values):
        raise ValueError("Non-finite or non-numeric embedding")
    norm = math.sqrt(sum(v * v for v in values))
    if not math.isfinite(norm) or norm <= 0:
        raise ValueError("Zero or invalid embedding norm")
    return [v / norm for v in values]


def validate_response(response: Any, count: int, dimensions: int) -> List[List[float]]:
    if len(response.data) != count:
        raise ValueError("Embedding response count mismatch")
    indices = [item.index for item in response.data]
    if any(type(i) is not int for i in indices) or set(indices) != set(range(count)):
        raise ValueError("Embedding response indices are missing or duplicated")
    vectors = [item.embedding for item in sorted(response.data, key=lambda item: item.index)]
    for vector in vectors:
        if len(vector) != dimensions:
            raise ValueError("Embedding response dimension mismatch")
        normalize(vector, dimensions)
    return vectors


def cache_key(model: Dict[str, Any], role: str, text: str) -> str:
    return digest({"endpoint": ENDPOINT, "model": model["id"], "dimensions": model["native"],
                   "input_type": role, "truncate": "NONE", "text": text, "encoding_format": "float"})


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def collect_vectors(cases: List[Dict[str, Any]], models: List[str], cache_dir: Path,
                    client: Any, rpm: float = 10, repeats: int = 3) -> Dict[str, Any]:
    """Interleave model batches, checkpoint valid vectors, and retain request timings.

    Failures raise after saving metadata. A rerun resumes cached vectors. SDK
    retries must be disabled: retries are visible new runs rather than hidden cost.
    """
    texts = {
        "query": sorted({c["query"] for c in cases}),
        "passage": sorted({p["text"] for c in cases for p in c["candidates"]}),
    }
    if rpm <= 0 or repeats < 1:
        raise ValueError("rpm and repeats must be positive")
    manifest = {"started_at": datetime.now(timezone.utc).isoformat(), "endpoint": ENDPOINT,
                "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "case_sha256": digest(cases),
                "python": platform.python_version(), "platform": platform.platform(),
                "rpm": rpm, "batch_size": 32, "models": {k: MODELS[k] for k in models},
                "requests": [], "prefix_checks": [], "cache_hits": 0}
    last_start = 0.0
    run_path = cache_dir / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + ".json")

    def request(alias: str, role: str, batch: List[str], dimensions: int, phase: str) -> List[List[float]]:
        nonlocal last_start
        time.sleep(max(0, 60 / rpm - (time.monotonic() - last_start)))
        last_start = time.monotonic()
        event = {"model": alias, "role": role, "count": len(batch), "dimensions": dimensions,
                 "phase": phase, "started_at": datetime.now(timezone.utc).isoformat()}
        try:
            response = client.embeddings.create(model=MODELS[alias]["id"], input=batch,
                dimensions=dimensions, encoding_format="float",
                extra_body={"input_type": role, "truncate": "NONE"})
            vectors = validate_response(response, len(batch), dimensions)
            event.update(status="ok", tokens=getattr(response.usage, "total_tokens", None))
            return vectors
        except Exception as exc:
            # Never persist provider bodies, request headers, or credentials.
            event.update(status="error", error_type=type(exc).__name__, http_status=getattr(exc, "status_code", None))
            raise RuntimeError(f"Embedding request failed: {alias}/{role}, {event['error_type']}, HTTP {event['http_status']}") from None
        finally:
            event["seconds"] = time.monotonic() - last_start
            manifest["requests"].append(event)
            save_json(run_path, manifest)
            print(json.dumps(event), flush=True)

    work = {alias: [] for alias in models}
    for alias in models:
        for role, values in texts.items():
            pending = []
            for value in values:
                path = cache_dir / "vectors" / (cache_key(MODELS[alias], role, value) + ".json")
                if path.exists():
                    vector = json.loads(path.read_text())
                    if len(vector) != MODELS[alias]["native"]:
                        raise ValueError("Cached vector dimension mismatch")
                    normalize(vector, len(vector))
                    manifest["cache_hits"] += 1
                else:
                    pending.append(value)
            work[alias].extend((role, pending[i:i+32]) for i in range(0, len(pending), 32))
    rng = random.Random(SEED)
    while any(work.values()):
        order = models.copy()
        rng.shuffle(order)
        for alias in order:
            if not work[alias]:
                continue
            role, batch = work[alias].pop(0)
            vectors = request(alias, role, batch, MODELS[alias]["native"], "corpus")
            for value, vector in zip(batch, vectors, strict=True):
                save_json(cache_dir / "vectors" / (cache_key(MODELS[alias], role, value) + ".json"), vector)
    # Verify every tested shortened dimension and both roles against the API.
    for alias in models:
        model = MODELS[alias]
        for role in texts:
            sample = texts[role][:2]
            for dimensions in model["dimensions"]:
                if dimensions == model["native"]:
                    continue
                direct = request(alias, role, sample, dimensions, "prefix_validation")
                cosines = []
                for value, vector in zip(sample, direct, strict=True):
                    native = json.loads((cache_dir / "vectors" / (cache_key(model, role, value) + ".json")).read_text())
                    cosines.append(sum(a*b for a, b in zip(normalize(native, dimensions), normalize(vector, dimensions), strict=True)))
                check = {"model": alias, "role": role, "dimensions": dimensions, "min_cosine": min(cosines)}
                manifest["prefix_checks"].append(check)
                save_json(run_path, manifest)
                if min(cosines) < 0.995:
                    raise ValueError("Native-prefix validation failed; do not use derived vectors")
    # Paired repeated requests measure the same 32 short passages for both models.
    for repeat in range(repeats):
        order = models.copy()
        rng.shuffle(order)
        for alias in order:
            request(alias, "passage", texts["passage"][:32], MODELS[alias]["native"], f"latency_repeat_{repeat+1}")
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    save_json(run_path, manifest)
    return manifest


def rank_case(case: Dict[str, Any], query: List[float], passages: List[List[float]]) -> Dict[str, Any]:
    ranked = sorted([
        {**p, "score": sum(a*b for a, b in zip(query, vector, strict=True))}
        for p, vector in zip(case["candidates"], passages, strict=True)
    ], key=lambda item: (-item["score"], digest(item["id"])))
    full = [i+1 for i, p in enumerate(ranked) if p["grade"] == 2]
    bad_scores = [p["score"] for p in ranked if p["grade"] < 2]
    good_scores = [p["score"] for p in ranked if p["grade"] == 2]
    ideal = sorted((p["grade"] for p in ranked), reverse=True)[:3]
    dcg = sum((2**p["grade"]-1)/math.log2(i+2) for i, p in enumerate(ranked[:3]))
    idcg = sum((2**grade-1)/math.log2(i+2) for i, grade in enumerate(ideal))
    return {
        **{k: case[k] for k in ("id", "family", "task", "split", "slice", "language", "query")},
        "ranked": ranked, "has_full": bool(full), "top_grade": ranked[0]["grade"],
        "hit1": int(bool(full) and full[0] == 1),
        "recall3": sum(rank <= 3 for rank in full)/len(full) if full else None,
        "mrr": 1/full[0] if full else None, "ndcg3": dcg/idcg if idcg else None,
        "margin": max(good_scores)-max(bad_scores) if good_scores and bad_scores else None,
    }


def calibrate_threshold(rows: List[Dict[str, Any]]) -> float:
    """Conservative zero observed false accepts on calibration evidence only.

    This diagnostic is deliberately not a production eligibility threshold.
    A small calibration set cannot guarantee a low true false-positive rate.
    """
    calibration = [r for r in rows if r["split"] == "calibration" and r["task"] == "evidence"]
    negatives = [p["score"] for row in calibration for p in row["ranked"] if p["grade"] < 2]
    if not calibration or not negatives:
        raise ValueError("Calibration evidence must include negatives")
    return math.nextafter(max(negatives), math.inf)


def summarize(rows: List[Dict[str, Any]], threshold: float) -> Dict[str, Any]:
    positive = [r for r in rows if r["has_full"]]
    no_match = [r for r in rows if not r["has_full"]]
    evidence = [r for r in rows if r["task"] == "evidence"]
    accepted = [r for r in evidence if r["ranked"][0]["score"] >= threshold]
    false_accepts = sum(r["top_grade"] < 2 for r in accepted)
    return {
        "cases": len(rows), "positive_cases": len(positive), "families": len({r["family"] for r in rows}),
        **{name: statistics.mean(r[name] for r in positive) if positive else None for name in ("hit1", "recall3", "mrr", "ndcg3")},
        "nonfull_outrank_rate": statistics.mean(r["margin"] <= 0 for r in positive) if positive else None,
        "evidence_accepted": len(accepted), "evidence_cases": len(evidence), "false_accepts": false_accepts,
        "accepted_precision": 1-false_accepts/len(accepted) if accepted else None,
        "no_match_cases": len(no_match),
        "no_match_false_accepts": sum(r["ranked"][0]["score"] >= threshold for r in no_match),
    }


def paired_bootstrap(left: List[Dict[str, Any]], right: List[Dict[str, Any]], iterations: int = 2000) -> Dict[str, Any]:
    """Paired family bootstrap; language variants never count as independent draws."""
    other = {r["id"]: r for r in right}
    groups = defaultdict(list)
    wins = losses = 0
    for row in left:
        if row["split"] != "holdout" or not row["has_full"]:
            continue
        delta = row["hit1"] - other[row["id"]]["hit1"]
        groups[row["family"]].append(delta)
        wins += delta > 0
        losses += delta < 0
    means = [statistics.mean(values) for values in groups.values()]
    if not means:
        raise ValueError("No paired holdout positive families")
    rng = random.Random(SEED)
    samples = sorted(statistics.mean(rng.choices(means, k=len(means))) for _ in range(iterations))
    return {"family_mean_hit1_delta": statistics.mean(means), "ci95": [samples[int(iterations*.025)], samples[int(iterations*.975)]],
            "family_count": len(means), "case_wins": wins, "case_losses": losses, "bootstrap_iterations": iterations}


def evaluate(fixture: Dict[str, Any], models: List[str], cache_dir: Path) -> Dict[str, Any]:
    cases = expand_cases(fixture)
    results = {}
    for alias in models:
        model = MODELS[alias]
        vectors = {}
        for case in cases:
            for role, texts in [("query", [case["query"]]), ("passage", [p["text"] for p in case["candidates"]])]:
                for text in texts:
                    key = cache_key(model, role, text)
                    if key not in vectors:
                        path = cache_dir / "vectors" / (key + ".json")
                        if not path.exists():
                            raise ValueError(f"Missing cached vector for {alias}; live collection required")
                        vector = json.loads(path.read_text())
                        if len(vector) != model["native"]:
                            raise ValueError("Cached vector dimension mismatch")
                        normalize(vector, model["native"])
                        vectors[key] = vector
        for dimensions in model["dimensions"]:
            normalized = {key: normalize(vector, dimensions) for key, vector in vectors.items()}
            rows = [rank_case(case, normalized[cache_key(model, "query", case["query"])],
                     [normalized[cache_key(model, "passage", p["text"])] for p in case["candidates"]]) for case in cases]
            threshold = calibrate_threshold(rows)
            heldout = [r for r in rows if r["split"] == "holdout"]
            slices = {}
            for field in ("task", "slice", "language"):
                for value in sorted({r[field] for r in heldout}):
                    slices[f"{field}:{value}"] = summarize([r for r in heldout if r[field] == value], threshold)
            results[f"{alias}/{dimensions}"] = {"model": alias, "dimensions": dimensions,
                "float32_bytes_per_vector": dimensions*4, "threshold": threshold,
                "holdout": summarize(heldout, threshold), "slices": slices, "rows": rows}
    baseline = "nemotron-1b/1024" if "nemotron-1b/1024" in results else next(iter(results))
    comparisons = {key: paired_bootstrap(value["rows"], results[baseline]["rows"]) for key, value in results.items()}
    return {"fixture_sha256": digest(fixture), "label_status": fixture["label_status"],
            "seed": SEED, "baseline": baseline, "configurations": results, "paired_vs_baseline": comparisons}


def review_families(report: Dict[str, Any], limit: int = 12) -> List[str]:
    """Prioritize failure/disagreement families, plus a seeded audit of passes."""
    by_case = defaultdict(list)
    priority = defaultdict(int)
    for config in report["configurations"].values():
        for row in config["rows"]:
            by_case[row["id"]].append(row)
            if row["has_full"] and (not row["hit1"] or row["margin"] < .03):
                priority[row["family"]] += 1
            if not row["has_full"] and row["ranked"][0]["score"] >= config["threshold"]:
                priority[row["family"]] += 2
    for rows in by_case.values():
        if len({r["ranked"][0]["id"] for r in rows}) > 1:
            priority[rows[0]["family"]] += 2
    all_families = sorted({rows[0]["family"] for rows in by_case.values()})
    flagged = sorted(priority, key=lambda f: (-priority[f], f))[:limit]
    clean = [f for f in all_families if f not in priority]
    random.Random(SEED).shuffle(clean)
    return flagged + clean[:4]


def write_html(report: Dict[str, Any], fixture: Dict[str, Any], path: Path) -> None:
    """Self-contained report with bounded first review, full details, and audit export."""
    def esc(value: Any) -> str:
        return html.escape(str(value), quote=True)

    def pct(value: Optional[float]) -> str:
        return "—" if value is None else f"{100*value:.1f}%"
    selected = review_families(report)
    report["priority_review_families"] = selected
    summary_rows = []
    for name, config in report["configurations"].items():
        s = config["holdout"]
        delta = report["paired_vs_baseline"][name]
        summary_rows.append(f"<tr><th>{esc(name)}</th><td>{pct(s['hit1'])}</td><td>{pct(s['recall3'])}</td>"
            f"<td>{s['mrr']:.3f}</td><td>{s['ndcg3']:.3f}</td><td>{s['evidence_accepted']}/{s['evidence_cases']}</td><td>{s['false_accepts']}/{s['evidence_accepted']}</td>"
            f"<td>{s['no_match_false_accepts']}/{s['no_match_cases']}</td><td>{config['float32_bytes_per_vector']//1024} KiB</td>"
            f"<td>{delta['family_mean_hit1_delta']*100:+.1f} pp [{delta['ci95'][0]*100:+.1f}, {delta['ci95'][1]*100:+.1f}]</td></tr>")
    timing_rows = []
    requests = report.get("collection", {}).get("requests", [])
    for alias in report.get("collection", {}).get("models", {}):
        measured = [r['seconds'] for r in requests if r['model'] == alias and r['phase'].startswith('latency_repeat') and r['status'] == 'ok']
        if measured:
            timing_rows.append(f"<tr><th>{esc(alias)}</th><td>{len(measured)}</td><td>{statistics.median(measured):.3f}s</td><td>{min(measured):.3f}–{max(measured):.3f}s</td></tr>")
    slice_names = ["task:retrieval", "task:evidence", "language:en-en", "language:ja-ja",
                   "language:en-ja", "language:ja-en", "slice:negation", "slice:numeric"]
    slice_rows = []
    for name, config in report["configurations"].items():
        cells = ''.join(f"<td>{pct(config['slices'].get(key, {}).get('hit1'))}</td>" for key in slice_names)
        slice_rows.append(f"<tr><th>{esc(name)}</th>{cells}</tr>")
    slice_headers = ''.join(f"<th>{esc(name)}</th>" for name in slice_names)
    details = []
    row_maps = {name: {r['id']: r for r in config['rows']} for name, config in report['configurations'].items()}
    for family in sorted(fixture['families'], key=lambda f: (f['id'] not in selected, selected.index(f['id']) if f['id'] in selected else 0, f['id'])):
        fid = family['id']
        labels = ''.join(f"<tr><td>{c['grade']}</td><td>{esc(c['text']['en'])}<br>{esc(c['text']['ja'])}</td><td>{esc(c['reason'])}</td></tr>" for c in family['candidates'])
        rankings = []
        for name, rows in row_maps.items():
            for row in rows.values():
                if row['family'] != fid:
                    continue
                margin = '—' if row['margin'] is None else f"{row['margin']:+.3f}"
                ranked = '<br>'.join(f"{p['score']:.3f} · grade {p['grade']} · {esc(p['text'])}" for p in row['ranked'])
                failure = row['has_full'] and not row['hit1']
                rankings.append(f"<tr class=\"{'failure' if failure else ''}\"><th>{esc(name)}<br>{esc(row['language'])}</th><td>{margin}</td><td>{ranked}</td></tr>")
        details.append(f"<details class=family data-priority=\"{str(fid in selected).lower()}\"><summary>{esc(fid)} · {esc(family['split'])} · {esc(family['slice'])}{' · PRIORITY' if fid in selected else ''}</summary>"
            f"<p>{esc(family['query']['en'])}<br>{esc(family['query']['ja'])}</p>"
            f"<label>Label review <select data-review=\"{esc(fid)}\"><option value=\"unreviewed\">Unreviewed</option><option value=\"accept\">Accept labels</option><option value=\"revise\">Needs correction</option><option value=\"unclear\">Ambiguous</option></select></label> "
            f"<label>Notes <input data-notes=\"{esc(fid)}\" placeholder=\"Optional correction\"></label>"
            f"<table><thead><tr><th>Grade</th><th>Candidate EN / JA</th><th>Rationale</th></tr></thead><tbody>{labels}</tbody></table>"
            f"<details><summary>All model rankings (4 language directions each)</summary><table><thead><tr><th>Configuration / language</th><th>Full-minus-other margin</th><th>Ranking</th></tr></thead><tbody>{''.join(rankings)}</tbody></table></details></details>")
    payload = json.dumps({'fixture_sha256': report['fixture_sha256'], 'families': [f['id'] for f in fixture['families']]}).replace('<', '\\u003c')
    cases = expand_cases(fixture)
    heldout = [c for c in cases if c['split'] == 'holdout']
    heldout_families = len({c['family'] for c in heldout})
    positive_count = sum(any(p['grade'] == 2 for p in c['candidates']) for c in heldout)
    calibration_families = len(fixture['families']) - heldout_families
    output = f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JobScout embedding benchmark</title><style>
body{{font:16px/1.55 system-ui,sans-serif;max-width:1250px;margin:32px auto;padding:0 20px;color:#172332;background:#f6f8fb}}h1,h2{{line-height:1.2}}table{{border-collapse:collapse;width:100%;margin:16px 0;font-size:14px;background:white}}th,td{{text-align:left;padding:10px;border:1px solid #cbd5e1;vertical-align:top}}th{{background:#edf2f7}}details{{background:white;border:1px solid #cbd5e1;padding:14px;margin:12px 0}}summary{{cursor:pointer;font-weight:650}}.failure{{background:#fff0ed}}.note{{border-left:4px solid #c68b12;padding:14px;background:#fff8e8}}button,select,input{{font:inherit;padding:6px}}.scroll{{overflow:auto}}code{{overflow-wrap:anywhere}}
</style><h1>JobScout embedding benchmark</h1>
<p>{len(cases)} cases · {len(fixture['families'])} scenario families · English / Japanese in four query–passage directions. Native vectors are cached once; supported prefixes are normalized for dimension comparisons.</p>
<p class="note"><strong>Draft synthetic labels — human adjudication pending.</strong> This is a four-candidate contrast test of the embedding component. It does not measure full-corpus recall, final eligibility, or Discord delivery. No Qwen baseline is included, so this is not a measured NVIDIA-versus-Qwen comparison. Do not treat confidence intervals as proof of generalization beyond these scenarios.</p>
<h2>Held-out quality</h2><p>{len(heldout)} cases in {heldout_families} held-out families; {positive_count} have a full match. Top-1 and recall@3 require grade 2; nDCG also rewards grade 1. Calibration uses {calibration_families} separate families. Confidence intervals resample whole families, preserving language dependence, and compare with {esc(report['baseline'])}.</p>
<div class="scroll"><table><thead><tr><th>Model / dimensions</th><th>Top-1</th><th>Recall@3</th><th>MRR</th><th>nDCG@3</th><th>Accepted / evidence cases</th><th>Wrong / accepted evidence</th><th>No-match false accepts</th><th>Vector storage</th><th>Top-1 delta, 95% CI</th></tr></thead><tbody>{''.join(summary_rows)}</tbody></table></div>
<p>The cosine threshold is fitted only on calibration evidence, above all observed non-full matches. Report coverage alongside errors: accepting nothing is not success. This is a diagnostic, not a deployable eligibility policy. Smaller output vectors reduce storage; these derived dimensions do not establish faster model inference.</p>
<h2>Held-out slices: full-match top-1</h2><p>Language direction is query–passage. Evidence cases require full support; retrieving a topically related passage alone does not pass. Small slices are diagnostic, and all denominators are in report.json.</p><div class="scroll"><table><thead><tr><th>Configuration</th>{slice_headers}</tr></thead><tbody>{''.join(slice_rows)}</tbody></table></div>
<h2>Observed API latency</h2><p>Repeated identical 32-passage requests, serial and interleaved, SDK retries disabled. Native output dimensions; model service load and output size both affect latency. Small sample: no p95 or SLA claim.</p><table><thead><tr><th>Model</th><th>Repeats</th><th>Median</th><th>Range</th></tr></thead><tbody>{''.join(timing_rows)}</tbody></table>
<h2>Review queue</h2><p>Start with {len(selected)} families: the highest failure/disagreement/low-margin priorities plus up to four seeded passing controls. Expand rankings only when needed. All {len(fixture['families'])} families remain available. Accepting a family validates its labels and both translations, not a model.</p>
<label><input type="checkbox" id="priority" checked> Priority queue only</label> <button id="export">Export label review</button>
{''.join(details)}
<h2>Reproducibility</h2><p>Fixture SHA-256: <code>{report['fixture_sha256']}</code>. Fixed seed: {SEED}. The adjacent report.json contains every score, slice, confidence interval, provider timing, and prefix validation. Model aliases may change server weights; a fresh cache directory is required for a new provider snapshot.</p>
<script>
const meta={payload};const storageKey='embedding-review:'+meta.fixture_sha256;
let saved={{}};try{{saved=JSON.parse(localStorage.getItem(storageKey)||'{{}}')}}catch(e){{}}
function persist(){{try{{localStorage.setItem(storageKey,JSON.stringify(saved))}}catch(e){{}}}}
document.querySelectorAll('[data-review]').forEach(el=>{{el.value=saved[el.dataset.review]?.status||'unreviewed';el.addEventListener('change',()=>{{let id=el.dataset.review;saved[id]={{...saved[id],status:el.value}};persist()}})}});
document.querySelectorAll('[data-notes]').forEach(el=>{{el.value=saved[el.dataset.notes]?.notes||'';el.addEventListener('input',()=>{{let id=el.dataset.notes;saved[id]={{...saved[id],notes:el.value}};persist()}})}});
function filter(){{document.querySelectorAll('.family').forEach(el=>el.hidden=document.getElementById('priority').checked&&el.dataset.priority!=='true')}}
document.getElementById('priority').addEventListener('change',filter);filter();
document.getElementById('export').addEventListener('click',()=>{{let data={{fixture_sha256:meta.fixture_sha256,reviewed_at:new Date().toISOString(),families:meta.families.map(id=>({{id,status:saved[id]?.status||'unreviewed',notes:saved[id]?.notes||''}}))}};let url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{{type:'application/json'}}));let a=document.createElement('a');a.href=url;a.download='embedding-label-review.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}});
</script></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output, encoding="utf-8")


def verify_collection(collection: Dict[str, Any], models: List[str]) -> None:
    """Do not publish an offline comparison from an interrupted validation run."""
    if not collection.get("completed_at"):
        raise ValueError("Collection is incomplete; resume with --collect before reporting")
    for alias in models:
        model = MODELS[alias]
        if collection.get("models", {}).get(alias) != model:
            raise ValueError("Collection model configuration differs; use a matching provider snapshot")
        for dimensions in model["dimensions"]:
            if dimensions == model["native"]:
                continue
            for role in ("query", "passage"):
                checks = [c for c in collection.get("prefix_checks", [])
                          if c["model"] == alias and c["dimensions"] == dimensions and c["role"] == role]
                if not checks or not all(math.isfinite(c["min_cosine"]) and c["min_cosine"] >= .995 for c in checks):
                    raise ValueError("Missing or failed dimension-prefix validation; resume with --collect")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--output", type=Path, default=ROOT / "output/embedding-benchmark")
    parser.add_argument("--models", nargs="+", choices=MODELS, default=list(MODELS))
    parser.add_argument("--collect", action="store_true", help="Explicitly call NVIDIA using NVIDIA_API_KEY")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--rpm", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    fixture = load_fixture(args.fixture)
    print(json.dumps({"fixture_sha256": digest(fixture), "families": len(fixture['families']), "cases": len(expand_cases(fixture))}))
    if args.validate_only:
        return 0
    cache_dir = args.output / "cache"
    collection: Optional[Dict[str, Any]] = None
    if args.collect:
        from openai import OpenAI

        key = os.environ.get("NVIDIA_API_KEY")
        if not key:
            parser.error("--collect requires NVIDIA_API_KEY")
        with OpenAI(api_key=key, base_url=ENDPOINT, timeout=60, max_retries=0) as client:
            collection = collect_vectors(expand_cases(fixture), args.models, cache_dir, client, args.rpm, args.repeats)
    if collection is None:
        manifests = sorted(cache_dir.glob("run-*.json"))
        if manifests:
            collection = json.loads(manifests[-1].read_text())
    verify_collection(collection or {}, args.models)
    report = evaluate(fixture, args.models, cache_dir)
    report["evaluation_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["collection"] = collection or {}
    write_html(report, fixture, args.output / "report.html")
    save_json(args.output / "report.json", report)
    print(json.dumps({"report": str(args.output / 'report.html'), "configurations": len(report['configurations'])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
