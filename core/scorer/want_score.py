from __future__ import annotations

from typing import Any, Dict, List, Tuple
import numpy as np

from core.config_loader import FacetWeights

FACET_KEYS = [
    "remote_flexibility",
    "compensation",
    "learning_growth",
    "company_culture",
    "work_life_balance",
    "tech_stack",
    "visa_sponsorship",
]

def _row_normed(x: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    x = np.asarray(x, dtype=np.float32)
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.maximum(n, eps)

def calculate_want_score(
    user_want_embeddings: List[np.ndarray],
    job_facet_embeddings: Dict[str, np.ndarray],
    facet_weights: FacetWeights,
) -> Tuple[float, Dict[str, Any]]:
    if not user_want_embeddings:
        return 0.0, {"error": "No user wants provided", "want_score": 0.0}
    if not job_facet_embeddings:
        return 0.0, {"error": "No job facet embeddings available", "want_score": 0.0}

    # Keep a stable facet order (prefer FACET_KEYS, otherwise whatever is present)
    facet_keys = [k for k in FACET_KEYS if k in job_facet_embeddings]
    if not facet_keys:
        facet_keys = list(job_facet_embeddings.keys())

    W = np.vstack(user_want_embeddings)                         # (num_wants, dim)
    F = np.vstack([job_facet_embeddings[k] for k in facet_keys]) # (num_facets, dim)

    # Cosine similarity matrix for all want/facet pairs via row-normalize + dot
    S = _row_normed(W) @ _row_normed(F).T                       # (num_wants, num_facets)

    # Map cosine sim from [-1, 1] -> [0, 1] and clamp low end
    S = np.clip((S + 1.0) / 2.0, 0.0, 1.0)

    # “A want can match any facet”: best facet per want, then average
    best_per_want = S.max(axis=1)                                # (num_wants,)
    aggregate_similarity = float(best_per_want.mean())

    # Facet contributions: mean score per facet across wants, then weight
    facet_means = S.mean(axis=0)                                 # (num_facets,)
    w = facet_weights.model_dump()                               # dict of weights
    weights = np.array([float(w.get(k, 0.0)) for k in facet_keys], dtype=np.float32)

    if float(weights.sum()) > 0:
        weighted_score = float((facet_means * weights).sum() / weights.sum())
    else:
        weighted_score = aggregate_similarity

    want_score = float(min(100.0, 100.0 * weighted_score))

    components = {
        "num_wants": int(W.shape[0]),
        "num_facets": int(F.shape[0]),
        "want_scores": best_per_want.tolist(),
        "aggregate_similarity": aggregate_similarity,
        "facet_weighted_score": weighted_score,
        "facet_contributions": {
            k: {
                "avg_score": float(m),
                "weight": float(wt),
                "contribution": float(m * wt),
            }
            for k, m, wt in zip(facet_keys, facet_means.tolist(), weights.tolist())
            if wt > 0
        },
        "want_score": want_score,
    }
    return want_score, components
