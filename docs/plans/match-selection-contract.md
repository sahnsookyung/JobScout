# Match Selection Contract

## Purpose

This document defines the canonical contract for how JobScout turns a scored
candidate pool into the persisted match set that powers notifications and the
default `/api/matches` experience.

The core goal is to stop re-deriving match membership and ordering in multiple
layers. The scorer-matcher pipeline publishes one committed selection run, and
downstream consumers read that artifact instead of rebuilding their own hidden
policy.

## Two Layers

### 1. Selection Preparation

Selection preparation gathers the inputs to ranking without deciding final
membership yet.

It includes:

- owner-scoped canonical resume resolution
- vector retrieval
- hard candidate-preference filters
- fit scoring
- soft preference scoring

### 2. Selection Engine

The selection engine is the canonical policy boundary.

It applies:

- fit floor
- required coverage floor
- ranking mode (`fit_first`, `preference_first`, `balanced`)
- `top_k` truncation
- alert subset derivation
- ranking explanation snapshots

This logic lives in [`core/match_selection/engine.py`](/Users/sookyungahn/repos/JobScout/core/match_selection/engine.py).

## Run Artifacts

The canonical published output is:

- [`match_selection_run`](/Users/sookyungahn/repos/JobScout/database/models/match_selection.py)
- [`match_selection_item`](/Users/sookyungahn/repos/JobScout/database/models/match_selection.py)

`JobMatch` remains the mutable current-state row for detailed match data and
user actions such as hiding/notified status. The selection-run artifact is the
authoritative record of which matches a run selected and in what canonical
order.

## Publication Protocol

Publication happens after match rows are saved successfully.

The intended transaction semantics are:

1. Create a new run in `pending`.
2. Write all selection items.
3. Supersede the previous current committed run for the same owner/resume.
4. Mark the new run `committed` and `is_current=true`.
5. Commit once.

Idempotency is keyed by `(resume_fingerprint, task_id)` for committed runs.
If a committed run already exists for that task, publication returns the
existing run instead of creating a second one.

## Read Source

Committed selection artifacts are the only canonical source for match
membership and ordering.

Consumers read:

1. committed `match_selection_run`
2. committed `match_selection_item`

## Web Match List

The default web match list now works like this:

1. Resolve the canonical resume selection.
2. If a committed selection run exists, load selection items for membership.
3. Apply request filters like `status`, `min_fit`, `remote_only`, `show_hidden`.
4. Apply request-time `ranking_mode` as presentation reranking over that fixed
   membership set.
5. Apply `top_k`.

Temporary `/api/matches?ranking_mode=...` overrides change presentation order
only. They do not change canonical persisted membership and they do not affect
notifications.

## Notifications

Notifications consume the committed selection run for the current pipeline run
when available.

Eligibility rules are:

- match is in the committed selected set
- match is still `active`
- match is not hidden
- match clears the fit-based alert floor

The alert floor is fit-only. Preference is used for ranking order, not as a
hard threshold.

## Hidden Semantics

Hiding a match does not rewrite historical run membership.

Instead:

- the match remains part of the published historical run artifact
- default web presentation excludes hidden matches
- notifications skip hidden matches at send time

## Snapshot Fields

Run-level policy is persisted as `policy_snapshot_json` on
`match_selection_run`.

Scalar projections are also stored for indexed/common queries:

- `ranking_mode_used`
- `ranking_config_version`
- `stable_tie_break_key`
- `fit_floor_used`
- `notification_fit_floor_used`
- `top_k_used`
- `candidate_pool_size`
- `selected_count`
- `alert_candidate_count`
- `resume_resolution_reason`

Per-item snapshots include:

- `rank_position`
- `fit_score_at_selection`
- `preference_score_at_selection`
- `job_similarity_at_selection`
- `required_coverage_at_selection`
- `alert_eligible`
- `dominant_reason_code`
- `explanation_label`
- `ranking_snapshot`

## Non-Goals

This contract does not currently:

- persist a separate presentation-only rerank artifact
- let request-time ranking overrides change alerts
