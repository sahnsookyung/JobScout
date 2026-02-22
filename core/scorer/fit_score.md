# Fit Score Tunable Parameters

They're the “knobs” that shape how strict matching is, how much each signal matters, and how hard you punish missing must-haves. They're also intentionally separated into (a) unitless fractions (0–1) that later become points, and (b) direct point penalties. (Cosine similarity itself is defined on [−1,1], and many embedding setups effectively use [0,1], which is why these defaults assume a 0–1 similarity scale.)​

## Similarity handling

### DEFAULT_REQ_SIMILARITY_THRESHOLD

This is the minimum cosine similarity score a requirement match must achieve to be treated as “real” coverage in _scaled_quality() (below it contributes 0). In v1.1, anything above the threshold contributes partially, scaled to 0–1, so 0.56 barely counts, while 0.95 counts almost fully.

Practical effect:

- Higher threshold ⇒ fewer matches count, but those that count are higher precision (less “fuzzy” matching).​
- Lower threshold ⇒ more matches count, but you’ll accept weaker semantic matches (higher recall, potentially more false positives).​

### DEFAULT_SIMILARITY_CLAMP

This clamps job_similarity and each match’s similarity into [0,1] before scoring, so misbehaving upstream code (e.g., returning -0.2 or 1.4) can’t silently distort the score.

Practical effect:

- Prevents core and coverage from exceeding expected bounds just because input similarities are out-of-range.
- Makes tuning meaningful because scores remain comparable across runs/environments.

## Core weights

### DEFAULT_WEIGHT_REQUIRED

This controls how strongly “requirements coverage” influences the core score relative to job-summary similarity. Core is computed as a weighted average (value × weight, divided by sum of weights), so bigger w_req pulls the score toward coverage more than textual similarity.​

Practical effect:

- Increasing this makes the scorer more “checklist-driven”: missing/marginal requirements hurt more even if the resume sounds broadly similar.

### DEFAULT_JOB_SIMILARITY_WEIGHT

This controls how much the overall resume↔job semantic similarity influences the core score. It’s useful as a backstop for cases where requirement extraction/matching misses something but the resume is obviously on-target.

Practical effect:

- Increasing this makes the scorer more “holistic”: a generally relevant candidate can score better even with imperfect requirement matching, because core is a weighted average.​

## Preferred bonus

### DEFAULT_PREFERRED_BONUS_MAX_FRACTION

This is the maximum fraction added on top of core due to preferred qualifications. In code, it becomes preferred_bonus_points = 100 * preferred_bonus_fraction, so 0.08 means “at most +8 points” if preferred coverage is perfect.

Practical effect:

- Preferred qualifications can only help, never hurt (because it’s additive and capped).
- Setting it low (like 0.08) ensures preferreds don’t overpower missing requireds.

## Missing required explicit penalty (points)

### DEFAULT_MISSING_REQUIRED_PENALTY_MAX (ratio-based)

This penalizes you based on how much required weight is missing:

missing_required_ratio * missing_required_penalty_max.

Practical effect:

- If you miss, say, 25% of required weight, this term alone can subtract about 10 points (0.25 × 40).
- It scales with job size naturally (missing 2 of 80 is a small ratio, so it’s a small penalty).

### DEFAULT_PER_MISSING_REQUIRED_PENALTY (count-based)

This penalizes you per missing required item:

missing_required_count * per_missing_required_penalty.

Practical effect:

- Ensures “missing one must-have” still hurts even if the job has many requirements and the ratio is tiny.

### DEFAULT_MISSING_REQUIRED_PENALTY_CAP

This caps the combined missing-required penalty (ratio + count) so it can’t run away and dominate everything else, especially if you also have separate fit_penalties.

Practical effect:

- Keeps scores from collapsing to ~0 for cases where missing required is already reflected in low required_coverage (i.e., reduces worst-case double punishment).

## Optional behavior toggle

### DEFAULT_ENABLE_EXPLICIT_MISSING_REQUIRED_PENALTY

This turns the explicit missing-required penalty on/off. When False, missing required items still hurt implicitly (because required coverage’s denominator includes missing), but you stop applying the extra explicit subtractive term.

Practical effect:

- True: stronger stance on must-haves (explicit deterrent).
- False: softer stance (only the coverage drop + other penalties apply).
