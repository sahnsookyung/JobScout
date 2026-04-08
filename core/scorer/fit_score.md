# Fit score math

Fit score is the capability signal.

It answers one question only: can this candidate do the job?

## Inputs

- `job_similarity`: overall dense similarity between the resume and job summary
- `matched_requirements`: requirement matches that cleared the semantic threshold
- `missing_requirements`: uncovered requirements
- `fit_penalties`: external penalty total from `core.scorer.penalties`

## Coverage

Required coverage is quality-weighted over the full required set:

$$
\text{required\_coverage} =
\frac{\sum (\text{required weight}_i \times \text{quality}_i)}{\sum \text{all required weight}_i}
$$

The denominator includes matched and missing required requirements, so uncovered required items reduce coverage directly.

Preferred coverage may still be reported as diagnostics metadata, but it does not change the fit score.

## Aggregation

The pre-penalty fit core blends required coverage with overall job similarity:

$$
\text{core} =
\frac{w_{req} \cdot \text{required\_coverage} + w_{sim} \cdot \text{job\_similarity}}{w_{req} + w_{sim}}
$$

## Final score

$$
\text{fit\_score} =
\mathrm{clamp}\left(
100 \cdot \text{core}
- \text{missing\_required\_penalty}
- \text{fit\_penalties},
0, 100
\right)
$$

## Separation of concerns

- Fit scoring is capability-only.
- Candidate preference semantics are calculated later as an independent `preference_score`.
- Ranking is the stage that blends `fit_score` and `preference_score`.
