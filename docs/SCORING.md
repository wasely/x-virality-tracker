# Scoring Specification

All detection is deterministic and versioned. Calculations use `Decimal`; ties use midranks; only observations with `observed_at <= as_of` may contribute.

## Observation horizons

Each discovered post receives target ages:

```text
120, 300, 600, 1200, 2400, 3600 seconds
```

`scheduled_for = posted_at + target_age`. Completed observations record target age, actual age, lateness, collector version, metric availability, and quality. A post discovered after a horizon does not receive invented historical metrics: expired target rows are `skipped_late` with observation/age/lateness fields left null, while one real discovery observation is retained at its actual age.

## Engagement v1

```text
likes + 2×reposts + 3×quotes + 1.5×replies + 0.25×bookmarks
```

Bookmarks and impressions can be unavailable. Missing means null, never zero. Engagement excludes the unavailable bookmark term rather than estimating it.

Velocity is an observed engagement delta divided by the actual elapsed seconds between real observations. Acceleration compares short and trailing real intervals. Impression velocity is computed only when both endpoints expose impressions. Labels never imply a nominal interval that was not observed.

## Cohorts and baselines

Comparisons prefer the same:

1. target-age horizon,
2. niche,
3. author history where sufficient.

Author lift uses the author's rolling same-horizon median and median absolute deviation (MAD). Sparse authors fall back to the same niche and logarithmic follower bucket, then niche/global history. Cohort size and fallback are evidence fields.

Percentiles use midranks for ties. Cohorts smaller than `alert_policy.min_cohort_size` are shrunk toward neutral rather than treated as high-confidence extremes.

## Post score `rules-v1`

Available percentile components are normalized to `[0, 1]`:

```text
100 × clamp(
    0.30 × velocity_percentile
  + 0.15 × acceleration_percentile
  + 0.20 × author_lift_percentile
  + 0.20 × topic_spike_percentile
  + 0.10 × conversation_spread_percentile
  + 0.05 × impression_velocity_percentile
  - spam_penalty,
  0, 1)
```

When a component is genuinely unavailable, its positive weight is removed and the remaining available positive weights are renormalized to one. A spam penalty is never renormalized away.

Spam evidence includes duplicate wording, one-author concentration, repost-only activity, coordinated new-account concentration, niche mismatch, and declining velocity. Each applied penalty and source fact is persisted.

### Breakout gate

A post alert requires all of:

- score at least `85`,
- velocity percentile at least `90%`,
- author lift at least `2×` baseline.

The alert policy/config hash is part of the dedupe key and evidence.

## Topic grouping

Signals are evaluated in priority order:

1. conversation/root/original/quoted post identity,
2. canonical external URL,
3. sufficiently specific context entity/hashtag signature.

URL canonicalization removes fragments and tracking parameters, normalizes host/path, and ignores X self-links. Every signal records kind, normalized value, strength, and first-seen time. Stronger evidence can transactionally merge clusters; old cluster IDs become explicit aliases through `merged_into_id`.

Current limitation: original and quoted identities use different signal kinds (`original_post` and `quoted_post`). A quoted post can therefore group with other quotes without necessarily joining the quoted original. Treat quoted-original identity unification as unfinished.

There is an interface seam for future semantic providers, but release v1 has no embeddings or LLM grouping.

## Topic features and score

A rolling topic snapshot records:

- post and unique-author growth,
- independent originals,
- repost/quote/reply mix,
- text diversity,
- new-author ratio,
- largest-author concentration,
- expected unique authors and baseline spike,
- acceleration and burst decay.

Topic score has its own version and coefficients in `score_versions`. It rewards topic spike, acceleration, unique-author growth, independent-original ratio, and diversity; it penalizes concentration and decaying/repost-only bursts. Components, baseline, confidence, explanation, and canonical input hash are persisted.

### Emerging-conversation gate

A topic alert requires all of:

- topic score at least `82`,
- at least `6` unique authors,
- activity at least `3×` baseline.

## Versioning and reproducibility

`score_versions` stores feature version, coefficients, implementation hash, activation state, and timestamp. Post/topic score uniqueness and canonical input hashes support idempotent replay comparison. They do **not** make rows immutable: there are no database update-prevention triggers, and current replay checks compare the input hash rather than every stored output field. Operational controls must protect evidence tables; new formula semantics should use a new version instead of rewriting old rows.

Current `rules-v1` implementation hashing uses built-in default weights while YAML exposes configurable weights. Until validation freezes those defaults or hashing covers the complete configured semantics, changing weights under the same version is unsupported.

Good/Bad Discord feedback is training/evaluation data only. It does not alter live `rules-v1` detection.
