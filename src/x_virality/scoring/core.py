from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from uuid import UUID

ZERO = Decimal("0")
HALF = Decimal("0.5")
ONE = Decimal("1")
HUNDRED = Decimal("100")
SCORE_QUANTUM = Decimal("0.0001")
CONFIDENCE_QUANTUM = Decimal("0.000001")

POST_SCORE_VERSION = "rules-v1"
POST_FEATURE_VERSION = "post-features-v1"
TOPIC_SCORE_VERSION = "topic-rules-v1"
TOPIC_FEATURE_VERSION = "topic-features-v1"

DEFAULT_POST_WEIGHTS: dict[str, Decimal] = {
    "velocity_percentile": Decimal("0.30"),
    "acceleration_percentile": Decimal("0.15"),
    "author_lift_percentile": Decimal("0.20"),
    "topic_spike_percentile": Decimal("0.20"),
    "conversation_spread_percentile": Decimal("0.10"),
    "impression_velocity_percentile": Decimal("0.05"),
}

# These are rules-v1 coefficients, not adaptive values. A change requires a new score version.
SPAM_PENALTY_WEIGHTS: dict[str, Decimal] = {
    "duplicate_wording": Decimal("0.08"),
    "one_author_concentration": Decimal("0.10"),
    "repost_only_activity": Decimal("0.12"),
    "coordinated_new_accounts": Decimal("0.10"),
    "niche_mismatch": Decimal("0.08"),
    "declining_velocity": Decimal("0.05"),
}
MAX_SPAM_PENALTY = Decimal("0.35")

TOPIC_POSITIVE_WEIGHTS: dict[str, Decimal] = {
    "topic_spike": Decimal("0.30"),
    "acceleration": Decimal("0.15"),
    "unique_author_growth": Decimal("0.20"),
    "independent_original_ratio": Decimal("0.20"),
    "text_diversity": Decimal("0.15"),
}
TOPIC_PENALTY_WEIGHTS: dict[str, Decimal] = {
    "author_concentration": Decimal("0.10"),
    "burst_decay": Decimal("0.10"),
    "repost_only_burst": Decimal("0.15"),
}
MAX_TOPIC_PENALTY = Decimal("0.30")


@dataclass(frozen=True, slots=True)
class MetricObservation:
    observed_at: datetime
    engagement: Decimal
    impressions: int | None = None


@dataclass(frozen=True, slots=True)
class IntervalFeatures:
    velocity: Decimal | None
    acceleration: Decimal | None
    impression_velocity: Decimal | None
    interval_seconds: Decimal | None
    trailing_interval_seconds: Decimal | None


@dataclass(frozen=True, slots=True)
class Percentile:
    raw: Decimal
    shrunk: Decimal
    cohort_size: int
    confidence: Decimal


@dataclass(frozen=True, slots=True)
class AuthorLift:
    ratio: Decimal
    mad_deviation: Decimal


@dataclass(frozen=True, slots=True)
class AppliedPenalty:
    name: str
    amount: Decimal
    source_value: Decimal | bool


@dataclass(frozen=True, slots=True)
class ScoreCalculation:
    score: Decimal
    positive_score: Decimal
    penalty: Decimal
    contributions: dict[str, Decimal]
    available_weight: Decimal


@dataclass(frozen=True, slots=True)
class GateDecision:
    passed: bool
    checks: dict[str, bool]


def decimal_value(value: Decimal | int | str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid decimal value: {value!r}") from error
    if not parsed.is_finite():
        raise ValueError("decimal values must be finite")
    return parsed


def clamp(value: Decimal, minimum: Decimal = ZERO, maximum: Decimal = ONE) -> Decimal:
    if minimum > maximum:
        raise ValueError("minimum cannot exceed maximum")
    return min(max(value, minimum), maximum)


def weighted_engagement(
    *,
    likes: int,
    reposts: int,
    quotes: int,
    replies: int,
    bookmarks: int | None,
    weights: Mapping[str, Decimal] | None = None,
) -> Decimal:
    """Return weighted engagement, omitting an unavailable bookmark term."""

    metrics = {
        "likes": likes,
        "reposts": reposts,
        "quotes": quotes,
        "replies": replies,
    }
    if any(value < 0 for value in metrics.values()) or (bookmarks is not None and bookmarks < 0):
        raise ValueError("engagement metrics cannot be negative")
    selected = weights or {
        "likes": Decimal("1"),
        "reposts": Decimal("2"),
        "quotes": Decimal("3"),
        "replies": Decimal("1.5"),
        "bookmarks": Decimal("0.25"),
    }
    result = sum(
        (Decimal(value) * decimal_value(selected[name]) for name, value in metrics.items()), ZERO
    )
    if bookmarks is not None:
        result += Decimal(bookmarks) * decimal_value(selected["bookmarks"])
    return result


def _elapsed_seconds(start: datetime, end: datetime) -> Decimal | None:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("observation timestamps must be timezone-aware")
    microseconds = (
        (end.astimezone(UTC) - start.astimezone(UTC)).days * 86_400_000_000
        + (end.astimezone(UTC) - start.astimezone(UTC)).seconds * 1_000_000
        + (end.astimezone(UTC) - start.astimezone(UTC)).microseconds
    )
    if microseconds <= 0:
        return None
    return Decimal(microseconds) / Decimal(1_000_000)


def actual_interval_features(observations: Sequence[MetricObservation]) -> IntervalFeatures:
    """Use the latest two/three real timestamps; nominal horizons never enter the math."""

    ordered = sorted(observations, key=lambda item: item.observed_at.astimezone(UTC))
    if len(ordered) < 2:
        return IntervalFeatures(None, None, None, None, None)
    previous, current = ordered[-2:]
    short_seconds = _elapsed_seconds(previous.observed_at, current.observed_at)
    if short_seconds is None:
        return IntervalFeatures(None, None, None, None, None)
    velocity = (current.engagement - previous.engagement) / short_seconds
    impression_velocity: Decimal | None = None
    if previous.impressions is not None and current.impressions is not None:
        impression_velocity = Decimal(current.impressions - previous.impressions) / short_seconds

    acceleration: Decimal | None = None
    trailing_seconds: Decimal | None = None
    if len(ordered) >= 3:
        trailing = ordered[-3]
        trailing_seconds = _elapsed_seconds(trailing.observed_at, previous.observed_at)
        if trailing_seconds is not None:
            trailing_velocity = (previous.engagement - trailing.engagement) / trailing_seconds
            # Velocity samples are located at interval midpoints. This denominator is the
            # actual distance between those midpoints, including uneven collection delays.
            midpoint_seconds = (trailing_seconds + short_seconds) / Decimal("2")
            acceleration = (velocity - trailing_velocity) / midpoint_seconds
    return IntervalFeatures(
        velocity=velocity,
        acceleration=acceleration,
        impression_velocity=impression_velocity,
        interval_seconds=short_seconds,
        trailing_interval_seconds=trailing_seconds,
    )


def median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(decimal_value(value) for value in values)
    if not ordered:
        raise ValueError("median requires at least one value")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def median_absolute_deviation(
    values: Iterable[Decimal], *, center: Decimal | None = None
) -> Decimal:
    materialized = [decimal_value(value) for value in values]
    if not materialized:
        raise ValueError("MAD requires at least one value")
    selected_center = center if center is not None else median(materialized)
    return median(abs(value - selected_center) for value in materialized)


def midrank_percentile(value: Decimal, population: Iterable[Decimal]) -> Decimal:
    """Return [0, 1] percentile rank using half the exact-tie mass."""

    selected = decimal_value(value)
    cohort = [decimal_value(item) for item in population]
    if not cohort:
        return HALF
    lower = sum(item < selected for item in cohort)
    tied = sum(item == selected for item in cohort)
    return (Decimal(lower) + Decimal(tied) / Decimal("2")) / Decimal(len(cohort))


def shrink_percentile(
    percentile: Decimal,
    cohort_size: int,
    minimum_cohort_size: int,
) -> Percentile:
    if cohort_size < 0:
        raise ValueError("cohort_size cannot be negative")
    if minimum_cohort_size < 1:
        raise ValueError("minimum_cohort_size must be positive")
    confidence = min(ONE, Decimal(cohort_size) / Decimal(minimum_cohort_size))
    raw = clamp(decimal_value(percentile))
    shrunk = HALF + (raw - HALF) * confidence
    return Percentile(
        raw=raw,
        shrunk=clamp(shrunk),
        cohort_size=cohort_size,
        confidence=confidence.quantize(CONFIDENCE_QUANTUM, rounding=ROUND_HALF_UP),
    )


def percentile_with_shrinkage(
    value: Decimal,
    population: Iterable[Decimal],
    *,
    minimum_cohort_size: int,
) -> Percentile:
    cohort = list(population)
    return shrink_percentile(midrank_percentile(value, cohort), len(cohort), minimum_cohort_size)


def logarithmic_follower_bucket(followers: int | None) -> int | None:
    if followers is None:
        return None
    if followers < 0:
        raise ValueError("followers cannot be negative")
    return max(0, followers).bit_length() - 1 if followers else 0


def calculate_author_lift(
    engagement: Decimal,
    *,
    median_engagement: Decimal,
    engagement_mad: Decimal,
) -> AuthorLift:
    current = decimal_value(engagement)
    baseline = decimal_value(median_engagement)
    mad = decimal_value(engagement_mad)
    if baseline < ZERO or mad < ZERO:
        raise ValueError("baseline values cannot be negative")
    if baseline > ZERO:
        ratio = current / baseline
    elif current == ZERO:
        ratio = ONE
    else:
        # A zero baseline has no finite mathematical ratio. A +1 pseudocount keeps
        # evidence JSON finite while preserving monotonicity and a conservative gate.
        ratio = current + ONE
    scale = max(mad, ONE)
    return AuthorLift(ratio=ratio, mad_deviation=(current - baseline) / scale)


def applied_penalties(
    facts: Mapping[str, Decimal | bool],
    *,
    weights: Mapping[str, Decimal] = SPAM_PENALTY_WEIGHTS,
    maximum: Decimal = MAX_SPAM_PENALTY,
) -> tuple[AppliedPenalty, ...]:
    penalties: list[AppliedPenalty] = []
    for name in sorted(weights):
        source = facts.get(name, False)
        active = source if isinstance(source, bool) else source > ZERO
        if active:
            penalties.append(
                AppliedPenalty(name=name, amount=decimal_value(weights[name]), source_value=source)
            )
    total = sum((item.amount for item in penalties), ZERO)
    if total <= maximum:
        return tuple(penalties)
    scale = maximum / total
    return tuple(
        AppliedPenalty(item.name, item.amount * scale, item.source_value) for item in penalties
    )


def renormalized_score(
    components: Mapping[str, Decimal | None],
    weights: Mapping[str, Decimal],
    *,
    penalty: Decimal = ZERO,
) -> ScoreCalculation:
    unknown = set(components) - set(weights)
    if unknown:
        raise ValueError(f"unknown score components: {sorted(unknown)}")
    available = {
        name: clamp(decimal_value(value)) for name, value in components.items() if value is not None
    }
    available_weight = sum((decimal_value(weights[name]) for name in available), ZERO)
    if available_weight <= ZERO:
        positive = ZERO
        contributions: dict[str, Decimal] = {}
    else:
        contributions = {
            name: decimal_value(weights[name]) * value / available_weight
            for name, value in available.items()
        }
        positive = sum(contributions.values(), ZERO)
    selected_penalty = max(ZERO, decimal_value(penalty))
    score = HUNDRED * clamp(positive - selected_penalty)
    return ScoreCalculation(
        score=score.quantize(SCORE_QUANTUM, rounding=ROUND_HALF_UP),
        positive_score=positive,
        penalty=selected_penalty,
        contributions=contributions,
        available_weight=available_weight,
    )


def calculate_post_score(
    components: Mapping[str, Decimal | None],
    *,
    spam_penalty: Decimal,
    weights: Mapping[str, Decimal] = DEFAULT_POST_WEIGHTS,
) -> ScoreCalculation:
    return renormalized_score(components, weights, penalty=spam_penalty)


def calculate_topic_score(
    components: Mapping[str, Decimal | None],
    *,
    topic_penalty: Decimal,
) -> ScoreCalculation:
    return renormalized_score(
        components,
        TOPIC_POSITIVE_WEIGHTS,
        penalty=min(MAX_TOPIC_PENALTY, max(ZERO, topic_penalty)),
    )


def breakout_gate(
    *,
    score: Decimal,
    velocity_percentile: Decimal | None,
    author_lift: Decimal | None,
    score_min: Decimal = Decimal("85"),
    velocity_percentile_min: Decimal = Decimal("90"),
    author_lift_min: Decimal = Decimal("2"),
) -> GateDecision:
    configured_percentile = decimal_value(velocity_percentile_min)
    threshold = (
        configured_percentile / HUNDRED if configured_percentile > ONE else configured_percentile
    )
    checks = {
        "score": decimal_value(score) >= decimal_value(score_min),
        "velocity_percentile": (
            velocity_percentile is not None and decimal_value(velocity_percentile) >= threshold
        ),
        "author_lift": author_lift is not None and decimal_value(author_lift) >= author_lift_min,
    }
    return GateDecision(passed=all(checks.values()), checks=checks)


def emerging_topic_gate(
    *,
    score: Decimal,
    unique_authors: int,
    topic_spike: Decimal | None,
    score_min: Decimal = Decimal("82"),
    unique_authors_min: int = 6,
    topic_spike_min: Decimal = Decimal("3"),
) -> GateDecision:
    checks = {
        "score": decimal_value(score) >= decimal_value(score_min),
        "unique_authors": unique_authors >= unique_authors_min,
        "topic_spike": (topic_spike is not None and decimal_value(topic_spike) >= topic_spike_min),
    }
    return GateDecision(passed=all(checks.values()), checks=checks)


def _json_value(value: object) -> Any:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical JSON cannot encode non-finite Decimal values")
        normalized = value.normalize()
        return "0" if normalized == ZERO else format(normalized, "f")
    if isinstance(value, float):
        return _json_value(decimal_value(str(value)))
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("canonical JSON timestamps must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def json_safe(value: object) -> Any:
    return _json_value(value)


def canonical_json(value: object) -> str:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_input_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def implementation_hash(*, score_version: str) -> str:
    if score_version == POST_SCORE_VERSION:
        specification: object = {
            "version": POST_SCORE_VERSION,
            "feature_version": POST_FEATURE_VERSION,
            "weights": DEFAULT_POST_WEIGHTS,
            "spam_weights": SPAM_PENALTY_WEIGHTS,
            "max_spam_penalty": MAX_SPAM_PENALTY,
            "percentile": "midrank",
            "cold_start": "linear-to-neutral",
            "missing": "positive-weight-renormalization",
        }
    elif score_version == TOPIC_SCORE_VERSION:
        specification = {
            "version": TOPIC_SCORE_VERSION,
            "feature_version": TOPIC_FEATURE_VERSION,
            "positive_weights": TOPIC_POSITIVE_WEIGHTS,
            "penalty_weights": TOPIC_PENALTY_WEIGHTS,
            "max_topic_penalty": MAX_TOPIC_PENALTY,
        }
    else:
        raise ValueError(f"unknown score version: {score_version}")
    return canonical_input_hash(specification)


def floor_seconds(start: datetime, end: datetime) -> int:
    elapsed = _elapsed_seconds(start, end)
    if elapsed is None:
        return 0
    return int(elapsed.to_integral_value(rounding=ROUND_FLOOR))
