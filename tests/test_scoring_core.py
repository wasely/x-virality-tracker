from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from x_virality.scoring.core import (
    MAX_SPAM_PENALTY,
    MetricObservation,
    actual_interval_features,
    applied_penalties,
    breakout_gate,
    calculate_post_score,
    emerging_topic_gate,
    midrank_percentile,
    percentile_with_shrinkage,
    renormalized_score,
    weighted_engagement,
)


def test_weighted_engagement_uses_exact_decimal_formula_and_omits_missing_bookmarks() -> None:
    inputs = {
        "likes": 1,
        "reposts": 2,
        "quotes": 3,
        "replies": 4,
    }

    assert weighted_engagement(**inputs, bookmarks=5) == Decimal("21.25")
    assert weighted_engagement(**inputs, bookmarks=None) == Decimal("20.0")
    assert isinstance(weighted_engagement(**inputs, bookmarks=5), Decimal)


def test_interval_features_use_actual_uneven_timestamps() -> None:
    start = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
    observations = [
        MetricObservation(start + timedelta(seconds=40), Decimal("110"), 220),
        MetricObservation(start, Decimal("0"), 100),
        MetricObservation(start + timedelta(seconds=10), Decimal("20"), 130),
    ]

    features = actual_interval_features(observations)

    assert features.velocity == Decimal("3")
    assert features.acceleration == Decimal("0.05")
    assert features.impression_velocity == Decimal("3")
    assert features.interval_seconds == Decimal("30")
    assert features.trailing_interval_seconds == Decimal("10")


def test_post_score_matches_rules_v1_decimal_formula() -> None:
    calculation = calculate_post_score(
        {
            "velocity_percentile": Decimal("0.9"),
            "acceleration_percentile": Decimal("0.8"),
            "author_lift_percentile": Decimal("0.7"),
            "topic_spike_percentile": Decimal("0.6"),
            "conversation_spread_percentile": Decimal("0.5"),
            "impression_velocity_percentile": Decimal("0.4"),
        },
        spam_penalty=Decimal("0.08"),
    )

    assert calculation.positive_score == Decimal("0.72")
    assert calculation.penalty == Decimal("0.08")
    assert calculation.score == Decimal("64.0000")


def test_unavailable_positive_weight_is_renormalized_but_penalty_is_not() -> None:
    calculation = renormalized_score(
        {"available": Decimal("0.8"), "missing": None},
        {"available": Decimal("0.25"), "missing": Decimal("0.75")},
        penalty=Decimal("0.1"),
    )

    assert calculation.available_weight == Decimal("0.25")
    assert calculation.contributions == {"available": Decimal("0.8")}
    assert calculation.positive_score == Decimal("0.8")
    assert calculation.score == Decimal("70.0000")


def test_midrank_ties_and_cold_start_shrink_toward_neutral() -> None:
    assert midrank_percentile(
        Decimal("2"),
        [Decimal("1"), Decimal("2"), Decimal("2"), Decimal("4")],
    ) == Decimal("0.5")

    sparse = percentile_with_shrinkage(
        Decimal("2"),
        [Decimal("1"), Decimal("2")],
        minimum_cohort_size=4,
    )
    empty = percentile_with_shrinkage(
        Decimal("99"),
        [],
        minimum_cohort_size=4,
    )

    assert sparse.raw == Decimal("0.75")
    assert sparse.confidence == Decimal("0.500000")
    assert sparse.shrunk == Decimal("0.625")
    assert empty.raw == empty.shrunk == Decimal("0.5")
    assert empty.confidence == Decimal("0.000000")


def test_spam_penalties_are_named_deterministically_and_capped() -> None:
    penalties = applied_penalties(
        {
            "duplicate_wording": True,
            "one_author_concentration": Decimal("0.75"),
            "repost_only_activity": True,
            "coordinated_new_accounts": True,
            "niche_mismatch": False,
            "declining_velocity": Decimal("0"),
        }
    )

    assert [penalty.name for penalty in penalties] == [
        "coordinated_new_accounts",
        "duplicate_wording",
        "one_author_concentration",
        "repost_only_activity",
    ]
    assert sum((penalty.amount for penalty in penalties), Decimal("0")) == MAX_SPAM_PENALTY


@pytest.mark.parametrize(
    ("overrides", "failed_check"),
    [
        ({"score": Decimal("84.9999")}, "score"),
        ({"velocity_percentile": Decimal("0.899999")}, "velocity_percentile"),
        ({"author_lift": Decimal("1.9999")}, "author_lift"),
    ],
)
def test_breakout_gate_is_inclusive_and_requires_every_threshold(
    overrides: dict[str, Decimal], failed_check: str
) -> None:
    inputs = {
        "score": Decimal("85"),
        "velocity_percentile": Decimal("0.90"),
        "author_lift": Decimal("2"),
    }
    assert breakout_gate(**inputs).passed

    failed = breakout_gate(**(inputs | overrides))
    assert not failed.passed
    assert not failed.checks[failed_check]


@pytest.mark.parametrize(
    ("overrides", "failed_check"),
    [
        ({"score": Decimal("81.9999")}, "score"),
        ({"unique_authors": 5}, "unique_authors"),
        ({"topic_spike": Decimal("2.9999")}, "topic_spike"),
    ],
)
def test_emerging_topic_gate_is_inclusive_and_requires_every_threshold(
    overrides: dict[str, Decimal | int], failed_check: str
) -> None:
    inputs: dict[str, Decimal | int] = {
        "score": Decimal("82"),
        "unique_authors": 6,
        "topic_spike": Decimal("3"),
    }
    assert emerging_topic_gate(**inputs).passed  # type: ignore[arg-type]

    failed = emerging_topic_gate(**(inputs | overrides))  # type: ignore[arg-type]
    assert not failed.passed
    assert not failed.checks[failed_check]
