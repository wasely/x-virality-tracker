from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import exists, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import RulesConfig
from x_virality.db.models import (
    Alert,
    Author,
    AuthorBaseline,
    Post,
    PostMetricSnapshot,
    PostRuleMatch,
    PostScore,
    ScoreVersion,
    TopicCluster,
    TopicMember,
    TopicScore,
    TopicSnapshot,
    XRule,
)
from x_virality.domain.models import AlertCandidate
from x_virality.scoring.core import (
    CONFIDENCE_QUANTUM,
    DEFAULT_POST_WEIGHTS,
    MAX_SPAM_PENALTY,
    MAX_TOPIC_PENALTY,
    POST_FEATURE_VERSION,
    POST_SCORE_VERSION,
    SPAM_PENALTY_WEIGHTS,
    TOPIC_FEATURE_VERSION,
    TOPIC_PENALTY_WEIGHTS,
    TOPIC_POSITIVE_WEIGHTS,
    TOPIC_SCORE_VERSION,
    AppliedPenalty,
    GateDecision,
    MetricObservation,
    Percentile,
    actual_interval_features,
    applied_penalties,
    breakout_gate,
    calculate_author_lift,
    calculate_post_score,
    calculate_topic_score,
    canonical_input_hash,
    clamp,
    emerging_topic_gate,
    implementation_hash,
    json_safe,
    logarithmic_follower_bucket,
    median,
    median_absolute_deviation,
    percentile_with_shrinkage,
)

_OBSERVED_QUALITIES = ("observed", "late")
_MAX_COHORT_ROWS = 2_000
_MIN_AUTHOR_HISTORY = 5
_GLOBAL_BASELINE_NICHE = "__global__"


@dataclass(frozen=True, slots=True)
class BaselineSelection:
    scope: str
    sample_count: int
    median_engagement: Decimal
    median_velocity: Decimal
    engagement_mad: Decimal
    follower_bucket: int | None
    baseline_id: UUID


@dataclass(frozen=True, slots=True)
class PostScoringResult:
    score_id: UUID
    post_id: str
    snapshot_id: UUID
    score: Decimal
    input_hash: str
    created: bool
    gate: GateDecision
    candidate: AlertCandidate | None


@dataclass(frozen=True, slots=True)
class TopicScoringResult:
    score_id: UUID
    cluster_id: UUID
    topic_snapshot_id: UUID
    score: Decimal
    input_hash: str
    created: bool
    gate: GateDecision
    candidate: AlertCandidate | None


@dataclass(frozen=True, slots=True)
class ScoringWorkflowResult:
    post: PostScoringResult
    topic: TopicScoringResult | None
    candidates: tuple[AlertCandidate, ...]


@dataclass(frozen=True, slots=True)
class _CohortRecord:
    snapshot: PostMetricSnapshot
    post: Post
    author: Author | None
    followers: int | None


@dataclass(frozen=True, slots=True)
class _PostContext:
    cluster: TopicCluster | None
    topic_key: str
    niche_key: str
    rule_niche_key: str


def _decimal_optional(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _post_weights(rules: RulesConfig) -> dict[str, Decimal]:
    dumped = rules.scoring.post_weights.model_dump()
    weights = {name: Decimal(str(value)) for name, value in dumped.items()}
    if set(weights) != set(DEFAULT_POST_WEIGHTS):
        raise ValueError("rules-v1 post component names cannot change")
    return weights


def _version_coefficients(rules: RulesConfig, version: str) -> dict[str, Any]:
    if version == POST_SCORE_VERSION:
        return {
            "positive_weights": _post_weights(rules),
            "spam_penalty_weights": SPAM_PENALTY_WEIGHTS,
            "maximum_spam_penalty": MAX_SPAM_PENALTY,
            "missing_components": "renormalize-positive-weights",
            "percentile": "midrank",
            "cold_start": "linear-shrink-to-0.5",
        }
    if version == TOPIC_SCORE_VERSION:
        return {
            "positive_weights": TOPIC_POSITIVE_WEIGHTS,
            "penalty_weights": TOPIC_PENALTY_WEIGHTS,
            "maximum_topic_penalty": MAX_TOPIC_PENALTY,
            "missing_components": "renormalize-positive-weights",
            "percentile": "midrank",
            "cold_start": "linear-shrink-to-0.5",
        }
    raise ValueError(f"unsupported immutable score version: {version}")


async def ensure_score_version(
    session: AsyncSession,
    *,
    version: str,
    rules: RulesConfig,
    activated_at: datetime,
) -> ScoreVersion:
    if activated_at.tzinfo is None:
        raise ValueError("activated_at must be timezone-aware")
    if version == POST_SCORE_VERSION and rules.scoring.version != POST_SCORE_VERSION:
        raise ValueError("rules-v1 implementation requires scoring.version=rules-v1")
    feature_version = (
        POST_FEATURE_VERSION if version == POST_SCORE_VERSION else TOPIC_FEATURE_VERSION
    )
    coefficients = json_safe(_version_coefficients(rules, version))
    selected_implementation_hash = implementation_hash(score_version=version)
    # Version registration is serialized, then validated rather than overwritten.
    await session.execute(
        select(func.pg_advisory_xact_lock(func.hashtextextended(f"score-version:{version}", 0)))
    )
    stored = await session.scalar(
        select(ScoreVersion).where(ScoreVersion.version == version).with_for_update()
    )
    if stored is None:
        stored = ScoreVersion(
            version=version,
            feature_version=feature_version,
            coefficients=coefficients,
            implementation_hash=selected_implementation_hash,
            active=True,
            activated_at=activated_at,
        )
        session.add(stored)
        await session.flush()
        return stored
    expected_hash = canonical_input_hash(coefficients)
    stored_hash = canonical_input_hash(stored.coefficients)
    if (
        stored.feature_version != feature_version
        or stored.implementation_hash != selected_implementation_hash
        or stored_hash != expected_hash
    ):
        raise RuntimeError(
            f"immutable score version {version} is registered with different semantics"
        )
    return stored


async def _post_context(session: AsyncSession, post_id: str) -> _PostContext:
    cluster = await session.scalar(
        select(TopicCluster)
        .join(TopicMember, TopicMember.cluster_id == TopicCluster.id)
        .where(TopicMember.post_id == post_id, TopicCluster.state == "active")
        .order_by(TopicCluster.canonical_key, TopicCluster.id)
        .limit(1)
    )
    rule_row = (
        await session.execute(
            select(XRule.topic_key, XRule.niche_key)
            .join(PostRuleMatch, PostRuleMatch.rule_id == XRule.id)
            .where(PostRuleMatch.post_id == post_id)
            .order_by(XRule.local_key, XRule.topic_key, XRule.niche_key)
            .limit(1)
        )
    ).first()
    rule_topic = str(rule_row[0]) if rule_row is not None else "unclassified"
    rule_niche = str(rule_row[1]) if rule_row is not None else "global"
    return _PostContext(
        cluster=cluster,
        topic_key=cluster.topic_key if cluster is not None else rule_topic,
        niche_key=cluster.niche_key if cluster is not None else rule_niche,
        rule_niche_key=rule_niche,
    )


def _niche_predicate(niche_key: str) -> Any:
    rule_match = exists(
        select(1)
        .select_from(PostRuleMatch)
        .join(XRule, XRule.id == PostRuleMatch.rule_id)
        .where(PostRuleMatch.post_id == Post.id, XRule.niche_key == niche_key)
    )
    topic_match = exists(
        select(1)
        .select_from(TopicMember)
        .join(TopicCluster, TopicCluster.id == TopicMember.cluster_id)
        .where(
            TopicMember.post_id == Post.id,
            TopicCluster.state == "active",
            TopicCluster.niche_key == niche_key,
        )
    )
    return or_(rule_match, topic_match)


async def _target_records(
    session: AsyncSession,
    *,
    target_age_seconds: int,
    collector_version: str,
    as_of: datetime,
    niche_key: str | None,
    before: datetime | None = None,
) -> list[_CohortRecord]:
    statement = (
        select(PostMetricSnapshot, Post, Author)
        .join(Post, Post.id == PostMetricSnapshot.post_id)
        .outerjoin(Author, Author.id == Post.author_id)
        .where(
            PostMetricSnapshot.target_age_seconds == target_age_seconds,
            PostMetricSnapshot.collector_version == collector_version,
            PostMetricSnapshot.quality.in_(_OBSERVED_QUALITIES),
            PostMetricSnapshot.engagement.is_not(None),
            PostMetricSnapshot.observed_at.is_not(None),
            PostMetricSnapshot.observed_at <= as_of,
        )
        .order_by(
            PostMetricSnapshot.observed_at.desc(),
            PostMetricSnapshot.id.desc(),
        )
        .limit(_MAX_COHORT_ROWS)
    )
    if before is not None:
        statement = statement.where(PostMetricSnapshot.observed_at < before)
    if niche_key is not None:
        statement = statement.where(_niche_predicate(niche_key))
    rows = (await session.execute(statement)).all()
    records = [
        _CohortRecord(
            snapshot=row[0],
            post=row[1],
            author=row[2],
            followers=(
                row[0].author_followers
                if row[0].author_followers is not None
                else (row[2].followers_count if row[2] is not None else None)
            ),
        )
        for row in rows
    ]
    records.sort(
        key=lambda item: (
            item.snapshot.observed_at or datetime.min.replace(tzinfo=UTC),
            str(item.snapshot.id),
        )
    )
    return records


async def _observation_map(
    session: AsyncSession,
    post_ids: Sequence[str],
    *,
    as_of: datetime,
) -> dict[str, list[PostMetricSnapshot]]:
    if not post_ids:
        return {}
    snapshots = list(
        (
            await session.scalars(
                select(PostMetricSnapshot)
                .where(
                    PostMetricSnapshot.post_id.in_(sorted(set(post_ids))),
                    PostMetricSnapshot.quality.in_(_OBSERVED_QUALITIES),
                    PostMetricSnapshot.engagement.is_not(None),
                    PostMetricSnapshot.observed_at.is_not(None),
                    PostMetricSnapshot.observed_at <= as_of,
                )
                .order_by(
                    PostMetricSnapshot.post_id,
                    PostMetricSnapshot.observed_at,
                    PostMetricSnapshot.id,
                )
            )
        ).all()
    )
    grouped: dict[str, list[PostMetricSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.post_id].append(snapshot)
    return dict(grouped)


def _interval_for(
    record: _CohortRecord,
    observations: Mapping[str, Sequence[PostMetricSnapshot]],
) -> Any:
    cutoff = record.snapshot.observed_at
    if cutoff is None:
        return actual_interval_features(())
    points = [
        MetricObservation(
            observed_at=item.observed_at,
            engagement=item.engagement,
            impressions=item.impressions,
        )
        for item in observations.get(record.post.id, ())
        if item.observed_at is not None
        and item.observed_at <= cutoff
        and item.engagement is not None
    ]
    return actual_interval_features(points)


def _baseline_row_scope(
    baseline: AuthorBaseline, *, author_id: str, follower_bucket: int | None
) -> str:
    if baseline.author_id == author_id:
        return "author"
    if baseline.niche_key == _GLOBAL_BASELINE_NICHE:
        return "global"
    if baseline.follower_bucket == follower_bucket and follower_bucket is not None:
        return "niche_follower_bucket"
    return "niche"


async def _stored_baseline(
    session: AsyncSession,
    *,
    author_id: str,
    niche_key: str,
    target_age_seconds: int,
    follower_bucket: int | None,
    as_of: datetime,
) -> BaselineSelection | None:
    common = (
        AuthorBaseline.target_age_seconds == target_age_seconds,
        AuthorBaseline.as_of <= as_of,
    )
    author_row = await session.scalar(
        select(AuthorBaseline)
        .where(
            *common,
            AuthorBaseline.author_id == author_id,
            AuthorBaseline.niche_key == niche_key,
            AuthorBaseline.sample_count >= _MIN_AUTHOR_HISTORY,
        )
        .order_by(AuthorBaseline.as_of.desc(), AuthorBaseline.id.desc())
        .limit(1)
    )
    selected = author_row
    if selected is None and follower_bucket is not None:
        selected = await session.scalar(
            select(AuthorBaseline)
            .where(
                *common,
                AuthorBaseline.author_id.is_(None),
                AuthorBaseline.niche_key == niche_key,
                AuthorBaseline.follower_bucket == follower_bucket,
            )
            .order_by(AuthorBaseline.as_of.desc(), AuthorBaseline.id.desc())
            .limit(1)
        )
    if selected is None:
        selected = await session.scalar(
            select(AuthorBaseline)
            .where(
                *common,
                AuthorBaseline.author_id.is_(None),
                AuthorBaseline.niche_key == niche_key,
                AuthorBaseline.follower_bucket.is_(None),
            )
            .order_by(AuthorBaseline.as_of.desc(), AuthorBaseline.id.desc())
            .limit(1)
        )
    if selected is None:
        selected = await session.scalar(
            select(AuthorBaseline)
            .where(
                *common,
                AuthorBaseline.author_id.is_(None),
                AuthorBaseline.niche_key == _GLOBAL_BASELINE_NICHE,
                AuthorBaseline.follower_bucket.is_(None),
            )
            .order_by(AuthorBaseline.as_of.desc(), AuthorBaseline.id.desc())
            .limit(1)
        )
    if selected is None:
        return None
    return BaselineSelection(
        scope=_baseline_row_scope(selected, author_id=author_id, follower_bucket=follower_bucket),
        sample_count=selected.sample_count,
        median_engagement=selected.median_engagement,
        median_velocity=selected.median_velocity,
        engagement_mad=selected.engagement_mad,
        follower_bucket=selected.follower_bucket,
        baseline_id=selected.id,
    )


async def _build_baseline(
    session: AsyncSession,
    *,
    current: _CohortRecord,
    context: _PostContext,
    as_of: datetime,
) -> BaselineSelection:
    observed_at = current.snapshot.observed_at
    if observed_at is None:
        raise ValueError("cannot build a baseline for an unobserved snapshot")
    bucket = logarithmic_follower_bucket(current.followers)
    niche_records = await _target_records(
        session,
        target_age_seconds=current.snapshot.target_age_seconds,
        collector_version=current.snapshot.collector_version,
        as_of=as_of,
        niche_key=context.niche_key,
        before=observed_at,
    )
    observations = await _observation_map(
        session, [item.post.id for item in niche_records], as_of=as_of
    )
    author_records = [
        item for item in niche_records if item.post.author_id == current.post.author_id
    ]
    bucket_records = (
        [item for item in niche_records if logarithmic_follower_bucket(item.followers) == bucket]
        if bucket is not None
        else []
    )
    if len(author_records) >= _MIN_AUTHOR_HISTORY:
        selected = author_records
        scope = "author"
        stored_author_id: str | None = current.post.author_id
        stored_bucket: int | None = None
        stored_niche = context.niche_key
    elif bucket_records:
        selected = bucket_records
        scope = "niche_follower_bucket"
        stored_author_id = None
        stored_bucket = bucket
        stored_niche = context.niche_key
    elif niche_records:
        selected = niche_records
        scope = "niche"
        stored_author_id = None
        stored_bucket = None
        stored_niche = context.niche_key
    else:
        selected = await _target_records(
            session,
            target_age_seconds=current.snapshot.target_age_seconds,
            collector_version=current.snapshot.collector_version,
            as_of=as_of,
            niche_key=None,
            before=observed_at,
        )
        scope = "global"
        stored_author_id = None
        stored_bucket = None
        stored_niche = _GLOBAL_BASELINE_NICHE
        observations = await _observation_map(
            session, [item.post.id for item in selected], as_of=as_of
        )
    engagements = [
        item.snapshot.engagement for item in selected if item.snapshot.engagement is not None
    ]
    if engagements:
        center = median(engagements)
        mad = median_absolute_deviation(engagements, center=center)
    else:
        center = Decimal("0")
        mad = Decimal("0")
    velocities = [
        interval.velocity
        for item in selected
        for interval in [_interval_for(item, observations)]
        if interval.velocity is not None
    ]
    median_velocity = median(velocities) if velocities else Decimal("0")
    identity = (
        f"baseline:{stored_author_id or '-'}:{stored_niche}:"
        f"{current.snapshot.target_age_seconds}:{stored_bucket}:{as_of.isoformat()}"
    )
    await session.execute(select(func.pg_advisory_xact_lock(func.hashtextextended(identity, 0))))
    existing = await session.scalar(
        select(AuthorBaseline).where(
            AuthorBaseline.author_id.is_(None)
            if stored_author_id is None
            else AuthorBaseline.author_id == stored_author_id,
            AuthorBaseline.niche_key == stored_niche,
            AuthorBaseline.target_age_seconds == current.snapshot.target_age_seconds,
            AuthorBaseline.follower_bucket.is_(None)
            if stored_bucket is None
            else AuthorBaseline.follower_bucket == stored_bucket,
            AuthorBaseline.as_of == as_of,
        )
    )
    if existing is None:
        existing = AuthorBaseline(
            id=uuid4(),
            author_id=stored_author_id,
            niche_key=stored_niche,
            target_age_seconds=current.snapshot.target_age_seconds,
            follower_bucket=stored_bucket,
            sample_count=len(selected),
            median_engagement=center,
            median_velocity=median_velocity,
            engagement_mad=mad,
            as_of=as_of,
        )
        session.add(existing)
        await session.flush()
    return BaselineSelection(
        scope=scope,
        sample_count=existing.sample_count,
        median_engagement=existing.median_engagement,
        median_velocity=existing.median_velocity,
        engagement_mad=existing.engagement_mad,
        follower_bucket=existing.follower_bucket,
        baseline_id=existing.id,
    )


async def _select_baseline(
    session: AsyncSession,
    *,
    current: _CohortRecord,
    context: _PostContext,
    as_of: datetime,
) -> BaselineSelection:
    bucket = logarithmic_follower_bucket(current.followers)
    stored = await _stored_baseline(
        session,
        author_id=current.post.author_id,
        niche_key=context.niche_key,
        target_age_seconds=current.snapshot.target_age_seconds,
        follower_bucket=bucket,
        as_of=as_of,
    )
    if stored is not None:
        return stored
    return await _build_baseline(session, current=current, context=context, as_of=as_of)


def _cohort_author_lifts(records: Sequence[_CohortRecord]) -> list[Decimal]:
    if not records:
        return []
    all_values = [
        item.snapshot.engagement for item in records if item.snapshot.engagement is not None
    ]
    if not all_values:
        return []
    niche_median = median(all_values)
    author_values: dict[str, list[Decimal]] = defaultdict(list)
    bucket_values: dict[int, list[Decimal]] = defaultdict(list)
    for record in records:
        if record.snapshot.engagement is None:
            continue
        author_values[record.post.author_id].append(record.snapshot.engagement)
        bucket = logarithmic_follower_bucket(record.followers)
        if bucket is not None:
            bucket_values[bucket].append(record.snapshot.engagement)
    lifts: list[Decimal] = []
    for record in records:
        engagement = record.snapshot.engagement
        if engagement is None:
            continue
        history = author_values[record.post.author_id]
        bucket = logarithmic_follower_bucket(record.followers)
        if len(history) >= _MIN_AUTHOR_HISTORY:
            baseline = median(history)
        elif bucket is not None and bucket_values[bucket]:
            baseline = median(bucket_values[bucket])
        else:
            baseline = niche_median
        lifts.append(
            calculate_author_lift(
                engagement,
                median_engagement=baseline,
                engagement_mad=Decimal("0"),
            ).ratio
        )
    return lifts


async def _topic_context_values(
    session: AsyncSession,
    *,
    context: _PostContext,
    as_of: datetime,
    topic_snapshot_id: UUID | None,
) -> tuple[TopicSnapshot | None, list[TopicSnapshot]]:
    current: TopicSnapshot | None = None
    if topic_snapshot_id is not None:
        current = await session.get(TopicSnapshot, topic_snapshot_id)
        if current is None:
            raise ValueError(f"unknown topic snapshot: {topic_snapshot_id}")
        if context.cluster is not None and current.cluster_id != context.cluster.id:
            raise ValueError("topic snapshot does not belong to the post cluster")
    elif context.cluster is not None:
        current = await session.scalar(
            select(TopicSnapshot)
            .where(
                TopicSnapshot.cluster_id == context.cluster.id,
                TopicSnapshot.observed_at <= as_of,
            )
            .order_by(TopicSnapshot.observed_at.desc(), TopicSnapshot.id.desc())
            .limit(1)
        )
    if current is None:
        return None, []
    cohort = list(
        (
            await session.scalars(
                select(TopicSnapshot)
                .join(TopicCluster, TopicCluster.id == TopicSnapshot.cluster_id)
                .where(
                    TopicCluster.niche_key == context.niche_key,
                    TopicSnapshot.window_minutes == current.window_minutes,
                    TopicSnapshot.observed_at <= as_of,
                )
                .order_by(TopicSnapshot.observed_at.desc(), TopicSnapshot.id.desc())
                .limit(_MAX_COHORT_ROWS)
            )
        ).all()
    )
    return current, cohort


def _percentile(
    value: Decimal | None,
    population: Sequence[Decimal],
    *,
    minimum_cohort_size: int,
) -> Percentile | None:
    if value is None:
        return None
    return percentile_with_shrinkage(
        value,
        population,
        minimum_cohort_size=minimum_cohort_size,
    )


def _confidence(
    percentiles: Mapping[str, Percentile | None], weights: Mapping[str, Decimal]
) -> Decimal:
    available = {
        name: item for name, item in percentiles.items() if item is not None and name in weights
    }
    weight_sum = sum((weights[name] for name in available), Decimal("0"))
    if weight_sum == Decimal("0"):
        return Decimal("0").quantize(CONFIDENCE_QUANTUM)
    value = (
        sum((weights[name] * item.confidence for name, item in available.items()), Decimal("0"))
        / weight_sum
    )
    return value.quantize(CONFIDENCE_QUANTUM, rounding=ROUND_HALF_UP)


async def _spam_facts(
    session: AsyncSession,
    *,
    current: _CohortRecord,
    context: _PostContext,
    interval: Any,
    topic_snapshot: TopicSnapshot | None,
    as_of: datetime,
) -> dict[str, Decimal | bool]:
    duplicate_count = 1
    if context.cluster is not None:
        duplicate_count = int(
            await session.scalar(
                select(func.count())
                .select_from(Post)
                .join(TopicMember, TopicMember.post_id == Post.id)
                .where(
                    TopicMember.cluster_id == context.cluster.id,
                    Post.normalized_text_hash == current.post.normalized_text_hash,
                    Post.posted_at <= as_of,
                )
            )
            or 0
        )
    concentration = (
        topic_snapshot.largest_author_concentration if topic_snapshot is not None else Decimal("0")
    )
    new_author_ratio = (
        topic_snapshot.new_author_ratio if topic_snapshot is not None else Decimal("0")
    )
    unique_authors = topic_snapshot.unique_authors if topic_snapshot is not None else 0
    repost_only = False
    if topic_snapshot is not None:
        features = topic_snapshot.features if isinstance(topic_snapshot.features, dict) else {}
        repost_only = bool(features.get("repost_only_burst")) or (
            topic_snapshot.original_posts == 0
            and topic_snapshot.reposts == topic_snapshot.post_count
        )
    return {
        "duplicate_wording": duplicate_count >= 3,
        "one_author_concentration": concentration >= Decimal("0.60"),
        "repost_only_activity": repost_only,
        "coordinated_new_accounts": (unique_authors >= 4 and new_author_ratio >= Decimal("0.50")),
        "niche_mismatch": (
            context.cluster is not None
            and context.rule_niche_key != "global"
            and context.cluster.niche_key != context.rule_niche_key
        ),
        "declining_velocity": interval.acceleration is not None
        and interval.acceleration < Decimal("0"),
    }


def _penalty_json(penalties: Sequence[AppliedPenalty]) -> list[dict[str, Any]]:
    return [
        {"name": item.name, "amount": item.amount, "source_value": item.source_value}
        for item in penalties
    ]


async def _persist_post_score(
    session: AsyncSession,
    *,
    current: _CohortRecord,
    cluster_id: UUID | None,
    as_of: datetime,
    score: Decimal,
    components: dict[str, Any],
    cohort_size: int,
    confidence: Decimal,
    explanation: str,
    input_hash: str,
) -> tuple[PostScore, bool]:
    proposed_id = uuid4()
    score_id = await session.scalar(
        pg_insert(PostScore)
        .values(
            id=proposed_id,
            post_id=current.post.id,
            cluster_id=cluster_id,
            snapshot_id=current.snapshot.id,
            score_version=POST_SCORE_VERSION,
            as_of=as_of,
            score=score,
            components=json_safe(components),
            cohort_size=cohort_size,
            confidence=confidence,
            explanation=explanation,
            input_hash=input_hash,
        )
        .on_conflict_do_nothing()
        .returning(PostScore.id)
    )
    created = score_id is not None
    if score_id is None:
        existing = await session.scalar(
            select(PostScore).where(
                PostScore.post_id == current.post.id,
                PostScore.snapshot_id == current.snapshot.id,
                PostScore.score_version == POST_SCORE_VERSION,
            )
        )
        if existing is None:
            existing = await session.scalar(
                select(PostScore).where(PostScore.input_hash == input_hash)
            )
        if existing is None:
            raise RuntimeError("post score conflict without an existing row")
        if existing.input_hash != input_hash:
            raise RuntimeError("immutable post score replay mismatch")
        return existing, False
    stored = await session.get(PostScore, score_id)
    if stored is None:
        raise RuntimeError("new post score was not readable")
    return stored, created


async def _cooldown_active(
    session: AsyncSession,
    *,
    kind: str,
    entity_type: str,
    entity_id: str,
    as_of: datetime,
) -> bool:
    alert_id = await session.scalar(
        select(Alert.id)
        .where(
            Alert.kind == kind,
            Alert.entity_type == entity_type,
            Alert.entity_id == entity_id,
            Alert.state != "suppressed",
            Alert.cooldown_until.is_not(None),
            Alert.cooldown_until > as_of,
        )
        .order_by(Alert.created_at.desc(), Alert.id.desc())
        .limit(1)
    )
    return alert_id is not None


async def score_post_snapshot(
    session: AsyncSession,
    snapshot_id: UUID,
    rules: RulesConfig,
    *,
    as_of: datetime | None = None,
    topic_snapshot_id: UUID | None = None,
) -> PostScoringResult:
    """Construct rules-v1 features and immutably persist one post score."""

    snapshot = await session.get(PostMetricSnapshot, snapshot_id)
    if snapshot is None:
        raise ValueError(f"unknown post metric snapshot: {snapshot_id}")
    if snapshot.quality not in _OBSERVED_QUALITIES or snapshot.observed_at is None:
        raise ValueError("only real observed/late snapshots can be scored")
    selected_as_of = as_of or snapshot.observed_at
    if selected_as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if selected_as_of < snapshot.observed_at:
        raise ValueError("as_of cannot precede the source observation")
    if snapshot.engagement is None:
        raise ValueError("source snapshot has no engagement")
    await ensure_score_version(
        session,
        version=POST_SCORE_VERSION,
        rules=rules,
        activated_at=selected_as_of,
    )
    post = await session.get(Post, snapshot.post_id)
    if post is None:
        raise RuntimeError("snapshot references a missing post")
    author = await session.get(Author, post.author_id)
    current = _CohortRecord(
        snapshot=snapshot,
        post=post,
        author=author,
        followers=(
            snapshot.author_followers
            if snapshot.author_followers is not None
            else (author.followers_count if author is not None else None)
        ),
    )
    context = await _post_context(session, post.id)
    records = await _target_records(
        session,
        target_age_seconds=snapshot.target_age_seconds,
        collector_version=snapshot.collector_version,
        as_of=selected_as_of,
        niche_key=context.niche_key if context.niche_key != "global" else None,
    )
    if not any(item.snapshot.id == snapshot.id for item in records):
        records.append(current)
        records.sort(
            key=lambda item: (item.snapshot.observed_at or selected_as_of, str(item.snapshot.id))
        )
    observations = await _observation_map(
        session, [item.post.id for item in records], as_of=selected_as_of
    )
    intervals = {item.snapshot.id: _interval_for(item, observations) for item in records}
    current_interval = intervals[snapshot.id]
    baseline = await _select_baseline(
        session, current=current, context=context, as_of=selected_as_of
    )
    author_lift = calculate_author_lift(
        snapshot.engagement,
        median_engagement=baseline.median_engagement,
        engagement_mad=baseline.engagement_mad,
    )
    topic_snapshot, topic_cohort = await _topic_context_values(
        session,
        context=context,
        as_of=selected_as_of,
        topic_snapshot_id=topic_snapshot_id,
    )
    minimum = rules.alert_policy.min_cohort_size
    velocity_population = [
        item.velocity for item in intervals.values() if item.velocity is not None
    ]
    acceleration_population = [
        item.acceleration for item in intervals.values() if item.acceleration is not None
    ]
    impression_population = [
        item.impression_velocity
        for item in intervals.values()
        if item.impression_velocity is not None
    ]
    author_lift_population = _cohort_author_lifts(records)
    topic_spike_population = [
        item.topic_spike for item in topic_cohort if item.topic_spike is not None
    ]
    spread_population = [Decimal(item.unique_authors) for item in topic_cohort]
    percentiles: dict[str, Percentile | None] = {
        "velocity_percentile": _percentile(
            current_interval.velocity, velocity_population, minimum_cohort_size=minimum
        ),
        "acceleration_percentile": _percentile(
            current_interval.acceleration,
            acceleration_population,
            minimum_cohort_size=minimum,
        ),
        "author_lift_percentile": _percentile(
            author_lift.ratio,
            author_lift_population,
            minimum_cohort_size=minimum,
        ),
        "topic_spike_percentile": _percentile(
            topic_snapshot.topic_spike if topic_snapshot is not None else None,
            topic_spike_population,
            minimum_cohort_size=minimum,
        ),
        "conversation_spread_percentile": _percentile(
            Decimal(topic_snapshot.unique_authors) if topic_snapshot is not None else None,
            spread_population,
            minimum_cohort_size=minimum,
        ),
        "impression_velocity_percentile": _percentile(
            current_interval.impression_velocity,
            impression_population,
            minimum_cohort_size=minimum,
        ),
    }
    component_values = {
        name: item.shrunk if item is not None else None for name, item in percentiles.items()
    }
    facts = await _spam_facts(
        session,
        current=current,
        context=context,
        interval=current_interval,
        topic_snapshot=topic_snapshot,
        as_of=selected_as_of,
    )
    penalties = applied_penalties(facts)
    penalty_total = sum((item.amount for item in penalties), Decimal("0"))
    weights = _post_weights(rules)
    calculation = calculate_post_score(
        component_values, spam_penalty=penalty_total, weights=weights
    )
    score_confidence = _confidence(percentiles, weights)
    explanation = (
        f"{POST_SCORE_VERSION}: actual-interval features; midrank {context.niche_key}/"
        f"{snapshot.target_age_seconds}s cohort n={len(records)}; "
        f"baseline={baseline.scope} n={baseline.sample_count}; "
        f"missing positive weights renormalized; spam penalty={penalty_total}."
    )
    own_observations = observations.get(post.id, [])
    cohort_inputs = [
        {
            "snapshot_id": item.snapshot.id,
            "post_id": item.post.id,
            "author_id": item.post.author_id,
            "observed_at": item.snapshot.observed_at,
            "engagement": item.snapshot.engagement,
            "followers": item.followers,
            "velocity": intervals[item.snapshot.id].velocity,
            "acceleration": intervals[item.snapshot.id].acceleration,
            "impression_velocity": intervals[item.snapshot.id].impression_velocity,
        }
        for item in records
    ]
    topic_cohort_inputs = [
        {
            "snapshot_id": item.id,
            "cluster_id": item.cluster_id,
            "observed_at": item.observed_at,
            "topic_spike": item.topic_spike,
            "unique_authors": item.unique_authors,
        }
        for item in sorted(topic_cohort, key=lambda row: (row.observed_at, str(row.id)))
    ]
    observation_inputs = [
        {
            "snapshot_id": item.id,
            "observed_at": item.observed_at,
            "engagement": item.engagement,
            "impressions": item.impressions,
        }
        for item in own_observations
    ]
    component_evidence: dict[str, Any] = {
        "score_version": POST_SCORE_VERSION,
        "feature_version": POST_FEATURE_VERSION,
        "implementation_hash": implementation_hash(score_version=POST_SCORE_VERSION),
        "values": component_values,
        "raw_percentiles": {
            name: item.raw if item is not None else None for name, item in percentiles.items()
        },
        "component_cohort_sizes": {
            name: item.cohort_size if item is not None else 0 for name, item in percentiles.items()
        },
        "component_confidence": {
            name: item.confidence if item is not None else Decimal("0")
            for name, item in percentiles.items()
        },
        "raw_features": {
            "velocity": current_interval.velocity,
            "acceleration": current_interval.acceleration,
            "impression_velocity": current_interval.impression_velocity,
            "actual_interval_seconds": current_interval.interval_seconds,
            "trailing_interval_seconds": current_interval.trailing_interval_seconds,
            "author_lift": author_lift.ratio,
            "author_mad_deviation": author_lift.mad_deviation,
            "topic_spike": topic_snapshot.topic_spike if topic_snapshot is not None else None,
            "conversation_spread": (
                topic_snapshot.unique_authors if topic_snapshot is not None else None
            ),
        },
        "cohort": {
            "target_age_seconds": snapshot.target_age_seconds,
            "collector_version": snapshot.collector_version,
            "niche_key": context.niche_key,
            "size": len(records),
            "minimum_size": minimum,
            "snapshot_ids": [item.snapshot.id for item in records],
            "input_hash": canonical_input_hash(cohort_inputs),
            "topic_input_hash": canonical_input_hash(topic_cohort_inputs),
        },
        "author_baseline": {
            "id": baseline.baseline_id,
            "scope": baseline.scope,
            "sample_count": baseline.sample_count,
            "follower_bucket": baseline.follower_bucket,
            "median_engagement": baseline.median_engagement,
            "median_velocity": baseline.median_velocity,
            "engagement_mad": baseline.engagement_mad,
        },
        "spam": {
            "facts": facts,
            "applied": _penalty_json(penalties),
            "total": penalty_total,
            "maximum": MAX_SPAM_PENALTY,
        },
        "weights": weights,
        "available_weight": calculation.available_weight,
        "contributions": calculation.contributions,
        "positive_score": calculation.positive_score,
        "source_snapshot": {
            "id": snapshot.id,
            "observed_at": snapshot.observed_at,
            "actual_age_seconds": snapshot.actual_age_seconds,
            "target_age_seconds": snapshot.target_age_seconds,
            "quality": snapshot.quality,
            "metrics": {
                "likes": snapshot.likes,
                "reposts": snapshot.reposts,
                "quotes": snapshot.quotes,
                "replies": snapshot.replies,
                "bookmarks": snapshot.bookmarks,
                "impressions": snapshot.impressions,
                "engagement": snapshot.engagement,
            },
        },
        "observation_ids": [item.id for item in own_observations],
        "observation_input_hash": canonical_input_hash(observation_inputs),
    }
    input_payload = {
        "score_version": POST_SCORE_VERSION,
        "implementation_hash": implementation_hash(score_version=POST_SCORE_VERSION),
        "rules_hash": rules.canonical_hash(),
        "post_id": post.id,
        "snapshot_id": snapshot.id,
        "as_of": selected_as_of,
        "components": component_evidence,
        "cohort_inputs": cohort_inputs,
        "topic_cohort_inputs": topic_cohort_inputs,
        "observation_inputs": observation_inputs,
    }
    input_hash = canonical_input_hash(input_payload)
    stored, created = await _persist_post_score(
        session,
        current=current,
        cluster_id=context.cluster.id if context.cluster is not None else None,
        as_of=selected_as_of,
        score=calculation.score,
        components=component_evidence,
        cohort_size=len(records),
        confidence=score_confidence,
        explanation=explanation,
        input_hash=input_hash,
    )
    gate = breakout_gate(
        score=stored.score,
        velocity_percentile=(
            percentiles["velocity_percentile"].shrunk
            if percentiles["velocity_percentile"] is not None
            else None
        ),
        author_lift=author_lift.ratio,
        score_min=rules.alert_policy.breakout.score_min,
        velocity_percentile_min=rules.alert_policy.breakout.velocity_percentile_min,
        author_lift_min=rules.alert_policy.breakout.author_lift_min,
    )
    candidate: AlertCandidate | None = None
    policy_hash = rules.canonical_hash()
    if gate.passed and not await _cooldown_active(
        session,
        kind="breakout_post",
        entity_type="post",
        entity_id=post.id,
        as_of=selected_as_of,
    ):
        snapshots_evidence = [
            {
                "snapshot_id": item.id,
                "observed_at": item.observed_at,
                "target_age_seconds": item.target_age_seconds,
                "actual_age_seconds": item.actual_age_seconds,
                "engagement": item.engagement,
                "impressions": item.impressions,
            }
            for item in own_observations
            if item.observed_at is not None and item.observed_at <= selected_as_of
        ]
        alert_evidence = json_safe(
            {
                "post_id": post.id,
                "post_text": post.text,
                "username": author.username if author is not None else None,
                "posted_at": post.posted_at,
                "as_of": selected_as_of,
                "score_version": POST_SCORE_VERSION,
                "input_hash": input_hash,
                "policy_hash": policy_hash,
                "components": component_values,
                "velocity_percentile": (
                    percentiles["velocity_percentile"].shrunk
                    if percentiles["velocity_percentile"] is not None
                    else None
                ),
                "author_lift": author_lift.ratio,
                "cohort_size": len(records),
                "confidence": score_confidence,
                "explanation": explanation,
                "spam_penalties": _penalty_json(penalties),
                "gate_checks": gate.checks,
                "snapshots": snapshots_evidence,
            }
        )
        candidate = AlertCandidate(
            kind="breakout_post",
            entity_id=post.id,
            score_id=str(stored.id),
            score=str(stored.score),
            evidence=alert_evidence,
            policy_hash=policy_hash,
            dedupe_key=(f"breakout_post:{post.id}:{stored.id}:{policy_hash}")[:255],
        )
    return PostScoringResult(
        score_id=stored.id,
        post_id=post.id,
        snapshot_id=snapshot.id,
        score=stored.score,
        input_hash=stored.input_hash,
        created=created,
        gate=gate,
        candidate=candidate,
    )


async def _persist_topic_score(
    session: AsyncSession,
    *,
    snapshot: TopicSnapshot,
    as_of: datetime,
    score: Decimal,
    components: dict[str, Any],
    cohort_size: int,
    confidence: Decimal,
    explanation: str,
    input_hash: str,
) -> tuple[TopicScore, bool]:
    score_id = await session.scalar(
        pg_insert(TopicScore)
        .values(
            id=uuid4(),
            cluster_id=snapshot.cluster_id,
            topic_snapshot_id=snapshot.id,
            score_version=TOPIC_SCORE_VERSION,
            as_of=as_of,
            score=score,
            components=json_safe(components),
            cohort_size=cohort_size,
            confidence=confidence,
            explanation=explanation,
            input_hash=input_hash,
        )
        .on_conflict_do_nothing()
        .returning(TopicScore.id)
    )
    created = score_id is not None
    if score_id is None:
        existing = await session.scalar(
            select(TopicScore).where(
                TopicScore.cluster_id == snapshot.cluster_id,
                TopicScore.topic_snapshot_id == snapshot.id,
                TopicScore.score_version == TOPIC_SCORE_VERSION,
            )
        )
        if existing is None:
            existing = await session.scalar(
                select(TopicScore).where(TopicScore.input_hash == input_hash)
            )
        if existing is None:
            raise RuntimeError("topic score conflict without an existing row")
        if existing.input_hash != input_hash:
            raise RuntimeError("immutable topic score replay mismatch")
        return existing, False
    stored = await session.get(TopicScore, score_id)
    if stored is None:
        raise RuntimeError("new topic score was not readable")
    return stored, created


async def score_topic_snapshot(
    session: AsyncSession,
    topic_snapshot_id: UUID,
    rules: RulesConfig,
    *,
    as_of: datetime | None = None,
) -> TopicScoringResult:
    """Calculate and immutably persist the separately versioned topic score."""

    snapshot = await session.get(TopicSnapshot, topic_snapshot_id)
    if snapshot is None:
        raise ValueError(f"unknown topic snapshot: {topic_snapshot_id}")
    selected_as_of = as_of or snapshot.observed_at
    if selected_as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if selected_as_of < snapshot.observed_at:
        raise ValueError("as_of cannot precede the source topic observation")
    cluster = await session.get(TopicCluster, snapshot.cluster_id)
    if cluster is None:
        raise RuntimeError("topic snapshot references a missing cluster")
    await ensure_score_version(
        session,
        version=TOPIC_SCORE_VERSION,
        rules=rules,
        activated_at=selected_as_of,
    )
    cohort = list(
        (
            await session.scalars(
                select(TopicSnapshot)
                .join(TopicCluster, TopicCluster.id == TopicSnapshot.cluster_id)
                .where(
                    TopicCluster.niche_key == cluster.niche_key,
                    TopicSnapshot.window_minutes == snapshot.window_minutes,
                    TopicSnapshot.observed_at <= selected_as_of,
                )
                .order_by(TopicSnapshot.observed_at.desc(), TopicSnapshot.id.desc())
                .limit(_MAX_COHORT_ROWS)
            )
        ).all()
    )
    if not any(item.id == snapshot.id for item in cohort):
        cohort.append(snapshot)
    features = snapshot.features if isinstance(snapshot.features, dict) else {}
    acceleration = _decimal_optional(features.get("unique_author_acceleration"))
    growth = _decimal_optional(features.get("unique_author_growth"))
    independent_ratio = _decimal_optional(features.get("independent_original_ratio"))
    if independent_ratio is None:
        independent_ratio = (
            Decimal(snapshot.original_posts) / Decimal(snapshot.post_count)
            if snapshot.post_count
            else Decimal("0")
        )
    acceleration_population = [
        parsed
        for item in cohort
        for parsed in [
            _decimal_optional(
                item.features.get("unique_author_acceleration")
                if isinstance(item.features, dict)
                else None
            )
        ]
        if parsed is not None
    ]
    growth_population = [
        parsed
        for item in cohort
        for parsed in [
            _decimal_optional(
                item.features.get("unique_author_growth")
                if isinstance(item.features, dict)
                else None
            )
        ]
        if parsed is not None
    ]
    spike_population = [item.topic_spike for item in cohort if item.topic_spike is not None]
    minimum = rules.alert_policy.min_cohort_size
    percentiles: dict[str, Percentile | None] = {
        "topic_spike": _percentile(
            snapshot.topic_spike, spike_population, minimum_cohort_size=minimum
        ),
        "acceleration": _percentile(
            acceleration, acceleration_population, minimum_cohort_size=minimum
        ),
        "unique_author_growth": _percentile(growth, growth_population, minimum_cohort_size=minimum),
        "independent_original_ratio": None,
        "text_diversity": None,
    }
    normalized_components: dict[str, Decimal | None] = {
        "topic_spike": (
            percentiles["topic_spike"].shrunk if percentiles["topic_spike"] is not None else None
        ),
        "acceleration": (
            percentiles["acceleration"].shrunk if percentiles["acceleration"] is not None else None
        ),
        "unique_author_growth": (
            percentiles["unique_author_growth"].shrunk
            if percentiles["unique_author_growth"] is not None
            else None
        ),
        "independent_original_ratio": clamp(independent_ratio),
        "text_diversity": clamp(snapshot.text_diversity),
    }
    concentration_excess = clamp(
        (snapshot.largest_author_concentration - Decimal("0.40")) / Decimal("0.60")
    )
    burst_decay = clamp(_decimal_optional(features.get("burst_decay")) or Decimal("0"))
    repost_only = bool(features.get("repost_only_burst"))
    topic_penalties = {
        "author_concentration": TOPIC_PENALTY_WEIGHTS["author_concentration"]
        * concentration_excess,
        "burst_decay": TOPIC_PENALTY_WEIGHTS["burst_decay"] * burst_decay,
        "repost_only_burst": (
            TOPIC_PENALTY_WEIGHTS["repost_only_burst"] if repost_only else Decimal("0")
        ),
    }
    topic_penalty = min(MAX_TOPIC_PENALTY, sum(topic_penalties.values(), Decimal("0")))
    calculation = calculate_topic_score(normalized_components, topic_penalty=topic_penalty)
    overall_cold_confidence = min(Decimal("1"), Decimal(len(cohort)) / Decimal(minimum))
    confidence = overall_cold_confidence.quantize(CONFIDENCE_QUANTUM, rounding=ROUND_HALF_UP)
    explanation = (
        f"{TOPIC_SCORE_VERSION}: topic spike/acceleration/growth plus independent originals "
        f"and diversity; {cluster.niche_key}/{snapshot.window_minutes}m cohort "
        f"n={len(cohort)}; concentration/decay/repost penalties={topic_penalty}."
    )
    cohort_inputs = [
        {
            "snapshot_id": item.id,
            "cluster_id": item.cluster_id,
            "observed_at": item.observed_at,
            "topic_spike": item.topic_spike,
            "unique_authors": item.unique_authors,
            "acceleration": _decimal_optional(
                item.features.get("unique_author_acceleration")
                if isinstance(item.features, dict)
                else None
            ),
            "growth": _decimal_optional(
                item.features.get("unique_author_growth")
                if isinstance(item.features, dict)
                else None
            ),
            "features_hash": canonical_input_hash(item.features),
        }
        for item in sorted(cohort, key=lambda row: (row.observed_at, str(row.id)))
    ]
    component_evidence: dict[str, Any] = {
        "score_version": TOPIC_SCORE_VERSION,
        "feature_version": TOPIC_FEATURE_VERSION,
        "implementation_hash": implementation_hash(score_version=TOPIC_SCORE_VERSION),
        "values": normalized_components,
        "raw_percentiles": {
            name: item.raw if item is not None else None for name, item in percentiles.items()
        },
        "raw_features": {
            "topic_spike": snapshot.topic_spike,
            "expected_unique_authors": snapshot.expected_unique_authors,
            "unique_authors": snapshot.unique_authors,
            "unique_author_acceleration": acceleration,
            "unique_author_growth": growth,
            "independent_original_ratio": independent_ratio,
            "text_diversity": snapshot.text_diversity,
            "largest_author_concentration": snapshot.largest_author_concentration,
            "new_author_ratio": snapshot.new_author_ratio,
            "burst_decay": burst_decay,
            "repost_only_burst": repost_only,
        },
        "cohort": {
            "niche_key": cluster.niche_key,
            "window_minutes": snapshot.window_minutes,
            "size": len(cohort),
            "minimum_size": minimum,
            "snapshot_ids": [item.id for item in cohort],
            "input_hash": canonical_input_hash(cohort_inputs),
        },
        "weights": TOPIC_POSITIVE_WEIGHTS,
        "penalties": topic_penalties,
        "penalty_total": topic_penalty,
        "available_weight": calculation.available_weight,
        "contributions": calculation.contributions,
        "positive_score": calculation.positive_score,
        "source_snapshot": {
            "id": snapshot.id,
            "observed_at": snapshot.observed_at,
            "post_count": snapshot.post_count,
            "unique_authors": snapshot.unique_authors,
            "original_posts": snapshot.original_posts,
            "reposts": snapshot.reposts,
            "quotes": snapshot.quotes,
            "replies": snapshot.replies,
            "features_hash": canonical_input_hash(snapshot.features),
        },
    }
    policy_hash = rules.canonical_hash()
    input_hash = canonical_input_hash(
        {
            "score_version": TOPIC_SCORE_VERSION,
            "implementation_hash": implementation_hash(score_version=TOPIC_SCORE_VERSION),
            "rules_hash": policy_hash,
            "cluster_id": cluster.id,
            "topic_snapshot_id": snapshot.id,
            "as_of": selected_as_of,
            "components": component_evidence,
            "cohort_inputs": cohort_inputs,
        }
    )
    stored, created = await _persist_topic_score(
        session,
        snapshot=snapshot,
        as_of=selected_as_of,
        score=calculation.score,
        components=component_evidence,
        cohort_size=len(cohort),
        confidence=confidence,
        explanation=explanation,
        input_hash=input_hash,
    )
    gate = emerging_topic_gate(
        score=stored.score,
        unique_authors=snapshot.unique_authors,
        topic_spike=snapshot.topic_spike,
        score_min=rules.alert_policy.emerging_conversation.score_min,
        unique_authors_min=rules.alert_policy.emerging_conversation.unique_authors_min,
        topic_spike_min=rules.alert_policy.emerging_conversation.topic_spike_min,
    )
    candidate: AlertCandidate | None = None
    if gate.passed and not await _cooldown_active(
        session,
        kind="emerging_conversation",
        entity_type="topic",
        entity_id=str(cluster.id),
        as_of=selected_as_of,
    ):
        alert_evidence = json_safe(
            {
                "topic_label": cluster.label or cluster.topic_key,
                "as_of": selected_as_of,
                "score_version": TOPIC_SCORE_VERSION,
                "input_hash": input_hash,
                "policy_hash": policy_hash,
                "components": normalized_components,
                "unique_authors": snapshot.unique_authors,
                "topic_spike": snapshot.topic_spike,
                "original_content_ratio": independent_ratio,
                "cohort_size": len(cohort),
                "confidence": confidence,
                "explanation": explanation,
                "gate_checks": gate.checks,
                "topic_snapshots": [
                    {
                        "snapshot_id": item.id,
                        "observed_at": item.observed_at,
                        "window_minutes": item.window_minutes,
                        "post_count": item.post_count,
                        "unique_authors": item.unique_authors,
                        "topic_spike": item.topic_spike,
                    }
                    for item in sorted(
                        (row for row in cohort if row.cluster_id == cluster.id),
                        key=lambda row: (row.observed_at, str(row.id)),
                    )
                ],
                "top_driving_posts": features.get("top_driving_posts", []),
            }
        )
        candidate = AlertCandidate(
            kind="emerging_conversation",
            entity_id=str(cluster.id),
            score_id=str(stored.id),
            score=str(stored.score),
            evidence=alert_evidence,
            policy_hash=policy_hash,
            dedupe_key=(f"emerging_conversation:{cluster.id}:{stored.id}:{policy_hash}")[:255],
        )
    return TopicScoringResult(
        score_id=stored.id,
        cluster_id=cluster.id,
        topic_snapshot_id=snapshot.id,
        score=stored.score,
        input_hash=stored.input_hash,
        created=created,
        gate=gate,
        candidate=candidate,
    )


async def process_snapshot_scores(
    session: AsyncSession,
    snapshot_id: UUID,
    rules: RulesConfig,
    *,
    topic_snapshot_id: UUID | None = None,
    as_of: datetime | None = None,
) -> ScoringWorkflowResult:
    post_result = await score_post_snapshot(
        session,
        snapshot_id,
        rules,
        as_of=as_of,
        topic_snapshot_id=topic_snapshot_id,
    )
    topic_result = (
        await score_topic_snapshot(session, topic_snapshot_id, rules, as_of=as_of)
        if topic_snapshot_id is not None
        else None
    )
    candidates = tuple(
        candidate
        for candidate in (
            post_result.candidate,
            topic_result.candidate if topic_result is not None else None,
        )
        if candidate is not None
    )
    return ScoringWorkflowResult(
        post=post_result,
        topic=topic_result,
        candidates=candidates,
    )


score_post = score_post_snapshot
score_topic = score_topic_snapshot
process_scores = process_snapshot_scores
