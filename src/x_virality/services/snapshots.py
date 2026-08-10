from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import RulesConfig
from x_virality.db.models import Post, PostMetricSnapshot
from x_virality.db.repositories import upsert_rate_limit
from x_virality.db.session import utcnow
from x_virality.domain.models import SNAPSHOT_HORIZONS_SECONDS, AlertCandidate, PostData
from x_virality.providers.x_api import XLookup
from x_virality.scoring.core import floor_seconds, json_safe, weighted_engagement
from x_virality.scoring.service import (
    ScoringWorkflowResult,
    score_post_snapshot,
    score_topic_snapshot,
)
from x_virality.services.topics import (
    DEFAULT_TOPIC_WINDOW_MINUTES,
    TopicAssignment,
    TopicProcessingResult,
    TopicSignalProvider,
    TopicSnapshotResult,
    assign_post_to_topic,
    create_topic_snapshot,
    resolve_topic_cluster,
)

DEFAULT_COLLECTOR_VERSION = "x-api-v1"
MAX_LOOKUP_POSTS = 100


class SnapshotProvider(Protocol):
    async def lookup_posts(self, ids: Iterable[str]) -> XLookup: ...


@dataclass(frozen=True, slots=True)
class SnapshotScheduleResult:
    post_id: str
    target_snapshot_ids: tuple[UUID, ...]
    discovery_snapshot_id: UUID | None
    skipped_late: int


@dataclass(frozen=True, slots=True)
class SnapshotOutcome:
    snapshot_id: UUID
    post_id: str
    quality: str
    observed_at: datetime | None
    actual_age_seconds: int | None
    topic: TopicProcessingResult | None
    scoring: ScoringWorkflowResult | None


@dataclass(frozen=True, slots=True)
class SnapshotBatchResult:
    outcomes: tuple[SnapshotOutcome, ...]
    candidates: tuple[AlertCandidate, ...]
    requested_posts: int
    returned_posts: int

    @property
    def observed(self) -> int:
        return sum(item.quality in {"observed", "late"} for item in self.outcomes)


@dataclass(frozen=True, slots=True)
class _ScheduleSpec:
    target_age_seconds: int
    scheduled_for: datetime
    quality: str
    observed_at: datetime | None
    actual_age_seconds: int | None
    late_seconds: int | None
    post_data: PostData | None
    discovery: bool


def _engagement_weights(rules: RulesConfig | None) -> dict[str, Decimal] | None:
    if rules is None:
        return None
    return {
        key: Decimal(str(value)) for key, value in rules.scoring.engagement.model_dump().items()
    }


def _metric_values(post: PostData, rules: RulesConfig | None) -> dict[str, Any]:
    metrics = post.metrics
    return {
        "likes": metrics.likes,
        "reposts": metrics.reposts,
        "replies": metrics.replies,
        "quotes": metrics.quotes,
        "bookmarks": metrics.bookmarks,
        "impressions": metrics.impressions,
        "author_followers": post.author.followers if post.author is not None else None,
        "engagement": weighted_engagement(
            likes=metrics.likes,
            reposts=metrics.reposts,
            quotes=metrics.quotes,
            replies=metrics.replies,
            bookmarks=metrics.bookmarks,
            weights=_engagement_weights(rules),
        ),
        "raw": json_safe(post.raw),
    }


async def _insert_scheduled_snapshot(
    session: AsyncSession,
    *,
    post_id: str,
    collector_version: str,
    spec: _ScheduleSpec,
    rules: RulesConfig | None,
) -> tuple[UUID, bool]:
    values: dict[str, Any] = {
        "id": uuid4(),
        "post_id": post_id,
        "target_age_seconds": spec.target_age_seconds,
        "collector_version": collector_version,
        "scheduled_for": spec.scheduled_for,
        "observed_at": spec.observed_at,
        "actual_age_seconds": spec.actual_age_seconds,
        "late_seconds": spec.late_seconds,
        "quality": spec.quality,
        "likes": None,
        "reposts": None,
        "replies": None,
        "quotes": None,
        "bookmarks": None,
        "impressions": None,
        "author_followers": None,
        "engagement": None,
        "raw": (
            {"reason": "target expired before discovery"}
            if spec.quality == "skipped_late"
            else None
        ),
    }
    if spec.post_data is not None:
        values.update(_metric_values(spec.post_data, rules))
    snapshot_id = await session.scalar(
        pg_insert(PostMetricSnapshot)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                PostMetricSnapshot.post_id,
                PostMetricSnapshot.target_age_seconds,
                PostMetricSnapshot.collector_version,
            ]
        )
        .returning(PostMetricSnapshot.id)
    )
    created = snapshot_id is not None
    if snapshot_id is None:
        existing = await session.scalar(
            select(PostMetricSnapshot).where(
                PostMetricSnapshot.post_id == post_id,
                PostMetricSnapshot.target_age_seconds == spec.target_age_seconds,
                PostMetricSnapshot.collector_version == collector_version,
            )
        )
        if existing is None:
            raise RuntimeError("snapshot schedule conflict without an existing row")
        if existing.scheduled_for != spec.scheduled_for:
            raise RuntimeError("immutable snapshot schedule mismatch")
        snapshot_id = existing.id
    return snapshot_id, created


async def schedule_post_snapshots(
    session: AsyncSession,
    post_id: str,
    *,
    discovered_at: datetime,
    discovery_post: PostData | None = None,
    rules: RulesConfig | None = None,
    collector_version: str = DEFAULT_COLLECTOR_VERSION,
    horizons: Sequence[int] = SNAPSHOT_HORIZONS_SECONDS,
) -> SnapshotScheduleResult:
    """Precreate fixed targets and retain one actual-age discovery observation."""

    if discovered_at.tzinfo is None:
        raise ValueError("discovered_at must be timezone-aware")
    if not collector_version or len(collector_version) > 32:
        raise ValueError("collector_version must contain 1-32 characters")
    if not horizons or any(age < 1 for age in horizons) or len(set(horizons)) != len(horizons):
        raise ValueError("snapshot horizons must be unique positive seconds")
    post = await session.scalar(select(Post).where(Post.id == post_id).with_for_update())
    if post is None:
        raise ValueError(f"unknown post: {post_id}")
    if discovery_post is not None and discovery_post.id != post_id:
        raise ValueError("discovery PostData does not match post_id")

    specs: dict[int, _ScheduleSpec] = {}
    for target_age in sorted(horizons):
        scheduled_for = post.posted_at + timedelta(seconds=target_age)
        expired = scheduled_for < discovered_at
        specs[target_age] = _ScheduleSpec(
            target_age_seconds=target_age,
            scheduled_for=scheduled_for,
            quality="skipped_late" if expired else "pending",
            observed_at=None,
            actual_age_seconds=None,
            late_seconds=None,
            post_data=None,
            discovery=False,
        )

    discovery_target: int | None = None
    if discovery_post is not None:
        discovery_target = floor_seconds(post.posted_at, discovered_at)
        discovery_scheduled = post.posted_at + timedelta(seconds=discovery_target)
        discovery_late = floor_seconds(discovery_scheduled, discovered_at)
        specs[discovery_target] = _ScheduleSpec(
            target_age_seconds=discovery_target,
            scheduled_for=discovery_scheduled,
            quality="late" if discovery_late > 0 else "observed",
            observed_at=discovered_at,
            actual_age_seconds=discovery_target,
            late_seconds=discovery_late,
            post_data=discovery_post,
            discovery=True,
        )

    target_ids: list[UUID] = []
    discovery_id: UUID | None = None
    skipped = 0
    for target_age, spec in sorted(specs.items()):
        snapshot_id, _ = await _insert_scheduled_snapshot(
            session,
            post_id=post_id,
            collector_version=collector_version,
            spec=spec,
            rules=rules,
        )
        if spec.discovery:
            discovery_id = snapshot_id
        if target_age in horizons:
            target_ids.append(snapshot_id)
            if spec.quality == "skipped_late":
                skipped += 1
    await session.flush()
    return SnapshotScheduleResult(
        post_id=post_id,
        target_snapshot_ids=tuple(target_ids),
        discovery_snapshot_id=discovery_id,
        skipped_late=skipped,
    )


def _error_ids(error: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for key in ("resource_id", "value", "id"):
        value = error.get(key)
        if isinstance(value, str | int):
            values.append(str(value))
        elif isinstance(value, list):
            values.extend(str(item) for item in value if isinstance(item, str | int))
    return tuple(dict.fromkeys(values))


def classify_lookup_error(error: Mapping[str, Any]) -> str:
    """Classify only explicit provider facts; ambiguous not-found stays unavailable."""

    text = " ".join(
        str(error.get(key) or "") for key in ("title", "detail", "type", "reason")
    ).casefold()
    if "protected" in text:
        return "protected"
    if "deleted" in text or "tombstone" in text:
        return "deleted"
    return "unavailable"


def _mark_status(
    snapshot: PostMetricSnapshot,
    post: Post,
    *,
    quality: str,
    observed_at: datetime,
    raw: dict[str, Any],
) -> None:
    snapshot.quality = quality
    snapshot.observed_at = observed_at
    snapshot.actual_age_seconds = floor_seconds(post.posted_at, observed_at)
    snapshot.late_seconds = max(0, floor_seconds(snapshot.scheduled_for, observed_at))
    snapshot.likes = None
    snapshot.reposts = None
    snapshot.replies = None
    snapshot.quotes = None
    snapshot.bookmarks = None
    snapshot.impressions = None
    snapshot.author_followers = None
    snapshot.engagement = None
    snapshot.raw = json_safe(raw)
    post.state = quality
    post.updated_at = observed_at


def _mark_skipped_duplicate_observation(
    snapshot: PostMetricSnapshot,
    *,
    selected_snapshot_id: UUID,
    observed_at: datetime,
) -> None:
    snapshot.quality = "skipped_late"
    snapshot.observed_at = None
    snapshot.actual_age_seconds = None
    snapshot.late_seconds = max(0, floor_seconds(snapshot.scheduled_for, observed_at))
    snapshot.raw = {
        "reason": "one provider observation cannot represent multiple missed horizons",
        "selected_snapshot_id": str(selected_snapshot_id),
        "attempted_at": observed_at.astimezone(UTC).isoformat(),
    }


def _apply_observation(
    snapshot: PostMetricSnapshot,
    stored_post: Post,
    observed: PostData,
    *,
    observed_at: datetime,
    rules: RulesConfig,
) -> None:
    actual_age = floor_seconds(stored_post.posted_at, observed_at)
    late_seconds = max(0, floor_seconds(snapshot.scheduled_for, observed_at))
    values = _metric_values(observed, rules)
    snapshot.observed_at = observed_at
    snapshot.actual_age_seconds = actual_age
    snapshot.late_seconds = late_seconds
    snapshot.quality = "late" if late_seconds > 0 else "observed"
    snapshot.likes = values["likes"]
    snapshot.reposts = values["reposts"]
    snapshot.replies = values["replies"]
    snapshot.quotes = values["quotes"]
    snapshot.bookmarks = values["bookmarks"]
    snapshot.impressions = values["impressions"]
    snapshot.author_followers = values["author_followers"]
    snapshot.engagement = values["engagement"]
    snapshot.raw = values["raw"]
    stored_post.state = "active"
    stored_post.last_seen_at = max(stored_post.last_seen_at, observed_at)
    stored_post.updated_at = observed_at


async def _hydrate_missing_engagement(
    session: AsyncSession,
    post_ids: Sequence[str],
    rules: RulesConfig,
) -> None:
    """Make task-7 discovery observations usable without inventing missing metrics."""

    if not post_ids:
        return
    rows = list(
        (
            await session.scalars(
                select(PostMetricSnapshot)
                .where(
                    PostMetricSnapshot.post_id.in_(sorted(set(post_ids))),
                    PostMetricSnapshot.quality.in_(("observed", "late")),
                    PostMetricSnapshot.engagement.is_(None),
                    PostMetricSnapshot.likes.is_not(None),
                    PostMetricSnapshot.reposts.is_not(None),
                    PostMetricSnapshot.quotes.is_not(None),
                    PostMetricSnapshot.replies.is_not(None),
                )
                .order_by(PostMetricSnapshot.post_id, PostMetricSnapshot.observed_at)
                .with_for_update()
            )
        ).all()
    )
    weights = _engagement_weights(rules)
    for row in rows:
        if row.likes is None or row.reposts is None or row.quotes is None or row.replies is None:
            continue
        row.engagement = weighted_engagement(
            likes=row.likes,
            reposts=row.reposts,
            quotes=row.quotes,
            replies=row.replies,
            bookmarks=row.bookmarks,
            weights=weights,
        )


async def collect_snapshot_rows(
    session: AsyncSession,
    provider: SnapshotProvider,
    snapshot_ids: Sequence[UUID],
    rules: RulesConfig,
    *,
    observed_at: datetime | None = None,
    topic_window_minutes: int = DEFAULT_TOPIC_WINDOW_MINUTES,
    topic_providers: Sequence[TopicSignalProvider] = (),
) -> SnapshotBatchResult:
    """Look up at most 100 posts, update precreated rows, then run public workflows."""

    unique_snapshot_ids = tuple(dict.fromkeys(snapshot_ids))
    if not unique_snapshot_ids:
        return SnapshotBatchResult((), (), 0, 0)
    rows = list(
        (
            await session.scalars(
                select(PostMetricSnapshot)
                .where(PostMetricSnapshot.id.in_(unique_snapshot_ids))
                .order_by(PostMetricSnapshot.post_id, PostMetricSnapshot.scheduled_for)
                .with_for_update()
            )
        ).all()
    )
    if len(rows) != len(unique_snapshot_ids):
        found = {item.id for item in rows}
        missing = sorted(str(item) for item in unique_snapshot_ids if item not in found)
        raise ValueError(f"unknown snapshot IDs: {missing}")
    non_pending = [item.id for item in rows if item.quality != "pending"]
    if non_pending:
        raise ValueError(f"snapshot rows are not pending: {[str(item) for item in non_pending]}")
    grouped: dict[str, list[PostMetricSnapshot]] = defaultdict(list)
    for row in rows:
        grouped[row.post_id].append(row)
    post_ids = tuple(sorted(grouped))
    if len(post_ids) > MAX_LOOKUP_POSTS:
        raise ValueError("snapshot collection accepts at most 100 unique posts")

    lookup = await provider.lookup_posts(post_ids)
    collection_time = observed_at or utcnow()
    if collection_time.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    await upsert_rate_limit(session, lookup.rate_limit)
    returned = {post.id: post for post in lookup.posts if post.id in grouped}
    errors_by_id: dict[str, dict[str, Any]] = {}
    for raw_error in lookup.errors:
        error = dict(raw_error)
        for post_id in _error_ids(error):
            if post_id in grouped:
                errors_by_id[post_id] = error

    post_rows = list(
        (await session.scalars(select(Post).where(Post.id.in_(post_ids)).with_for_update())).all()
    )
    posts = {post.id: post for post in post_rows}
    if len(posts) != len(post_ids):
        missing_posts = sorted(set(post_ids) - set(posts))
        raise RuntimeError(f"snapshot rows reference missing posts: {missing_posts}")

    selected_observations: list[PostMetricSnapshot] = []
    terminal_outcomes: list[SnapshotOutcome] = []
    for post_id in post_ids:
        targets = grouped[post_id]
        stored_post = posts[post_id]
        observed_post = returned.get(post_id)
        if observed_post is None:
            lookup_error = errors_by_id.get(post_id)
            quality = (
                classify_lookup_error(lookup_error) if lookup_error is not None else "unavailable"
            )
            raw = {
                "provider_error": lookup_error,
                "lookup_missing": lookup_error is None,
                "lookup_observed_at": collection_time,
            }
            for target in targets:
                _mark_status(
                    target,
                    stored_post,
                    quality=quality,
                    observed_at=collection_time,
                    raw=raw,
                )
                terminal_outcomes.append(
                    SnapshotOutcome(
                        snapshot_id=target.id,
                        post_id=post_id,
                        quality=quality,
                        observed_at=collection_time,
                        actual_age_seconds=target.actual_age_seconds,
                        topic=None,
                        scoring=None,
                    )
                )
            continue

        actual_age = floor_seconds(stored_post.posted_at, collection_time)
        selected = min(
            targets,
            key=lambda target: (
                abs(target.target_age_seconds - actual_age),
                -target.target_age_seconds,
                str(target.id),
            ),
        )
        _apply_observation(
            selected,
            stored_post,
            observed_post,
            observed_at=collection_time,
            rules=rules,
        )
        selected_observations.append(selected)
        for target in targets:
            if target.id != selected.id:
                _mark_skipped_duplicate_observation(
                    target,
                    selected_snapshot_id=selected.id,
                    observed_at=collection_time,
                )
                terminal_outcomes.append(
                    SnapshotOutcome(
                        snapshot_id=target.id,
                        post_id=post_id,
                        quality="skipped_late",
                        observed_at=None,
                        actual_age_seconds=None,
                        topic=None,
                        scoring=None,
                    )
                )
    await _hydrate_missing_engagement(
        session, [item.post_id for item in selected_observations], rules
    )
    await session.flush()

    ordered_observations = sorted(
        selected_observations, key=lambda item: (item.post_id, str(item.id))
    )
    assignments: dict[UUID, TopicAssignment] = {}
    for snapshot in ordered_observations:
        assignments[snapshot.id] = await assign_post_to_topic(
            session,
            snapshot.post_id,
            assigned_at=collection_time,
            providers=topic_providers,
        )

    # A later bridge assignment can merge an earlier winner. Resolve every alias only
    # after all assignments, then materialize one immutable snapshot per final cluster.
    assignments_by_snapshot: dict[UUID, TopicAssignment] = {}
    cluster_ids: set[UUID] = set()
    for snapshot in ordered_observations:
        assignment = assignments[snapshot.id]
        root = await resolve_topic_cluster(session, assignment.cluster_id)
        resolved = TopicAssignment(
            cluster_id=root.id,
            canonical_key=root.canonical_key,
            topic_key=root.topic_key,
            niche_key=root.niche_key,
            merged_cluster_ids=assignment.merged_cluster_ids,
            evidence=assignment.evidence,
        )
        assignments_by_snapshot[snapshot.id] = resolved
        cluster_ids.add(root.id)

    topic_snapshots: dict[UUID, TopicSnapshotResult] = {}
    for cluster_id in sorted(cluster_ids, key=str):
        topic_snapshots[cluster_id] = await create_topic_snapshot(
            session,
            cluster_id,
            observed_at=collection_time,
            window_minutes=topic_window_minutes,
        )
    topic_scores = {
        cluster_id: await score_topic_snapshot(
            session,
            topic_snapshot.snapshot_id,
            rules,
            as_of=collection_time,
        )
        for cluster_id, topic_snapshot in sorted(
            topic_snapshots.items(), key=lambda item: str(item[0])
        )
    }

    workflow_outcomes: list[SnapshotOutcome] = []
    candidates_by_key: dict[str, AlertCandidate] = {}
    for snapshot in ordered_observations:
        assignment = assignments_by_snapshot[snapshot.id]
        topic_snapshot = topic_snapshots[assignment.cluster_id]
        topic_score = topic_scores[assignment.cluster_id]
        post_score = await score_post_snapshot(
            session,
            snapshot.id,
            rules,
            topic_snapshot_id=topic_snapshot.snapshot_id,
            as_of=collection_time,
        )
        workflow_candidates = tuple(
            candidate
            for candidate in (post_score.candidate, topic_score.candidate)
            if candidate is not None
        )
        for candidate in workflow_candidates:
            candidates_by_key.setdefault(candidate.dedupe_key, candidate)
        workflow_outcomes.append(
            SnapshotOutcome(
                snapshot_id=snapshot.id,
                post_id=snapshot.post_id,
                quality=snapshot.quality,
                observed_at=snapshot.observed_at,
                actual_age_seconds=snapshot.actual_age_seconds,
                topic=TopicProcessingResult(
                    assignment=assignment,
                    snapshot=topic_snapshot,
                ),
                scoring=ScoringWorkflowResult(
                    post=post_score,
                    topic=topic_score,
                    candidates=workflow_candidates,
                ),
            )
        )
    outcomes = tuple(
        sorted(
            terminal_outcomes + workflow_outcomes,
            key=lambda item: (item.post_id, str(item.snapshot_id)),
        )
    )
    return SnapshotBatchResult(
        outcomes=outcomes,
        candidates=tuple(candidates_by_key.values()),
        requested_posts=len(post_ids),
        returned_posts=len(returned),
    )


async def due_snapshot_ids(
    session: AsyncSession,
    *,
    as_of: datetime,
    limit: int = MAX_LOOKUP_POSTS,
) -> tuple[UUID, ...]:
    """Lock and choose the oldest due target for each of at most 100 posts."""

    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if not 1 <= limit <= MAX_LOOKUP_POSTS:
        raise ValueError("limit must be between 1 and 100")
    due = list(
        (
            await session.scalars(
                select(PostMetricSnapshot)
                .where(
                    PostMetricSnapshot.quality == "pending",
                    PostMetricSnapshot.scheduled_for <= as_of,
                )
                .order_by(
                    PostMetricSnapshot.scheduled_for,
                    PostMetricSnapshot.post_id,
                    PostMetricSnapshot.id,
                )
                .limit(limit * len(SNAPSHOT_HORIZONS_SECONDS))
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    selected: list[UUID] = []
    seen_posts: set[str] = set()
    for snapshot in due:
        if snapshot.post_id in seen_posts:
            continue
        selected.append(snapshot.id)
        seen_posts.add(snapshot.post_id)
        if len(selected) == limit:
            break
    return tuple(selected)


async def collect_due_snapshots(
    session: AsyncSession,
    provider: SnapshotProvider,
    rules: RulesConfig,
    *,
    as_of: datetime | None = None,
    limit: int = MAX_LOOKUP_POSTS,
    topic_window_minutes: int = DEFAULT_TOPIC_WINDOW_MINUTES,
    topic_providers: Sequence[TopicSignalProvider] = (),
) -> SnapshotBatchResult:
    collection_time = as_of or utcnow()
    snapshot_ids = await due_snapshot_ids(session, as_of=collection_time, limit=limit)
    if not snapshot_ids:
        return SnapshotBatchResult((), (), 0, 0)
    return await collect_snapshot_rows(
        session,
        provider,
        snapshot_ids,
        rules,
        observed_at=collection_time,
        topic_window_minutes=topic_window_minutes,
        topic_providers=topic_providers,
    )


async def mark_snapshot_failed(
    session: AsyncSession,
    snapshot_id: UUID,
    error: Exception,
    *,
    attempted_at: datetime | None = None,
) -> bool:
    row = await session.scalar(
        select(PostMetricSnapshot).where(PostMetricSnapshot.id == snapshot_id).with_for_update()
    )
    if row is None:
        raise ValueError(f"unknown snapshot: {snapshot_id}")
    if row.quality != "pending":
        return False
    now = attempted_at or utcnow()
    if now.tzinfo is None:
        raise ValueError("attempted_at must be timezone-aware")
    post = await session.get(Post, row.post_id)
    row.quality = "failed"
    row.observed_at = now
    row.actual_age_seconds = floor_seconds(post.posted_at, now) if post is not None else None
    row.late_seconds = max(0, floor_seconds(row.scheduled_for, now))
    row.raw = {
        "error_class": type(error).__name__,
        "error_detail": str(error)[:4000],
    }
    await session.flush()
    return True


async def process_snapshot_job(
    session: AsyncSession,
    provider: SnapshotProvider,
    rules: RulesConfig,
    payload: Mapping[str, Any],
    *,
    observed_at: datetime | None = None,
    topic_window_minutes: int = DEFAULT_TOPIC_WINDOW_MINUTES,
    topic_providers: Sequence[TopicSignalProvider] = (),
) -> SnapshotBatchResult:
    """Process the ingestion service's versioned durable ``snapshot`` payload.

    Preferred payload fields are ``schema_version``, ``post_id``,
    ``target_age_seconds``, ``collector_version``, and ``scheduled_for``. A direct
    ``snapshot_id`` is also accepted for callers that already resolved the row.
    """

    raw_snapshot_id = payload.get("snapshot_id")
    if raw_snapshot_id is not None:
        try:
            snapshot_id = UUID(str(raw_snapshot_id))
        except ValueError as error:
            raise ValueError("snapshot_id must be a UUID") from error
    else:
        if payload.get("schema_version") != 1:
            raise ValueError("snapshot job payload requires schema_version=1")
        post_id = payload.get("post_id")
        collector_version = payload.get("collector_version")
        raw_target = payload.get("target_age_seconds")
        if not isinstance(post_id, str) or not post_id:
            raise ValueError("snapshot job payload requires post_id")
        if not isinstance(collector_version, str) or not collector_version:
            raise ValueError("snapshot job payload requires collector_version")
        if isinstance(raw_target, bool) or not isinstance(raw_target, int | str):
            raise ValueError("target_age_seconds must be an integer")
        try:
            target_age_seconds = int(raw_target)
        except (TypeError, ValueError) as error:
            raise ValueError("target_age_seconds must be an integer") from error
        row = await session.scalar(
            select(PostMetricSnapshot).where(
                PostMetricSnapshot.post_id == post_id,
                PostMetricSnapshot.target_age_seconds == target_age_seconds,
                PostMetricSnapshot.collector_version == collector_version,
            )
        )
        if row is None:
            raise ValueError("snapshot job does not reference a precreated target row")
        raw_scheduled_for = payload.get("scheduled_for")
        if isinstance(raw_scheduled_for, str):
            try:
                scheduled_for = datetime.fromisoformat(raw_scheduled_for.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("scheduled_for must be an ISO-8601 timestamp") from error
            if scheduled_for.tzinfo is None or scheduled_for != row.scheduled_for:
                raise ValueError("snapshot job scheduled_for does not match its target row")
        snapshot_id = row.id

    existing = await session.get(PostMetricSnapshot, snapshot_id)
    if existing is None:
        raise ValueError(f"unknown snapshot: {snapshot_id}")
    if existing.quality != "pending":
        outcome = SnapshotOutcome(
            snapshot_id=existing.id,
            post_id=existing.post_id,
            quality=existing.quality,
            observed_at=existing.observed_at,
            actual_age_seconds=existing.actual_age_seconds,
            topic=None,
            scoring=None,
        )
        return SnapshotBatchResult((outcome,), (), 0, 0)
    return await collect_snapshot_rows(
        session,
        provider,
        (snapshot_id,),
        rules,
        observed_at=observed_at,
        topic_window_minutes=topic_window_minutes,
        topic_providers=topic_providers,
    )


precreate_snapshot_targets = schedule_post_snapshots
collect_snapshots = collect_snapshot_rows
process_snapshot = process_snapshot_job
