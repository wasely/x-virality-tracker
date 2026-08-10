from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import RulesConfig
from x_virality.db.jobs import enqueue_job
from x_virality.db.models import (
    Author,
    AuthorMetricSnapshot,
    Post,
    PostMetricSnapshot,
    PostRuleMatch,
    XRule,
)
from x_virality.db.repositories import rules_by_remote_id, store_failed_event, store_raw_payload
from x_virality.db.session import Database, utcnow
from x_virality.domain.models import SNAPSHOT_HORIZONS_SECONDS, AuthorData, PostData
from x_virality.logging import get_logger
from x_virality.scoring.core import weighted_engagement

STREAM_SOURCE = "filtered_stream"
RECENT_SEARCH_SOURCE = "recent_search"
SNAPSHOT_JOB_KIND = "snapshot"
TOPIC_JOB_KIND = "topic"
SNAPSHOT_COLLECTOR_VERSION = "x-api-v1"
DISCOVERY_COLLECTOR_VERSION = "discovery-v1"
DISCOVERY_TARGET_AGE_SECONDS = 0

_TRACKING_QUERY_RE = re.compile(r"^(?:utm_.+|fbclid|gclid|dclid|msclkid)$", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
logger = get_logger("services.ingestion")


@dataclass(frozen=True, slots=True)
class IngestionResult:
    post_id: str
    raw_payload_id: UUID
    matched_rule_count: int
    unknown_remote_rule_ids: tuple[str, ...]
    snapshot_jobs_created: int
    topic_job_created: bool
    skipped_horizons: tuple[int, ...]


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def normalized_text_hash(value: str) -> str:
    return hashlib.sha256(normalize_text(value).encode("utf-8")).hexdigest()


def _canonical_url(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").rstrip(".").lower()
    if scheme not in {"http", "https"} or not host:
        return None
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return None
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{host}:{port}"
    else:
        netloc = host
    path = parsed.path or "/"
    query = urlencode(
        sorted(
            (key, item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
            if not _TRACKING_QUERY_RE.fullmatch(key)
        ),
        doseq=True,
    )
    return urlunsplit((scheme, netloc, path, query, ""))


def link_fingerprint(entities: Mapping[str, Any]) -> str | None:
    raw_urls = entities.get("urls")
    if not isinstance(raw_urls, Sequence) or isinstance(raw_urls, str | bytes | bytearray):
        return None
    urls: set[str] = set()
    for item in raw_urls:
        if not isinstance(item, Mapping):
            continue
        for key in ("unwound_url", "expanded_url", "url"):
            canonical = _canonical_url(item.get(key))
            if canonical is not None:
                urls.add(canonical)
                break
    if not urls:
        return None
    return hashlib.sha256("\n".join(sorted(urls)).encode("utf-8")).hexdigest()


def snapshot_dedupe_key(post_id: str, target_age_seconds: int, collector_version: str) -> str:
    return f"snapshot:{collector_version}:{post_id}:{target_age_seconds}"


def snapshot_job_payload(
    post_id: str,
    target_age_seconds: int,
    collector_version: str,
    scheduled_for: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "post_id": post_id,
        "target_age_seconds": target_age_seconds,
        "collector_version": collector_version,
        "scheduled_for": _iso(scheduled_for),
        "usage_sensitive": True,
    }


async def _upsert_author(
    session: AsyncSession,
    *,
    author_id: str,
    author: AuthorData | None,
    observed_at: datetime,
) -> None:
    values: dict[str, Any] = {
        "id": author_id,
        "username": author.username if author else None,
        "name": author.name if author else None,
        "account_created_at": author.created_at if author else None,
        "verified": author.verified if author else None,
        "followers_count": author.followers if author else None,
        "following_count": author.following if author else None,
        "post_count": author.posts if author else None,
        "listed_count": author.listed if author else None,
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "raw": author.raw if author else {},
        "created_at": observed_at,
        "updated_at": observed_at,
    }
    statement = pg_insert(Author).values(**values)
    excluded = statement.excluded
    updates: dict[str, Any] = {
        "username": func.coalesce(excluded.username, Author.username),
        "name": func.coalesce(excluded.name, Author.name),
        "account_created_at": func.coalesce(excluded.account_created_at, Author.account_created_at),
        "verified": func.coalesce(excluded.verified, Author.verified),
        "followers_count": func.coalesce(excluded.followers_count, Author.followers_count),
        "following_count": func.coalesce(excluded.following_count, Author.following_count),
        "post_count": func.coalesce(excluded.post_count, Author.post_count),
        "listed_count": func.coalesce(excluded.listed_count, Author.listed_count),
        "first_seen_at": func.least(Author.first_seen_at, excluded.first_seen_at),
        "last_seen_at": func.greatest(Author.last_seen_at, excluded.last_seen_at),
        "updated_at": observed_at,
    }
    if author is not None:
        updates["raw"] = excluded.raw
    await session.execute(statement.on_conflict_do_update(index_elements=[Author.id], set_=updates))

    if author is None:
        return
    metric_statement = pg_insert(AuthorMetricSnapshot).values(
        id=uuid4(),
        author_id=author.id,
        observed_at=observed_at,
        followers_count=author.followers,
        following_count=author.following,
        post_count=author.posts,
        listed_count=author.listed,
        raw=author.raw,
    )
    metric_excluded = metric_statement.excluded
    await session.execute(
        metric_statement.on_conflict_do_update(
            index_elements=[AuthorMetricSnapshot.author_id, AuthorMetricSnapshot.observed_at],
            set_={
                "followers_count": func.coalesce(
                    metric_excluded.followers_count,
                    AuthorMetricSnapshot.followers_count,
                ),
                "following_count": func.coalesce(
                    metric_excluded.following_count,
                    AuthorMetricSnapshot.following_count,
                ),
                "post_count": func.coalesce(
                    metric_excluded.post_count,
                    AuthorMetricSnapshot.post_count,
                ),
                "listed_count": func.coalesce(
                    metric_excluded.listed_count,
                    AuthorMetricSnapshot.listed_count,
                ),
                "raw": metric_excluded.raw,
            },
        )
    )


async def _upsert_post(
    session: AsyncSession,
    post: PostData,
    *,
    observed_at: datetime,
) -> None:
    statement = pg_insert(Post).values(
        id=post.id,
        author_id=post.author_id,
        text=post.text,
        language=post.language,
        conversation_id=post.conversation_id,
        reply_to_post_id=post.reference_id("replied_to"),
        quoted_post_id=post.reference_id("quoted"),
        reposted_post_id=post.reference_id("retweeted"),
        posted_at=post.created_at,
        first_seen_at=observed_at,
        last_seen_at=observed_at,
        normalized_text_hash=normalized_text_hash(post.text),
        link_fingerprint=link_fingerprint(post.entities),
        entities=post.entities,
        context_annotations=list(post.context_annotations),
        state="active",
        created_at=observed_at,
        updated_at=observed_at,
    )
    excluded = statement.excluded
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=[Post.id],
            set_={
                "author_id": excluded.author_id,
                "text": excluded.text,
                "language": func.coalesce(excluded.language, Post.language),
                "conversation_id": func.coalesce(excluded.conversation_id, Post.conversation_id),
                "reply_to_post_id": func.coalesce(excluded.reply_to_post_id, Post.reply_to_post_id),
                "quoted_post_id": func.coalesce(excluded.quoted_post_id, Post.quoted_post_id),
                "reposted_post_id": func.coalesce(excluded.reposted_post_id, Post.reposted_post_id),
                "posted_at": excluded.posted_at,
                "first_seen_at": func.least(Post.first_seen_at, excluded.first_seen_at),
                "last_seen_at": func.greatest(Post.last_seen_at, excluded.last_seen_at),
                "normalized_text_hash": excluded.normalized_text_hash,
                "link_fingerprint": func.coalesce(excluded.link_fingerprint, Post.link_fingerprint),
                "entities": excluded.entities,
                "context_annotations": excluded.context_annotations,
                "state": "active",
                "updated_at": observed_at,
            },
        )
    )


def _matched_tags(raw_payload: Mapping[str, Any]) -> dict[str, str | None]:
    raw_matches = raw_payload.get("matching_rules")
    if not isinstance(raw_matches, Sequence) or isinstance(raw_matches, str | bytes | bytearray):
        return {}
    matches: dict[str, str | None] = {}
    for item in raw_matches:
        if not isinstance(item, Mapping) or item.get("id") is None:
            continue
        tag = item.get("tag")
        matches[str(item["id"])] = str(tag) if tag is not None else None
    return matches


async def _store_rule_matches(
    session: AsyncSession,
    post: PostData,
    *,
    observed_at: datetime,
    raw_payload: Mapping[str, Any],
    local_rule_id: UUID | None,
    local_matched_tag: str | None,
) -> tuple[int, tuple[str, ...]]:
    matches: dict[UUID, str | None] = {}
    unknown: list[str] = []
    tags = _matched_tags(raw_payload)
    if post.matching_rule_ids:
        remote_rules = await rules_by_remote_id(session)
        for remote_id in post.matching_rule_ids:
            rule = remote_rules.get(remote_id)
            if rule is None:
                unknown.append(remote_id)
                continue
            matches.setdefault(rule.id, tags.get(remote_id) or rule.managed_tag)
    if local_rule_id is not None:
        matched_tag = local_matched_tag
        if matched_tag is None:
            local_rule = await session.get(XRule, local_rule_id)
            if local_rule is None:
                raise ValueError(f"local recovery rule does not exist: {local_rule_id}")
            matched_tag = local_rule.managed_tag
        matches.setdefault(local_rule_id, matched_tag)

    for rule_id, matched_tag in sorted(matches.items(), key=lambda item: str(item[0])):
        await session.execute(
            pg_insert(PostRuleMatch)
            .values(
                post_id=post.id,
                rule_id=rule_id,
                first_seen_at=observed_at,
                matched_tag=matched_tag,
            )
            .on_conflict_do_nothing(index_elements=[PostRuleMatch.post_id, PostRuleMatch.rule_id])
        )
    return len(matches), tuple(sorted(set(unknown)))


def _engagement_weights(rules: RulesConfig | None) -> dict[str, Decimal] | None:
    if rules is None:
        return None
    return {
        key: Decimal(str(value)) for key, value in rules.scoring.engagement.model_dump().items()
    }


async def _store_discovery_observation(
    session: AsyncSession,
    post: PostData,
    *,
    observed_at: datetime,
    rules: RulesConfig | None,
) -> None:
    actual_age = max(0, math.floor((observed_at - post.created_at).total_seconds()))
    await session.execute(
        pg_insert(PostMetricSnapshot)
        .values(
            id=uuid4(),
            post_id=post.id,
            target_age_seconds=DISCOVERY_TARGET_AGE_SECONDS,
            collector_version=DISCOVERY_COLLECTOR_VERSION,
            scheduled_for=post.created_at,
            observed_at=observed_at,
            actual_age_seconds=actual_age,
            late_seconds=actual_age,
            quality="late" if actual_age > 0 else "observed",
            likes=post.metrics.likes,
            reposts=post.metrics.reposts,
            replies=post.metrics.replies,
            quotes=post.metrics.quotes,
            bookmarks=post.metrics.bookmarks,
            impressions=post.metrics.impressions,
            author_followers=post.author.followers if post.author else None,
            engagement=weighted_engagement(
                likes=post.metrics.likes,
                reposts=post.metrics.reposts,
                quotes=post.metrics.quotes,
                replies=post.metrics.replies,
                bookmarks=post.metrics.bookmarks,
                weights=_engagement_weights(rules),
            ),
            raw=post.raw,
        )
        .on_conflict_do_nothing(
            index_elements=[
                PostMetricSnapshot.post_id,
                PostMetricSnapshot.target_age_seconds,
                PostMetricSnapshot.collector_version,
            ]
        )
    )


async def _schedule_topic_discovery(
    session: AsyncSession,
    post: PostData,
    *,
    observed_at: datetime,
) -> bool:
    job_id = await enqueue_job(
        session,
        kind=TOPIC_JOB_KIND,
        dedupe_key=f"topic:v1:{post.id}",
        payload={
            "schema_version": 1,
            "post_id": post.id,
            "observed_at": _iso(observed_at),
        },
        run_at=observed_at,
        priority=40,
        max_attempts=8,
    )
    return job_id is not None


async def _schedule_horizons(
    session: AsyncSession,
    post: PostData,
    *,
    observed_at: datetime,
) -> tuple[int, tuple[int, ...]]:
    jobs_created = 0
    skipped: list[int] = []
    for horizon in SNAPSHOT_HORIZONS_SECONDS:
        scheduled_for = post.created_at + timedelta(seconds=horizon)
        expired = scheduled_for < observed_at
        late_seconds = (
            max(0, math.ceil((observed_at - scheduled_for).total_seconds())) if expired else None
        )
        await session.execute(
            pg_insert(PostMetricSnapshot)
            .values(
                id=uuid4(),
                post_id=post.id,
                target_age_seconds=horizon,
                collector_version=SNAPSHOT_COLLECTOR_VERSION,
                scheduled_for=scheduled_for,
                observed_at=None,
                actual_age_seconds=None,
                late_seconds=late_seconds,
                quality="skipped_late" if expired else "pending",
                likes=None,
                reposts=None,
                replies=None,
                quotes=None,
                bookmarks=None,
                impressions=None,
                author_followers=None,
                engagement=None,
                raw=None,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    PostMetricSnapshot.post_id,
                    PostMetricSnapshot.target_age_seconds,
                    PostMetricSnapshot.collector_version,
                ]
            )
        )
        if expired:
            skipped.append(horizon)
            continue
        job_id = await enqueue_job(
            session,
            kind=SNAPSHOT_JOB_KIND,
            dedupe_key=snapshot_dedupe_key(post.id, horizon, SNAPSHOT_COLLECTOR_VERSION),
            payload=snapshot_job_payload(
                post.id,
                horizon,
                SNAPSHOT_COLLECTOR_VERSION,
                scheduled_for,
            ),
            run_at=scheduled_for,
            priority=50,
            max_attempts=8,
        )
        jobs_created += job_id is not None
    return jobs_created, tuple(skipped)


async def ingest_post_in_transaction(
    session: AsyncSession,
    post: PostData,
    *,
    source: str,
    observed_at: datetime | None = None,
    raw_payload: Mapping[str, Any] | None = None,
    local_rule_id: UUID | None = None,
    local_matched_tag: str | None = None,
    rules: RulesConfig | None = None,
) -> IngestionResult:
    """Persist raw, canonical, matching, and scheduling state in the caller transaction."""

    if not source or len(source) > 32:
        raise ValueError("source must contain 1-32 characters")
    seen_at = _aware_utc(observed_at or utcnow(), field="observed_at")
    created_at = _aware_utc(post.created_at, field="post.created_at")
    if created_at != post.created_at:
        post = PostData(
            id=post.id,
            author_id=post.author_id,
            text=post.text,
            created_at=created_at,
            conversation_id=post.conversation_id,
            language=post.language,
            references=post.references,
            entities=post.entities,
            context_annotations=post.context_annotations,
            metrics=post.metrics,
            author=post.author,
            matching_rule_ids=post.matching_rule_ids,
            raw=post.raw,
        )
    payload = dict(raw_payload) if raw_payload is not None else post.raw
    raw_payload_id = await store_raw_payload(
        session,
        source=source,
        object_type="post",
        object_id=post.id,
        payload=payload,
        seen_at=seen_at,
    )
    await _upsert_author(
        session,
        author_id=post.author_id,
        author=post.author,
        observed_at=seen_at,
    )
    await _upsert_post(session, post, observed_at=seen_at)
    matched_count, unknown = await _store_rule_matches(
        session,
        post,
        observed_at=seen_at,
        raw_payload=payload,
        local_rule_id=local_rule_id,
        local_matched_tag=local_matched_tag,
    )
    await _store_discovery_observation(
        session,
        post,
        observed_at=seen_at,
        rules=rules,
    )
    topic_job_created = await _schedule_topic_discovery(
        session,
        post,
        observed_at=seen_at,
    )
    jobs_created, skipped = await _schedule_horizons(session, post, observed_at=seen_at)
    return IngestionResult(
        post_id=post.id,
        raw_payload_id=raw_payload_id,
        matched_rule_count=matched_count,
        unknown_remote_rule_ids=unknown,
        snapshot_jobs_created=jobs_created,
        topic_job_created=topic_job_created,
        skipped_horizons=skipped,
    )


async def record_failed_ingestion(
    database: Database,
    *,
    source: str,
    payload: Mapping[str, Any],
    error: Exception,
) -> UUID | None:
    try:
        async with database.transaction() as session:
            return await store_failed_event(
                session,
                source=source,
                payload=dict(payload),
                error=error,
            )
    except Exception as persistence_error:
        logger.error(
            "failed_event_persistence_failed",
            source=source,
            error_class=type(persistence_error).__name__,
            error=str(persistence_error),
        )
        return None


async def ingest_post(
    database: Database,
    post: PostData,
    *,
    source: str,
    observed_at: datetime | None = None,
    raw_payload: Mapping[str, Any] | None = None,
    local_rule_id: UUID | None = None,
    local_matched_tag: str | None = None,
    rules: RulesConfig | None = None,
) -> IngestionResult:
    payload = dict(raw_payload) if raw_payload is not None else post.raw
    try:
        async with database.transaction() as session:
            return await ingest_post_in_transaction(
                session,
                post,
                source=source,
                observed_at=observed_at,
                raw_payload=payload,
                local_rule_id=local_rule_id,
                local_matched_tag=local_matched_tag,
                rules=rules,
            )
    except Exception as error:
        await record_failed_ingestion(
            database,
            source=source,
            payload=payload,
            error=error,
        )
        raise


async def pending_snapshot_count(session: AsyncSession, post_id: str) -> int:
    value = await session.scalar(
        select(func.count())
        .select_from(PostMetricSnapshot)
        .where(
            PostMetricSnapshot.post_id == post_id,
            PostMetricSnapshot.collector_version == SNAPSHOT_COLLECTOR_VERSION,
            PostMetricSnapshot.quality == "pending",
        )
    )
    return int(value or 0)


canonical_text_hash = normalized_text_hash
external_link_fingerprint = link_fingerprint
