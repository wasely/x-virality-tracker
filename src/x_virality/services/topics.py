from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Protocol
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.db.models import (
    Author,
    Post,
    PostRuleMatch,
    TopicCluster,
    TopicMember,
    TopicSignal,
    TopicSnapshot,
    XRule,
)
from x_virality.scoring.core import (
    MetricObservation,
    actual_interval_features,
    canonical_input_hash,
    clamp,
    json_safe,
    median,
)

TOPIC_ASSIGNMENT_VERSION = "deterministic-topics-v1"
TOPIC_SNAPSHOT_VERSION = "topic-snapshot-v1"
DEFAULT_TOPIC_WINDOW_MINUTES = 60
BASELINE_LOOKBACK_DAYS = 7
NEW_ACCOUNT_DAYS = 30
_MAX_BASELINE_SNAPSHOTS = 200

_TRACKING_PARAMETERS = frozenset(
    {
        "dclid",
        "fbclid",
        "gclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "mkt_tok",
        "msclkid",
        "ref_src",
        "ref_url",
        "s_cid",
        "spm",
        "vero_conv",
        "vero_id",
        "yclid",
    }
)
_X_HOSTS = frozenset(
    {
        "x.com",
        "www.x.com",
        "mobile.x.com",
        "twitter.com",
        "www.twitter.com",
        "mobile.twitter.com",
        "t.co",
        "www.t.co",
    }
)
_TOKEN_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class TopicPostView:
    id: str
    text: str
    conversation_id: str | None
    reply_to_post_id: str | None
    quoted_post_id: str | None
    reposted_post_id: str | None
    normalized_text_hash: str
    entities: dict[str, Any]
    context_annotations: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class TopicEvidence:
    kind: str
    value: str
    strength: Decimal
    provider: str
    details: dict[str, Any]


class TopicSignalProvider(Protocol):
    """Optional deterministic signal extension; release v1 configures none."""

    name: str
    version: str

    async def signals_for(self, post: TopicPostView) -> Sequence[TopicEvidence]: ...


@dataclass(frozen=True, slots=True)
class TopicAssignment:
    cluster_id: UUID
    canonical_key: str
    topic_key: str
    niche_key: str
    merged_cluster_ids: tuple[UUID, ...]
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TopicSnapshotResult:
    snapshot_id: UUID
    cluster_id: UUID
    post_count: int
    unique_authors: int
    topic_spike: Decimal | None
    created: bool


@dataclass(frozen=True, slots=True)
class TopicProcessingResult:
    assignment: TopicAssignment
    snapshot: TopicSnapshotResult


def _normalized_token(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value)).casefold()
    return _TOKEN_RE.sub("-", normalized).strip("-")


def canonicalize_external_url(value: str) -> str | None:
    """Canonicalize an external HTTP URL for identity, excluding X self-links."""

    candidate = unicodedata.normalize("NFKC", value).strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    try:
        parsed = urlsplit(candidate)
        host = (parsed.hostname or "").encode("idna").decode("ascii").lower().rstrip(".")
        port = parsed.port
    except (UnicodeError, ValueError):
        return None
    if parsed.scheme.lower() not in {"http", "https"} or not host or host in _X_HOSTS:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if host.startswith("www."):
        host = host[4:]
    netloc = host
    if port is not None and port not in {80, 443}:
        netloc = f"{host}:{port}"

    decoded_path = unquote(parsed.path or "/", errors="replace")
    path_parts = [part for part in decoded_path.split("/") if part not in {"", "."}]
    normalized_parts: list[str] = []
    for part in path_parts:
        if part == "..":
            if normalized_parts:
                normalized_parts.pop()
            continue
        normalized_parts.append(part)
    path = "/" + "/".join(quote(part, safe="!$&'()*+,;=:@~-._") for part in normalized_parts)
    if path != "/":
        path = path.rstrip("/")

    query_items = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_") and key.casefold() not in _TRACKING_PARAMETERS
    ]
    query_items.sort(key=lambda pair: (pair[0].casefold(), pair[0], pair[1]))
    query = urlencode(query_items, doseq=True)
    return urlunsplit(("https", netloc, path, query, ""))


def _external_urls(entities: dict[str, Any]) -> tuple[str, ...]:
    raw_urls = entities.get("urls")
    records = raw_urls if isinstance(raw_urls, list) else []
    canonical: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        for key in ("unwound_url", "expanded_url", "url"):
            raw = record.get(key)
            if not isinstance(raw, str):
                continue
            normalized = canonicalize_external_url(raw)
            if normalized is not None:
                canonical.add(normalized)
                break
    return tuple(sorted(canonical))


def _entity_signature(
    entities: dict[str, Any], context_annotations: Sequence[dict[str, Any]]
) -> tuple[str, tuple[str, ...]] | None:
    context_tokens: set[str] = set()
    for annotation in context_annotations:
        domain = annotation.get("domain")
        entity = annotation.get("entity")
        domain = domain if isinstance(domain, dict) else {}
        entity = entity if isinstance(entity, dict) else {}
        domain_id = _normalized_token(domain.get("id") or domain.get("name") or "")
        entity_id = _normalized_token(entity.get("id") or entity.get("name") or "")
        if entity_id:
            context_tokens.add(f"context:{domain_id or 'unknown'}:{entity_id}")

    hashtags_raw = entities.get("hashtags")
    hashtags = hashtags_raw if isinstance(hashtags_raw, list) else []
    hashtag_tokens = {
        f"hashtag:{token}"
        for item in hashtags
        if isinstance(item, dict)
        for token in [_normalized_token(item.get("tag") or item.get("text") or "")]
        if token
    }
    # A single hashtag is intentionally not specific enough. Context entity IDs are.
    selected = context_tokens | (hashtag_tokens if len(hashtag_tokens) >= 2 else set())
    if not selected:
        return None
    tokens = tuple(sorted(selected)[:8])
    digest = hashlib.sha256("\x1f".join(tokens).encode("utf-8")).hexdigest()
    return f"v1:{digest}", tokens


def deterministic_topic_evidence(post: TopicPostView) -> tuple[TopicEvidence, ...]:
    signals: list[TopicEvidence] = []

    def add(kind: str, value: str | None, strength: str, **details: Any) -> None:
        if value:
            signals.append(
                TopicEvidence(
                    kind=kind,
                    value=value,
                    strength=Decimal(strength),
                    provider=TOPIC_ASSIGNMENT_VERSION,
                    details=details,
                )
            )

    add("conversation", post.conversation_id, "1.000000", source="conversation_id")
    original_id = post.reposted_post_id or (
        post.id if post.reply_to_post_id is None and post.quoted_post_id is None else None
    )
    add(
        "original_post",
        original_id,
        "0.980000",
        source="reposted_post_id" if post.reposted_post_id else "post_id",
    )
    add("quoted_post", post.quoted_post_id, "0.950000", source="quoted_post_id")
    for url in _external_urls(post.entities):
        add("external_url", url, "0.850000", canonical_url=url)
    signature = _entity_signature(post.entities, post.context_annotations)
    if signature is not None:
        value, tokens = signature
        add("entity_signature", value, "0.650000", tokens=list(tokens))

    unique: dict[tuple[str, str], TopicEvidence] = {}
    for signal in signals:
        key = (signal.kind, signal.value)
        previous = unique.get(key)
        if previous is None or signal.strength > previous.strength:
            unique[key] = signal
    return tuple(sorted(unique.values(), key=lambda item: (-item.strength, item.kind, item.value)))


def _post_view(post: Post) -> TopicPostView:
    entities = post.entities if isinstance(post.entities, dict) else {}
    annotations = post.context_annotations if isinstance(post.context_annotations, list) else []
    return TopicPostView(
        id=post.id,
        text=post.text,
        conversation_id=post.conversation_id,
        reply_to_post_id=post.reply_to_post_id,
        quoted_post_id=post.quoted_post_id,
        reposted_post_id=post.reposted_post_id,
        normalized_text_hash=post.normalized_text_hash,
        entities=entities,
        context_annotations=tuple(item for item in annotations if isinstance(item, dict)),
    )


def _canonical_cluster_key(signal: TopicEvidence) -> str:
    candidate = f"{signal.kind}:{signal.value}"
    if len(candidate) <= 255:
        return candidate
    return f"{signal.kind}:sha256:{hashlib.sha256(signal.value.encode('utf-8')).hexdigest()}"


async def _classification(session: AsyncSession, post_id: str) -> tuple[str, str, list[str]]:
    rows = (
        await session.execute(
            select(XRule.topic_key, XRule.niche_key, XRule.local_key)
            .join(PostRuleMatch, PostRuleMatch.rule_id == XRule.id)
            .where(PostRuleMatch.post_id == post_id)
            .order_by(XRule.local_key, XRule.topic_key, XRule.niche_key)
        )
    ).all()
    if not rows:
        return "unclassified", "global", []
    topic_key, niche_key, _ = rows[0]
    return str(topic_key), str(niche_key), [str(row.local_key) for row in rows]


async def _root_cluster(
    session: AsyncSession, cluster_id: UUID, *, lock: bool = False
) -> TopicCluster:
    seen: set[UUID] = set()
    current_id = cluster_id
    while current_id not in seen:
        seen.add(current_id)
        statement = select(TopicCluster).where(TopicCluster.id == current_id)
        if lock:
            statement = statement.with_for_update()
        cluster = await session.scalar(statement)
        if cluster is None:
            raise RuntimeError(f"topic cluster {current_id} disappeared")
        if cluster.state != "merged" or cluster.merged_into_id is None:
            return cluster
        current_id = cluster.merged_into_id
    raise RuntimeError("topic cluster merge cycle detected")


async def resolve_topic_cluster(session: AsyncSession, cluster_id: UUID) -> TopicCluster:
    return await _root_cluster(session, cluster_id)


async def _merge_clusters(
    session: AsyncSession, winner: TopicCluster, losers: Sequence[TopicCluster]
) -> tuple[UUID, ...]:
    # Lock every participating cluster in one global order. Locking loser then winner
    # per pair allows two bridge assignments to deadlock in opposite directions.
    cluster_ids = sorted({winner.id, *(item.id for item in losers)}, key=str)
    if cluster_ids:
        await session.scalars(
            select(TopicCluster)
            .where(TopicCluster.id.in_(cluster_ids))
            .order_by(TopicCluster.id)
            .with_for_update()
        )
    merged: list[UUID] = []
    for loser in sorted(losers, key=lambda item: str(item.id)):
        if loser.id == winner.id or loser.state == "merged":
            continue
        members = list(
            (
                await session.scalars(
                    select(TopicMember)
                    .where(TopicMember.cluster_id == loser.id)
                    .order_by(TopicMember.post_id)
                    .with_for_update()
                )
            ).all()
        )
        for member in members:
            existing = await session.scalar(
                select(TopicMember)
                .where(
                    TopicMember.cluster_id == winner.id,
                    TopicMember.post_id == member.post_id,
                )
                .with_for_update()
            )
            if existing is None:
                prior = dict(member.evidence)
                prior["merged_from_cluster_id"] = str(loser.id)
                session.add(
                    TopicMember(
                        cluster_id=winner.id,
                        post_id=member.post_id,
                        evidence=prior,
                        assigned_at=member.assigned_at,
                    )
                )
            else:
                evidence = dict(existing.evidence)
                prior_ids = set(evidence.get("merged_from_cluster_ids", []))
                prior_ids.add(str(loser.id))
                evidence["merged_from_cluster_ids"] = sorted(str(item) for item in prior_ids)
                existing.evidence = evidence
                existing.assigned_at = min(existing.assigned_at, member.assigned_at)
        await session.execute(delete(TopicMember).where(TopicMember.cluster_id == loser.id))
        await session.execute(
            update(TopicSignal)
            .where(TopicSignal.cluster_id == loser.id)
            .values(cluster_id=winner.id)
        )
        await session.execute(
            update(TopicCluster)
            .where(TopicCluster.merged_into_id == loser.id)
            .values(merged_into_id=winner.id)
        )
        winner.first_seen_at = min(winner.first_seen_at, loser.first_seen_at)
        winner.last_activity_at = max(winner.last_activity_at, loser.last_activity_at)
        loser.state = "merged"
        loser.merged_into_id = winner.id
        merged.append(loser.id)
    await session.flush()
    return tuple(merged)


async def assign_post_to_topic(
    session: AsyncSession,
    post_id: str,
    *,
    assigned_at: datetime,
    providers: Sequence[TopicSignalProvider] = (),
) -> TopicAssignment:
    """Assign and merge topic clusters inside the caller's database transaction."""

    if assigned_at.tzinfo is None:
        raise ValueError("assigned_at must be timezone-aware")
    post = await session.scalar(select(Post).where(Post.id == post_id).with_for_update())
    if post is None:
        raise ValueError(f"unknown post: {post_id}")
    view = _post_view(post)
    signals = list(deterministic_topic_evidence(view))
    for provider in sorted(providers, key=lambda item: (item.name, item.version)):
        provided = await provider.signals_for(view)
        for signal in provided:
            if signal.provider != f"{provider.name}:{provider.version}":
                raise ValueError("extension signals must identify their provider and version")
            if (
                not signal.kind
                or not signal.value
                or not Decimal("0") <= signal.strength <= Decimal("1")
            ):
                raise ValueError("extension provider emitted an invalid topic signal")
            signals.append(signal)
    deduplicated: dict[tuple[str, str], TopicEvidence] = {}
    for signal in signals:
        if len(signal.kind) > 32 or len(signal.value) > 512:
            raise ValueError("topic signal kind/value exceeds the persisted schema limit")
        key = (signal.kind, signal.value)
        previous = deduplicated.get(key)
        if previous is None or signal.strength > previous.strength:
            deduplicated[key] = signal
    ordered = sorted(
        deduplicated.values(), key=lambda item: (-item.strength, item.kind, item.value)
    )
    if not ordered:
        raise ValueError(f"post {post_id} has no deterministic topic identity")

    # Serialize assignments sharing any signal. Sorted advisory locks avoid deadlocks and
    # make the globally unique (kind, value) identity safe under concurrent workers.
    for signal in sorted(ordered, key=lambda item: (item.kind, item.value)):
        identity = f"{signal.kind}\x1f{signal.value}"
        await session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(identity, 0)))
        )

    pairs = [(signal.kind, signal.value) for signal in ordered]
    existing_signals = list(
        (
            await session.scalars(
                select(TopicSignal)
                .where(tuple_(TopicSignal.kind, TopicSignal.value).in_(pairs))
                .with_for_update()
            )
        ).all()
    )
    membership_ids = list(
        (
            await session.scalars(
                select(TopicMember.cluster_id)
                .where(TopicMember.post_id == post_id)
                .with_for_update()
            )
        ).all()
    )
    signal_strength = {(signal.kind, signal.value): signal.strength for signal in ordered}
    candidate_strengths: dict[UUID, Decimal] = {}
    for stored in existing_signals:
        root = await _root_cluster(session, stored.cluster_id)
        strength = signal_strength.get((stored.kind, stored.value), stored.strength)
        candidate_strengths[root.id] = max(candidate_strengths.get(root.id, Decimal("0")), strength)
    for cluster_id in membership_ids:
        root = await _root_cluster(session, cluster_id)
        candidate_strengths[root.id] = max(
            candidate_strengths.get(root.id, Decimal("0")), Decimal("1")
        )

    topic_key, niche_key, matched_rules = await _classification(session, post_id)
    candidates: list[TopicCluster] = []
    for cluster_id in sorted(candidate_strengths, key=str):
        candidates.append(await _root_cluster(session, cluster_id))
    if candidates:
        candidates.sort(
            key=lambda cluster: (
                -candidate_strengths[cluster.id],
                cluster.first_seen_at.astimezone(UTC),
                cluster.canonical_key,
                str(cluster.id),
            )
        )
        winner = candidates[0]
    else:
        strongest = ordered[0]
        winner = TopicCluster(
            id=uuid4(),
            canonical_key=_canonical_cluster_key(strongest),
            topic_key=topic_key,
            niche_key=niche_key,
            label=None,
            first_seen_at=min(post.first_seen_at, assigned_at),
            last_activity_at=max(post.posted_at, assigned_at),
            state="active",
            merged_into_id=None,
            created_at=assigned_at,
            updated_at=assigned_at,
        )
        session.add(winner)
        await session.flush()

    merged_ids = await _merge_clusters(
        session, winner, [cluster for cluster in candidates[1:] if cluster.id != winner.id]
    )
    winner = await _root_cluster(session, winner.id, lock=True)
    winner.last_activity_at = max(winner.last_activity_at, post.posted_at, assigned_at)
    winner.updated_at = assigned_at

    for signal in ordered:
        await session.execute(
            pg_insert(TopicSignal)
            .values(
                id=uuid4(),
                cluster_id=winner.id,
                kind=signal.kind,
                value=signal.value,
                strength=signal.strength,
                first_seen_at=min(post.first_seen_at, assigned_at),
            )
            .on_conflict_do_update(
                index_elements=[TopicSignal.kind, TopicSignal.value],
                set_={
                    "cluster_id": winner.id,
                    "strength": func.greatest(TopicSignal.strength, signal.strength),
                    "first_seen_at": func.least(
                        TopicSignal.first_seen_at, min(post.first_seen_at, assigned_at)
                    ),
                },
            )
        )

    evidence: dict[str, Any] = {
        "assignment_version": TOPIC_ASSIGNMENT_VERSION,
        "assigned_at": assigned_at,
        "post_id": post_id,
        "classification": {
            "topic_key": topic_key,
            "niche_key": niche_key,
            "matched_rules": matched_rules,
        },
        "signals": [
            {
                "kind": signal.kind,
                "value": signal.value,
                "strength": signal.strength,
                "provider": signal.provider,
                "details": signal.details,
            }
            for signal in ordered
        ],
        "selected_cluster_key": winner.canonical_key,
        "merged_cluster_ids": [str(item) for item in merged_ids],
    }
    safe_evidence = json_safe(evidence)
    existing_member = await session.scalar(
        select(TopicMember)
        .where(TopicMember.cluster_id == winner.id, TopicMember.post_id == post_id)
        .with_for_update()
    )
    if existing_member is None:
        session.add(
            TopicMember(
                cluster_id=winner.id,
                post_id=post_id,
                evidence=safe_evidence,
                assigned_at=assigned_at,
            )
        )
    else:
        previous_assigned = existing_member.assigned_at
        safe_evidence["first_assigned_at"] = previous_assigned
        existing_member.evidence = safe_evidence
        existing_member.assigned_at = min(previous_assigned, assigned_at)
    await session.flush()
    return TopicAssignment(
        cluster_id=winner.id,
        canonical_key=winner.canonical_key,
        topic_key=winner.topic_key,
        niche_key=winner.niche_key,
        merged_cluster_ids=merged_ids,
        evidence=safe_evidence,
    )


async def _baseline_unique_authors(
    session: AsyncSession,
    cluster: TopicCluster,
    *,
    window_minutes: int,
    observed_at: datetime,
) -> tuple[Decimal | None, str, int]:
    earliest = observed_at - timedelta(days=BASELINE_LOOKBACK_DAYS)

    async def values(scope: str) -> list[Decimal]:
        statement = (
            select(TopicSnapshot.unique_authors)
            .join(TopicCluster, TopicCluster.id == TopicSnapshot.cluster_id)
            .where(
                TopicSnapshot.window_minutes == window_minutes,
                TopicSnapshot.observed_at < observed_at,
                TopicSnapshot.observed_at >= earliest,
            )
            .order_by(TopicSnapshot.observed_at.desc(), TopicSnapshot.id.desc())
            .limit(_MAX_BASELINE_SNAPSHOTS)
        )
        if scope == "cluster":
            statement = statement.where(TopicSnapshot.cluster_id == cluster.id)
        elif scope == "niche":
            statement = statement.where(TopicCluster.niche_key == cluster.niche_key)
        rows = list((await session.scalars(statement)).all())
        return [Decimal(item) for item in rows]

    cluster_values = await values("cluster")
    if len(cluster_values) >= 3:
        return median(cluster_values), "cluster", len(cluster_values)
    niche_values = await values("niche")
    if niche_values:
        return median(niche_values), "niche", len(niche_values)
    global_values = await values("global")
    if global_values:
        return median(global_values), "global", len(global_values)
    return None, "unavailable", 0


async def create_topic_snapshot(
    session: AsyncSession,
    cluster_id: UUID,
    *,
    observed_at: datetime,
    window_minutes: int = DEFAULT_TOPIC_WINDOW_MINUTES,
) -> TopicSnapshotResult:
    """Persist one immutable rolling topic observation using posts visible by as_of."""

    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    if not 1 <= window_minutes <= 10_080:
        raise ValueError("window_minutes must be between 1 and 10080")
    cluster = await _root_cluster(session, cluster_id, lock=True)
    window_start = observed_at - timedelta(minutes=window_minutes)
    rows = (
        await session.execute(
            select(Post, Author)
            .join(TopicMember, TopicMember.post_id == Post.id)
            .outerjoin(Author, Author.id == Post.author_id)
            .where(
                TopicMember.cluster_id == cluster.id,
                TopicMember.assigned_at <= observed_at,
                Post.posted_at > window_start,
                Post.posted_at <= observed_at,
            )
            .order_by(Post.posted_at, Post.id)
        )
    ).all()
    posts = [row[0] for row in rows]
    authors_by_id = {row[0].author_id: row[1] for row in rows if row[1] is not None}
    post_count = len(posts)
    author_counts = Counter(post.author_id for post in posts)
    unique_authors = len(author_counts)
    original_posts = sum(
        post.reply_to_post_id is None
        and post.quoted_post_id is None
        and post.reposted_post_id is None
        for post in posts
    )
    reposts = sum(post.reposted_post_id is not None for post in posts)
    quotes = sum(post.quoted_post_id is not None for post in posts)
    replies = sum(post.reply_to_post_id is not None for post in posts)
    unique_texts = len({post.normalized_text_hash for post in posts})
    ratio_quantum = Decimal("0.00000001")
    text_diversity = (
        Decimal(unique_texts) / Decimal(post_count) if post_count else Decimal("0")
    ).quantize(ratio_quantum, rounding=ROUND_HALF_UP)
    largest_concentration = (
        Decimal(max(author_counts.values())) / Decimal(post_count) if post_count else Decimal("0")
    ).quantize(ratio_quantum, rounding=ROUND_HALF_UP)
    new_authors = 0
    known_author_ages = 0
    for author_id in author_counts:
        author = authors_by_id.get(author_id)
        if author is None or author.account_created_at is None:
            continue
        known_author_ages += 1
        if author.account_created_at >= observed_at - timedelta(days=NEW_ACCOUNT_DAYS):
            new_authors += 1
    new_author_ratio = (
        Decimal(new_authors) / Decimal(unique_authors) if unique_authors else Decimal("0")
    ).quantize(ratio_quantum, rounding=ROUND_HALF_UP)

    expected, baseline_scope, baseline_count = await _baseline_unique_authors(
        session,
        cluster,
        window_minutes=window_minutes,
        observed_at=observed_at,
    )
    baseline_quantum = Decimal("0.000001")
    if expected is not None:
        expected = expected.quantize(baseline_quantum, rounding=ROUND_HALF_UP)
    topic_spike = (
        (Decimal(unique_authors) / expected).quantize(baseline_quantum, rounding=ROUND_HALF_UP)
        if expected is not None and expected > Decimal("0")
        else None
    )
    prior_snapshots = list(
        (
            await session.scalars(
                select(TopicSnapshot)
                .where(
                    TopicSnapshot.cluster_id == cluster.id,
                    TopicSnapshot.window_minutes == window_minutes,
                    TopicSnapshot.observed_at < observed_at,
                )
                .order_by(TopicSnapshot.observed_at.desc(), TopicSnapshot.id.desc())
                .limit(2)
            )
        ).all()
    )
    observations = [
        MetricObservation(
            observed_at=item.observed_at,
            engagement=Decimal(item.unique_authors),
            impressions=None,
        )
        for item in reversed(prior_snapshots)
    ]
    observations.append(
        MetricObservation(
            observed_at=observed_at,
            engagement=Decimal(unique_authors),
            impressions=None,
        )
    )
    intervals = actual_interval_features(observations)
    previous = prior_snapshots[0] if prior_snapshots else None
    unique_author_growth = (
        Decimal(unique_authors - previous.unique_authors) / Decimal(max(previous.unique_authors, 1))
        if previous is not None
        else None
    )
    burst_decay = Decimal("0")
    if intervals.velocity is not None and intervals.velocity < Decimal("0"):
        burst_decay = clamp(abs(intervals.velocity) * Decimal(window_minutes * 60))
    independent_ratio = (
        Decimal(original_posts) / Decimal(post_count) if post_count else Decimal("0")
    )
    repost_ratio = Decimal(reposts) / Decimal(post_count) if post_count else Decimal("0")
    features: dict[str, Any] = {
        "feature_version": TOPIC_SNAPSHOT_VERSION,
        "window_start": window_start,
        "window_end": observed_at,
        "unique_original_texts": len(
            {
                post.normalized_text_hash
                for post in posts
                if post.reply_to_post_id is None
                and post.quoted_post_id is None
                and post.reposted_post_id is None
            }
        ),
        "independent_original_ratio": independent_ratio,
        "repost_ratio": repost_ratio,
        "quote_ratio": Decimal(quotes) / Decimal(post_count) if post_count else Decimal("0"),
        "reply_ratio": Decimal(replies) / Decimal(post_count) if post_count else Decimal("0"),
        "known_author_age_coverage": (
            Decimal(known_author_ages) / Decimal(unique_authors) if unique_authors else Decimal("0")
        ),
        "new_account_days": NEW_ACCOUNT_DAYS,
        "baseline_scope": baseline_scope,
        "baseline_count": baseline_count,
        "unique_author_velocity": intervals.velocity,
        "unique_author_acceleration": intervals.acceleration,
        "unique_author_growth": unique_author_growth,
        "burst_decay": burst_decay,
        "declining_burst": burst_decay > Decimal("0"),
        "repost_only_burst": original_posts == 0 and repost_ratio >= Decimal("0.8"),
        "top_driving_posts": [
            {
                "post_id": post.id,
                "text": post.text[:160],
                "author_id": post.author_id,
                "posted_at": post.posted_at,
            }
            for post in sorted(posts, key=lambda item: (item.posted_at, item.id), reverse=True)[:3]
        ],
    }
    safe_features = json_safe(features)
    proposed_id = uuid4()
    snapshot_id = await session.scalar(
        pg_insert(TopicSnapshot)
        .values(
            id=proposed_id,
            cluster_id=cluster.id,
            window_minutes=window_minutes,
            observed_at=observed_at,
            post_count=post_count,
            unique_authors=unique_authors,
            original_posts=original_posts,
            reposts=reposts,
            quotes=quotes,
            replies=replies,
            text_diversity=text_diversity,
            largest_author_concentration=largest_concentration,
            new_author_ratio=new_author_ratio,
            expected_unique_authors=expected,
            topic_spike=topic_spike,
            features=safe_features,
        )
        .on_conflict_do_nothing(
            index_elements=[
                TopicSnapshot.cluster_id,
                TopicSnapshot.window_minutes,
                TopicSnapshot.observed_at,
            ]
        )
        .returning(TopicSnapshot.id)
    )
    created = snapshot_id is not None
    if snapshot_id is None:
        existing = await session.scalar(
            select(TopicSnapshot).where(
                TopicSnapshot.cluster_id == cluster.id,
                TopicSnapshot.window_minutes == window_minutes,
                TopicSnapshot.observed_at == observed_at,
            )
        )
        if existing is None:
            raise RuntimeError("topic snapshot conflict without an existing row")
        expected_hash = canonical_input_hash(
            {
                "cluster_id": cluster.id,
                "window_minutes": window_minutes,
                "observed_at": observed_at,
                "post_count": post_count,
                "unique_authors": unique_authors,
                "original_posts": original_posts,
                "reposts": reposts,
                "quotes": quotes,
                "replies": replies,
                "text_diversity": text_diversity,
                "largest_author_concentration": largest_concentration,
                "new_author_ratio": new_author_ratio,
                "expected_unique_authors": expected,
                "topic_spike": topic_spike,
                "features": safe_features,
            }
        )
        existing_hash = canonical_input_hash(
            {
                "cluster_id": existing.cluster_id,
                "window_minutes": existing.window_minutes,
                "observed_at": existing.observed_at,
                "post_count": existing.post_count,
                "unique_authors": existing.unique_authors,
                "original_posts": existing.original_posts,
                "reposts": existing.reposts,
                "quotes": existing.quotes,
                "replies": existing.replies,
                "text_diversity": existing.text_diversity,
                "largest_author_concentration": existing.largest_author_concentration,
                "new_author_ratio": existing.new_author_ratio,
                "expected_unique_authors": existing.expected_unique_authors,
                "topic_spike": existing.topic_spike,
                "features": existing.features,
            }
        )
        if existing_hash != expected_hash:
            raise RuntimeError("immutable topic snapshot replay mismatch")
        snapshot_id = existing.id
        topic_spike = existing.topic_spike
        post_count = existing.post_count
        unique_authors = existing.unique_authors
    return TopicSnapshotResult(
        snapshot_id=snapshot_id,
        cluster_id=cluster.id,
        post_count=post_count,
        unique_authors=unique_authors,
        topic_spike=topic_spike,
        created=created,
    )


async def process_post_topic(
    session: AsyncSession,
    post_id: str,
    *,
    observed_at: datetime,
    window_minutes: int = DEFAULT_TOPIC_WINDOW_MINUTES,
    providers: Sequence[TopicSignalProvider] = (),
) -> TopicProcessingResult:
    assignment = await assign_post_to_topic(
        session, post_id, assigned_at=observed_at, providers=providers
    )
    snapshot = await create_topic_snapshot(
        session,
        assignment.cluster_id,
        observed_at=observed_at,
        window_minutes=window_minutes,
    )
    return TopicProcessingResult(assignment=assignment, snapshot=snapshot)


assign_post = assign_post_to_topic
snapshot_topic = create_topic_snapshot
