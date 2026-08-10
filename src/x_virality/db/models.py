from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class RuleSet(Base):
    __tablename__ = "rule_sets"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    environment: Mapped[str] = mapped_column(String(32), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="validated")
    validation_error: Mapped[str | None] = mapped_column(Text)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint("state IN ('validated','applying','applied','failed')", name="state"),
    )


class XRule(Base, TimestampMixin):
    __tablename__ = "x_rules"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    local_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    remote_rule_id: Mapped[str | None] = mapped_column(String(32), unique=True)
    query: Mapped[str] = mapped_column(String(512), nullable=False)
    managed_tag: Mapped[str] = mapped_column(String(255), nullable=False)
    topic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    niche_key: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (Index("ix_x_rules_enabled", "enabled", "local_key"),)


class Author(Base, TimestampMixin):
    __tablename__ = "authors"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    username: Mapped[str | None] = mapped_column(String(255))
    name: Mapped[str | None] = mapped_column(String(255))
    account_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified: Mapped[bool | None] = mapped_column(Boolean)
    followers_count: Mapped[int | None] = mapped_column(BigInteger)
    following_count: Mapped[int | None] = mapped_column(BigInteger)
    post_count: Mapped[int | None] = mapped_column(BigInteger)
    listed_count: Mapped[int | None] = mapped_column(BigInteger)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class AuthorMetricSnapshot(Base):
    __tablename__ = "author_metric_snapshots"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    author_id: Mapped[str] = mapped_column(
        ForeignKey("authors.id", ondelete="CASCADE"), nullable=False
    )
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    followers_count: Mapped[int | None] = mapped_column(BigInteger)
    following_count: Mapped[int | None] = mapped_column(BigInteger)
    post_count: Mapped[int | None] = mapped_column(BigInteger)
    listed_count: Mapped[int | None] = mapped_column(BigInteger)
    raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("author_id", "observed_at", name="author_observed"),
        Index("ix_author_metric_snapshots_author_time", "author_id", "observed_at"),
    )


class Post(Base, TimestampMixin):
    __tablename__ = "posts"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    author_id: Mapped[str] = mapped_column(ForeignKey("authors.id"), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str | None] = mapped_column(String(16))
    conversation_id: Mapped[str | None] = mapped_column(String(32))
    reply_to_post_id: Mapped[str | None] = mapped_column(String(32))
    quoted_post_id: Mapped[str | None] = mapped_column(String(32))
    reposted_post_id: Mapped[str | None] = mapped_column(String(32))
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    normalized_text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    link_fingerprint: Mapped[str | None] = mapped_column(String(64))
    entities: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    context_annotations: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, default=list
    )
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="active")

    __table_args__ = (
        CheckConstraint("state IN ('active','deleted','protected','unavailable')", name="state"),
        Index("ix_posts_posted_at", "posted_at"),
        Index("ix_posts_author_posted", "author_id", "posted_at"),
        Index("ix_posts_conversation", "conversation_id"),
        Index("ix_posts_text_hash", "normalized_text_hash"),
    )


class PostRuleMatch(Base):
    __tablename__ = "post_rule_matches"

    post_id: Mapped[str] = mapped_column(
        ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True
    )
    rule_id: Mapped[UUID] = mapped_column(
        ForeignKey("x_rules.id", ondelete="CASCADE"), primary_key=True
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    matched_tag: Mapped[str | None] = mapped_column(String(255))


class RawXPayload(Base):
    __tablename__ = "raw_x_payloads"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    object_type: Mapped[str] = mapped_column(String(32), nullable=False)
    object_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    __table_args__ = (
        UniqueConstraint(
            "source", "object_type", "object_id", "payload_hash", name="payload_version"
        ),
        Index("ix_raw_x_payloads_object", "object_type", "object_id", "last_seen_at"),
    )


class FailedEvent(Base):
    __tablename__ = "failed_events"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    error_class: Mapped[str] = mapped_column(String(128), nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="unresolved")
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("state IN ('unresolved','replayed','discarded')", name="state"),
        Index("ix_failed_events_state_received", "state", "received_at"),
    )


class PostMetricSnapshot(Base):
    __tablename__ = "post_metric_snapshots"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    post_id: Mapped[str] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), nullable=False)
    target_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    collector_version: Mapped[str] = mapped_column(String(32), nullable=False)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    actual_age_seconds: Mapped[int | None] = mapped_column(Integer)
    late_seconds: Mapped[int | None] = mapped_column(Integer)
    quality: Mapped[str] = mapped_column(String(24), nullable=False)
    likes: Mapped[int | None] = mapped_column(BigInteger)
    reposts: Mapped[int | None] = mapped_column(BigInteger)
    replies: Mapped[int | None] = mapped_column(BigInteger)
    quotes: Mapped[int | None] = mapped_column(BigInteger)
    bookmarks: Mapped[int | None] = mapped_column(BigInteger)
    impressions: Mapped[int | None] = mapped_column(BigInteger)
    author_followers: Mapped[int | None] = mapped_column(BigInteger)
    engagement: Mapped[Decimal | None] = mapped_column(Numeric(20, 4))
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        UniqueConstraint(
            "post_id", "target_age_seconds", "collector_version", name="post_horizon_version"
        ),
        CheckConstraint(
            "quality IN ('pending','observed','late','skipped_late','deleted','protected','unavailable','failed')",
            name="quality",
        ),
        Index("ix_post_metric_snapshots_due", "quality", "scheduled_for"),
        Index("ix_post_metric_snapshots_post_time", "post_id", "observed_at"),
    )


class ServiceLease(Base):
    __tablename__ = "service_leases"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(255), nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RecoveryRun(Base, TimestampMixin):
    __tablename__ = "recovery_runs"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    rule_id: Mapped[UUID] = mapped_column(ForeignKey("x_rules.id"), nullable=False)
    requested_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    requested_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recovered_through: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_token: Mapped[str | None] = mapped_column(String(255))
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")
    pages_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    posts_recovered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint(
            "state IN ('queued','running','completed','failed','unrecoverable')", name="state"
        ),
        Index("ix_recovery_runs_state", "state", "requested_start"),
    )


class Job(Base, TimestampMixin):
    __tablename__ = "jobs"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=6)
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    fencing_token: Mapped[UUID | None] = mapped_column(Uuid)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error_class: Mapped[str | None] = mapped_column(String(128))
    last_error: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "state IN ('queued','leased','retry','completed','failed','dead')", name="state"
        ),
        Index("ix_jobs_claim", "state", "run_at", "priority"),
        Index("ix_jobs_lease_expiry", "state", "lease_expires_at"),
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    fencing_token: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(24))
    error_class: Mapped[str | None] = mapped_column(String(128))
    error_detail: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="job_attempt_number"),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('completed','retry','failed','dead','rate_limited')",
            name="outcome",
        ),
    )


class JobDeadLetter(Base):
    __tablename__ = "job_dead_letters"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    error_class: Mapped[str] = mapped_column(String(128), nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    dead_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TopicCluster(Base, TimestampMixin):
    __tablename__ = "topic_clusters"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    canonical_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    topic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    niche_key: Mapped[str] = mapped_column(String(64), nullable=False)
    label: Mapped[str | None] = mapped_column(String(255))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="active")
    merged_into_id: Mapped[UUID | None] = mapped_column(ForeignKey("topic_clusters.id"))

    __table_args__ = (
        CheckConstraint("state IN ('active','merged','closed')", name="state"),
        Index("ix_topic_clusters_activity", "state", "last_activity_at"),
    )


class TopicSignal(Base):
    __tablename__ = "topic_signals"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    cluster_id: Mapped[UUID] = mapped_column(
        ForeignKey("topic_clusters.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    value: Mapped[str] = mapped_column(String(512), nullable=False)
    strength: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("kind", "value", name="topic_signal_identity"),
        Index("ix_topic_signals_cluster", "cluster_id"),
    )


class TopicMember(Base):
    __tablename__ = "topic_members"

    cluster_id: Mapped[UUID] = mapped_column(
        ForeignKey("topic_clusters.id", ondelete="CASCADE"), primary_key=True
    )
    post_id: Mapped[str] = mapped_column(
        ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True
    )
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_topic_members_post", "post_id"),)


class TopicSnapshot(Base):
    __tablename__ = "topic_snapshots"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    cluster_id: Mapped[UUID] = mapped_column(
        ForeignKey("topic_clusters.id", ondelete="CASCADE"), nullable=False
    )
    window_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    post_count: Mapped[int] = mapped_column(Integer, nullable=False)
    unique_authors: Mapped[int] = mapped_column(Integer, nullable=False)
    original_posts: Mapped[int] = mapped_column(Integer, nullable=False)
    reposts: Mapped[int] = mapped_column(Integer, nullable=False)
    quotes: Mapped[int] = mapped_column(Integer, nullable=False)
    replies: Mapped[int] = mapped_column(Integer, nullable=False)
    text_diversity: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    largest_author_concentration: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    new_author_ratio: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    expected_unique_authors: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    topic_spike: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    features: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "cluster_id", "window_minutes", "observed_at", name="cluster_window_observed"
        ),
        Index("ix_topic_snapshots_cluster_time", "cluster_id", "observed_at"),
    )


class AuthorBaseline(Base):
    __tablename__ = "author_baselines"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    author_id: Mapped[str | None] = mapped_column(ForeignKey("authors.id"))
    niche_key: Mapped[str] = mapped_column(String(64), nullable=False)
    target_age_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    follower_bucket: Mapped[int | None] = mapped_column(Integer)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    median_engagement: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    median_velocity: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    engagement_mad: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "author_id",
            "niche_key",
            "target_age_seconds",
            "follower_bucket",
            "as_of",
            name="baseline_identity",
        ),
    )


class ScoreVersion(Base):
    __tablename__ = "score_versions"

    version: Mapped[str] = mapped_column(String(64), primary_key=True)
    feature_version: Mapped[str] = mapped_column(String(64), nullable=False)
    coefficients: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    implementation_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class PostScore(Base):
    __tablename__ = "post_scores"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    post_id: Mapped[str] = mapped_column(ForeignKey("posts.id"), nullable=False)
    cluster_id: Mapped[UUID | None] = mapped_column(ForeignKey("topic_clusters.id"))
    snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("post_metric_snapshots.id"), nullable=False
    )
    score_version: Mapped[str] = mapped_column(ForeignKey("score_versions.version"))
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    components: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    cohort_size: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        UniqueConstraint("post_id", "snapshot_id", "score_version", name="post_score_version"),
        Index("ix_post_scores_score_time", "score", "as_of"),
    )


class TopicScore(Base):
    __tablename__ = "topic_scores"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    cluster_id: Mapped[UUID] = mapped_column(ForeignKey("topic_clusters.id"), nullable=False)
    topic_snapshot_id: Mapped[UUID] = mapped_column(
        ForeignKey("topic_snapshots.id"), nullable=False
    )
    score_version: Mapped[str] = mapped_column(ForeignKey("score_versions.version"))
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    components: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    cohort_size: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        UniqueConstraint(
            "cluster_id", "topic_snapshot_id", "score_version", name="topic_score_version"
        ),
        Index("ix_topic_scores_score_time", "score", "as_of"),
    )


class Alert(Base, TimestampMixin):
    __tablename__ = "alerts"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(16), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    score_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")

    __table_args__ = (
        CheckConstraint(
            "kind IN ('breakout_post','emerging_conversation','system_failure')", name="kind"
        ),
        CheckConstraint("entity_type IN ('post','topic','system')", name="entity_type"),
        CheckConstraint("state IN ('queued','sent','failed','suppressed')", name="state"),
        Index("ix_alerts_entity_created", "entity_type", "entity_id", "created_at"),
    )


class DiscordOutbox(Base, TimestampMixin):
    __tablename__ = "discord_outbox"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    alert_id: Mapped[UUID] = mapped_column(ForeignKey("alerts.id"), nullable=False)
    channel_id: Mapped[str] = mapped_column(String(32), nullable=False)
    nonce: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    chart_spec: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=6)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    fencing_token: Mapped[UUID | None] = mapped_column(Uuid)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    discord_message_id: Mapped[str | None] = mapped_column(String(32), unique=True)
    provider_response: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("alert_id", "channel_id", name="alert_destination"),
        CheckConstraint("state IN ('queued','leased','retry','sent','dead')", name="state"),
        Index("ix_discord_outbox_claim", "state", "run_at"),
    )


class AlertFeedback(Base):
    __tablename__ = "alert_feedback"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    alert_id: Mapped[UUID] = mapped_column(ForeignKey("alerts.id"), nullable=False)
    outbox_id: Mapped[UUID] = mapped_column(ForeignKey("discord_outbox.id"), nullable=False)
    interaction_id: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    discord_user_id: Mapped[str] = mapped_column(String(32), nullable=False)
    vote: Mapped[str] = mapped_column(String(8), nullable=False)
    first_voted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("alert_id", "discord_user_id", name="alert_user_vote"),
        CheckConstraint("vote IN ('good','bad')", name="vote"),
    )


class StreamState(Base):
    __tablename__ = "stream_state"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default="primary")
    connected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    connected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_keepalive_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_disconnect_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_disconnect_reason: Mapped[str | None] = mapped_column(Text)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_connect_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rules_hash: Mapped[str | None] = mapped_column(String(64))
    recovery_covered_through: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    degraded_reason: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"

    role: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    build_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    __table_args__ = (Index("ix_worker_heartbeats_role_time", "role", "heartbeat_at"),)


class ApiRateState(Base):
    __tablename__ = "api_rate_state"

    provider: Mapped[str] = mapped_column(String(32), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(128), primary_key=True)
    auth_scope: Mapped[str] = mapped_column(String(64), primary_key=True, default="app")
    limit_value: Mapped[int | None] = mapped_column(BigInteger)
    remaining: Mapped[int | None] = mapped_column(BigInteger)
    reset_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class XUsageDaily(Base):
    __tablename__ = "x_usage_daily"

    usage_date: Mapped[date] = mapped_column(Date, primary_key=True)
    project_id: Mapped[str] = mapped_column(String(64), primary_key=True, default="default")
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True, default="default")
    posts_consumed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    project_cap: Mapped[int | None] = mapped_column(BigInteger)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
