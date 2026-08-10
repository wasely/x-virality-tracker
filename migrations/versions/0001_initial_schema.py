"""Create the initial x-virality schema.

Revision ID: 0001_initial_schema
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "alerts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("entity_type", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("score_id", sa.Uuid(), nullable=False),
        sa.Column("score", sa.Numeric(precision=8, scale=4), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("policy_hash", sa.String(length=64), nullable=False),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "entity_type IN ('post','topic','system')",
            name=op.f("ck_alerts_entity_type"),
        ),
        sa.CheckConstraint(
            "kind IN ('breakout_post','emerging_conversation','system_failure')",
            name=op.f("ck_alerts_kind"),
        ),
        sa.CheckConstraint(
            "state IN ('queued','sent','failed','suppressed')",
            name=op.f("ck_alerts_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_alerts")),
        sa.UniqueConstraint("dedupe_key", name=op.f("uq_alerts_dedupe_key")),
    )
    op.create_index(
        "ix_alerts_entity_created",
        "alerts",
        ["entity_type", "entity_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "api_rate_state",
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("auth_scope", sa.String(length=64), nullable=False),
        sa.Column("limit_value", sa.BigInteger(), nullable=True),
        sa.Column("remaining", sa.BigInteger(), nullable=True),
        sa.Column("reset_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status", sa.Integer(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("headers", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint(
            "provider", "endpoint", "auth_scope", name=op.f("pk_api_rate_state")
        ),
    )

    op.create_table(
        "authors",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("account_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified", sa.Boolean(), nullable=True),
        sa.Column("followers_count", sa.BigInteger(), nullable=True),
        sa.Column("following_count", sa.BigInteger(), nullable=True),
        sa.Column("post_count", sa.BigInteger(), nullable=True),
        sa.Column("listed_count", sa.BigInteger(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_authors")),
    )

    op.create_table(
        "failed_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("error_class", sa.String(length=128), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('unresolved','replayed','discarded')",
            name=op.f("ck_failed_events_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_failed_events")),
    )
    op.create_index(
        "ix_failed_events_state_received",
        "failed_events",
        ["state", "received_at"],
        unique=False,
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("dedupe_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("fencing_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_error_class", sa.String(length=128), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dead_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('queued','leased','retry','completed','failed','dead')",
            name=op.f("ck_jobs_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
        sa.UniqueConstraint("dedupe_key", name=op.f("uq_jobs_dedupe_key")),
    )
    op.create_index("ix_jobs_claim", "jobs", ["state", "run_at", "priority"], unique=False)
    op.create_index(
        "ix_jobs_lease_expiry",
        "jobs",
        ["state", "lease_expires_at"],
        unique=False,
    )

    op.create_table(
        "raw_x_payloads",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("object_type", sa.String(length=32), nullable=False),
        sa.Column("object_id", sa.String(length=64), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seen_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_x_payloads")),
        sa.UniqueConstraint(
            "source",
            "object_type",
            "object_id",
            "payload_hash",
            name="payload_version",
        ),
    )
    op.create_index(
        "ix_raw_x_payloads_object",
        "raw_x_payloads",
        ["object_type", "object_id", "last_seen_at"],
        unique=False,
    )

    op.create_table(
        "rule_sets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("environment", sa.String(length=32), nullable=False),
        sa.Column("config_hash", sa.String(length=64), nullable=False),
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('validated','applying','applied','failed')",
            name=op.f("ck_rule_sets_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rule_sets")),
        sa.UniqueConstraint("config_hash", name=op.f("uq_rule_sets_config_hash")),
    )

    op.create_table(
        "score_versions",
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("feature_version", sa.String(length=64), nullable=False),
        sa.Column("coefficients", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("implementation_hash", sa.String(length=64), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("version", name=op.f("pk_score_versions")),
    )

    op.create_table(
        "service_leases",
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("owner_id", sa.String(length=255), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_service_leases")),
    )

    op.create_table(
        "stream_state",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("connected", sa.Boolean(), nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_keepalive_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_disconnect_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_disconnect_reason", sa.Text(), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("next_connect_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rules_hash", sa.String(length=64), nullable=True),
        sa.Column("recovery_covered_through", sa.DateTime(timezone=True), nullable=True),
        sa.Column("degraded_reason", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_stream_state")),
    )

    op.create_table(
        "topic_clusters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("canonical_key", sa.String(length=255), nullable=False),
        sa.Column("topic_key", sa.String(length=64), nullable=False),
        sa.Column("niche_key", sa.String(length=64), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_activity_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("merged_into_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('active','merged','closed')",
            name=op.f("ck_topic_clusters_state"),
        ),
        sa.ForeignKeyConstraint(
            ["merged_into_id"],
            ["topic_clusters.id"],
            name=op.f("fk_topic_clusters_merged_into_id_topic_clusters"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_topic_clusters")),
        sa.UniqueConstraint("canonical_key", name=op.f("uq_topic_clusters_canonical_key")),
    )
    op.create_index(
        "ix_topic_clusters_activity",
        "topic_clusters",
        ["state", "last_activity_at"],
        unique=False,
    )

    op.create_table(
        "worker_heartbeats",
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("instance_id", sa.String(length=255), nullable=False),
        sa.Column("build_sha", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("role", "instance_id", name=op.f("pk_worker_heartbeats")),
    )
    op.create_index(
        "ix_worker_heartbeats_role_time",
        "worker_heartbeats",
        ["role", "heartbeat_at"],
        unique=False,
    )

    op.create_table(
        "x_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("local_key", sa.String(length=64), nullable=False),
        sa.Column("remote_rule_id", sa.String(length=32), nullable=True),
        sa.Column("query", sa.String(length=512), nullable=False),
        sa.Column("managed_tag", sa.String(length=255), nullable=False),
        sa.Column("topic_key", sa.String(length=64), nullable=False),
        sa.Column("niche_key", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("config_hash", sa.String(length=64), nullable=False),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_x_rules")),
        sa.UniqueConstraint("local_key", name=op.f("uq_x_rules_local_key")),
        sa.UniqueConstraint("remote_rule_id", name=op.f("uq_x_rules_remote_rule_id")),
    )
    op.create_index("ix_x_rules_enabled", "x_rules", ["enabled", "local_key"], unique=False)

    op.create_table(
        "x_usage_daily",
        sa.Column("usage_date", sa.Date(), nullable=False),
        sa.Column("project_id", sa.String(length=64), nullable=False),
        sa.Column("app_id", sa.String(length=64), nullable=False),
        sa.Column("posts_consumed", sa.BigInteger(), nullable=False),
        sa.Column("project_cap", sa.BigInteger(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint(
            "usage_date", "project_id", "app_id", name=op.f("pk_x_usage_daily")
        ),
    )

    op.create_table(
        "author_baselines",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("author_id", sa.String(length=32), nullable=True),
        sa.Column("niche_key", sa.String(length=64), nullable=False),
        sa.Column("target_age_seconds", sa.Integer(), nullable=False),
        sa.Column("follower_bucket", sa.Integer(), nullable=True),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("median_engagement", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("median_velocity", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("engagement_mad", sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["authors.id"],
            name=op.f("fk_author_baselines_author_id_authors"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_author_baselines")),
        sa.UniqueConstraint(
            "author_id",
            "niche_key",
            "target_age_seconds",
            "follower_bucket",
            "as_of",
            name="baseline_identity",
        ),
    )

    op.create_table(
        "author_metric_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("author_id", sa.String(length=32), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("followers_count", sa.BigInteger(), nullable=True),
        sa.Column("following_count", sa.BigInteger(), nullable=True),
        sa.Column("post_count", sa.BigInteger(), nullable=True),
        sa.Column("listed_count", sa.BigInteger(), nullable=True),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["authors.id"],
            name=op.f("fk_author_metric_snapshots_author_id_authors"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_author_metric_snapshots")),
        sa.UniqueConstraint("author_id", "observed_at", name="author_observed"),
    )
    op.create_index(
        "ix_author_metric_snapshots_author_time",
        "author_metric_snapshots",
        ["author_id", "observed_at"],
        unique=False,
    )

    op.create_table(
        "discord_outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("alert_id", sa.Uuid(), nullable=False),
        sa.Column("channel_id", sa.String(length=32), nullable=False),
        sa.Column("nonce", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("chart_spec", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("fencing_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("discord_message_id", sa.String(length=32), nullable=True),
        sa.Column(
            "provider_response",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dead_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('queued','leased','retry','sent','dead')",
            name=op.f("ck_discord_outbox_state"),
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"],
            ["alerts.id"],
            name=op.f("fk_discord_outbox_alert_id_alerts"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discord_outbox")),
        sa.UniqueConstraint("alert_id", "channel_id", name="alert_destination"),
        sa.UniqueConstraint(
            "discord_message_id",
            name=op.f("uq_discord_outbox_discord_message_id"),
        ),
        sa.UniqueConstraint("nonce", name=op.f("uq_discord_outbox_nonce")),
    )
    op.create_index(
        "ix_discord_outbox_claim",
        "discord_outbox",
        ["state", "run_at"],
        unique=False,
    )

    op.create_table(
        "job_attempts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column("fencing_token", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(length=24), nullable=True),
        sa.Column("error_class", sa.String(length=128), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('completed','retry','failed','dead','rate_limited')",
            name=op.f("ck_job_attempts_outcome"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name=op.f("fk_job_attempts_job_id_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_attempts")),
        sa.UniqueConstraint("job_id", "attempt_number", name="job_attempt_number"),
    )

    op.create_table(
        "job_dead_letters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error_class", sa.String(length=128), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=False),
        sa.Column("dead_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name=op.f("fk_job_dead_letters_job_id_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_dead_letters")),
        sa.UniqueConstraint("job_id", name=op.f("uq_job_dead_letters_job_id")),
    )

    op.create_table(
        "posts",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("author_id", sa.String(length=32), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=True),
        sa.Column("conversation_id", sa.String(length=32), nullable=True),
        sa.Column("reply_to_post_id", sa.String(length=32), nullable=True),
        sa.Column("quoted_post_id", sa.String(length=32), nullable=True),
        sa.Column("reposted_post_id", sa.String(length=32), nullable=True),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("normalized_text_hash", sa.String(length=64), nullable=False),
        sa.Column("link_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("entities", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "context_annotations",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('active','deleted','protected','unavailable')",
            name=op.f("ck_posts_state"),
        ),
        sa.ForeignKeyConstraint(
            ["author_id"],
            ["authors.id"],
            name=op.f("fk_posts_author_id_authors"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_posts")),
    )
    op.create_index(
        "ix_posts_author_posted",
        "posts",
        ["author_id", "posted_at"],
        unique=False,
    )
    op.create_index(
        "ix_posts_conversation",
        "posts",
        ["conversation_id"],
        unique=False,
    )
    op.create_index("ix_posts_posted_at", "posts", ["posted_at"], unique=False)
    op.create_index(
        "ix_posts_text_hash",
        "posts",
        ["normalized_text_hash"],
        unique=False,
    )

    op.create_table(
        "recovery_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("rule_id", sa.Uuid(), nullable=False),
        sa.Column("requested_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recovered_through", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_token", sa.String(length=255), nullable=True),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("pages_processed", sa.Integer(), nullable=False),
        sa.Column("posts_recovered", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "state IN ('queued','running','completed','failed','unrecoverable')",
            name=op.f("ck_recovery_runs_state"),
        ),
        sa.ForeignKeyConstraint(
            ["rule_id"],
            ["x_rules.id"],
            name=op.f("fk_recovery_runs_rule_id_x_rules"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_recovery_runs")),
    )
    op.create_index(
        "ix_recovery_runs_state",
        "recovery_runs",
        ["state", "requested_start"],
        unique=False,
    )

    op.create_table(
        "topic_signals",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cluster_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("value", sa.String(length=512), nullable=False),
        sa.Column("strength", sa.Numeric(precision=8, scale=6), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["topic_clusters.id"],
            name=op.f("fk_topic_signals_cluster_id_topic_clusters"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_topic_signals")),
        sa.UniqueConstraint("kind", "value", name="topic_signal_identity"),
    )
    op.create_index(
        "ix_topic_signals_cluster",
        "topic_signals",
        ["cluster_id"],
        unique=False,
    )

    op.create_table(
        "topic_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cluster_id", sa.Uuid(), nullable=False),
        sa.Column("window_minutes", sa.Integer(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("post_count", sa.Integer(), nullable=False),
        sa.Column("unique_authors", sa.Integer(), nullable=False),
        sa.Column("original_posts", sa.Integer(), nullable=False),
        sa.Column("reposts", sa.Integer(), nullable=False),
        sa.Column("quotes", sa.Integer(), nullable=False),
        sa.Column("replies", sa.Integer(), nullable=False),
        sa.Column("text_diversity", sa.Numeric(precision=12, scale=8), nullable=False),
        sa.Column(
            "largest_author_concentration",
            sa.Numeric(precision=12, scale=8),
            nullable=False,
        ),
        sa.Column("new_author_ratio", sa.Numeric(precision=12, scale=8), nullable=False),
        sa.Column(
            "expected_unique_authors",
            sa.Numeric(precision=18, scale=6),
            nullable=True,
        ),
        sa.Column("topic_spike", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("features", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["topic_clusters.id"],
            name=op.f("fk_topic_snapshots_cluster_id_topic_clusters"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_topic_snapshots")),
        sa.UniqueConstraint(
            "cluster_id",
            "window_minutes",
            "observed_at",
            name="cluster_window_observed",
        ),
    )
    op.create_index(
        "ix_topic_snapshots_cluster_time",
        "topic_snapshots",
        ["cluster_id", "observed_at"],
        unique=False,
    )

    op.create_table(
        "alert_feedback",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("alert_id", sa.Uuid(), nullable=False),
        sa.Column("outbox_id", sa.Uuid(), nullable=False),
        sa.Column("interaction_id", sa.String(length=32), nullable=False),
        sa.Column("discord_user_id", sa.String(length=32), nullable=False),
        sa.Column("vote", sa.String(length=8), nullable=False),
        sa.Column("first_voted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("vote IN ('good','bad')", name=op.f("ck_alert_feedback_vote")),
        sa.ForeignKeyConstraint(
            ["alert_id"],
            ["alerts.id"],
            name=op.f("fk_alert_feedback_alert_id_alerts"),
        ),
        sa.ForeignKeyConstraint(
            ["outbox_id"],
            ["discord_outbox.id"],
            name=op.f("fk_alert_feedback_outbox_id_discord_outbox"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_alert_feedback")),
        sa.UniqueConstraint("alert_id", "discord_user_id", name="alert_user_vote"),
        sa.UniqueConstraint("interaction_id", name=op.f("uq_alert_feedback_interaction_id")),
    )

    op.create_table(
        "post_metric_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("post_id", sa.String(length=32), nullable=False),
        sa.Column("target_age_seconds", sa.Integer(), nullable=False),
        sa.Column("collector_version", sa.String(length=32), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("actual_age_seconds", sa.Integer(), nullable=True),
        sa.Column("late_seconds", sa.Integer(), nullable=True),
        sa.Column("quality", sa.String(length=24), nullable=False),
        sa.Column("likes", sa.BigInteger(), nullable=True),
        sa.Column("reposts", sa.BigInteger(), nullable=True),
        sa.Column("replies", sa.BigInteger(), nullable=True),
        sa.Column("quotes", sa.BigInteger(), nullable=True),
        sa.Column("bookmarks", sa.BigInteger(), nullable=True),
        sa.Column("impressions", sa.BigInteger(), nullable=True),
        sa.Column("author_followers", sa.BigInteger(), nullable=True),
        sa.Column("engagement", sa.Numeric(precision=20, scale=4), nullable=True),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "quality IN ('pending','observed','late','skipped_late','deleted','protected','unavailable','failed')",
            name=op.f("ck_post_metric_snapshots_quality"),
        ),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["posts.id"],
            name=op.f("fk_post_metric_snapshots_post_id_posts"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_post_metric_snapshots")),
        sa.UniqueConstraint(
            "post_id",
            "target_age_seconds",
            "collector_version",
            name="post_horizon_version",
        ),
    )
    op.create_index(
        "ix_post_metric_snapshots_due",
        "post_metric_snapshots",
        ["quality", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "ix_post_metric_snapshots_post_time",
        "post_metric_snapshots",
        ["post_id", "observed_at"],
        unique=False,
    )

    op.create_table(
        "post_rule_matches",
        sa.Column("post_id", sa.String(length=32), nullable=False),
        sa.Column("rule_id", sa.Uuid(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("matched_tag", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["posts.id"],
            name=op.f("fk_post_rule_matches_post_id_posts"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["rule_id"],
            ["x_rules.id"],
            name=op.f("fk_post_rule_matches_rule_id_x_rules"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("post_id", "rule_id", name=op.f("pk_post_rule_matches")),
    )

    op.create_table(
        "topic_members",
        sa.Column("cluster_id", sa.Uuid(), nullable=False),
        sa.Column("post_id", sa.String(length=32), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["topic_clusters.id"],
            name=op.f("fk_topic_members_cluster_id_topic_clusters"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["posts.id"],
            name=op.f("fk_topic_members_post_id_posts"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("cluster_id", "post_id", name=op.f("pk_topic_members")),
    )
    op.create_index("ix_topic_members_post", "topic_members", ["post_id"], unique=False)

    op.create_table(
        "topic_scores",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cluster_id", sa.Uuid(), nullable=False),
        sa.Column("topic_snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("score_version", sa.String(length=64), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("score", sa.Numeric(precision=8, scale=4), nullable=False),
        sa.Column("components", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cohort_size", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Numeric(precision=8, scale=6), nullable=False),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["topic_clusters.id"],
            name=op.f("fk_topic_scores_cluster_id_topic_clusters"),
        ),
        sa.ForeignKeyConstraint(
            ["score_version"],
            ["score_versions.version"],
            name=op.f("fk_topic_scores_score_version_score_versions"),
        ),
        sa.ForeignKeyConstraint(
            ["topic_snapshot_id"],
            ["topic_snapshots.id"],
            name=op.f("fk_topic_scores_topic_snapshot_id_topic_snapshots"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_topic_scores")),
        sa.UniqueConstraint(
            "cluster_id",
            "topic_snapshot_id",
            "score_version",
            name="topic_score_version",
        ),
        sa.UniqueConstraint("input_hash", name=op.f("uq_topic_scores_input_hash")),
    )
    op.create_index(
        "ix_topic_scores_score_time",
        "topic_scores",
        ["score", "as_of"],
        unique=False,
    )

    op.create_table(
        "post_scores",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("post_id", sa.String(length=32), nullable=False),
        sa.Column("cluster_id", sa.Uuid(), nullable=True),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("score_version", sa.String(length=64), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("score", sa.Numeric(precision=8, scale=4), nullable=False),
        sa.Column("components", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cohort_size", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Numeric(precision=8, scale=6), nullable=False),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["topic_clusters.id"],
            name=op.f("fk_post_scores_cluster_id_topic_clusters"),
        ),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["posts.id"],
            name=op.f("fk_post_scores_post_id_posts"),
        ),
        sa.ForeignKeyConstraint(
            ["score_version"],
            ["score_versions.version"],
            name=op.f("fk_post_scores_score_version_score_versions"),
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["post_metric_snapshots.id"],
            name=op.f("fk_post_scores_snapshot_id_post_metric_snapshots"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_post_scores")),
        sa.UniqueConstraint("input_hash", name=op.f("uq_post_scores_input_hash")),
        sa.UniqueConstraint("post_id", "snapshot_id", "score_version", name="post_score_version"),
    )
    op.create_index(
        "ix_post_scores_score_time",
        "post_scores",
        ["score", "as_of"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_post_scores_score_time", table_name="post_scores")
    op.drop_table("post_scores")
    op.drop_index("ix_topic_scores_score_time", table_name="topic_scores")
    op.drop_table("topic_scores")
    op.drop_index("ix_topic_members_post", table_name="topic_members")
    op.drop_table("topic_members")
    op.drop_table("post_rule_matches")
    op.drop_index("ix_post_metric_snapshots_post_time", table_name="post_metric_snapshots")
    op.drop_index("ix_post_metric_snapshots_due", table_name="post_metric_snapshots")
    op.drop_table("post_metric_snapshots")
    op.drop_table("alert_feedback")
    op.drop_index("ix_topic_snapshots_cluster_time", table_name="topic_snapshots")
    op.drop_table("topic_snapshots")
    op.drop_index("ix_topic_signals_cluster", table_name="topic_signals")
    op.drop_table("topic_signals")
    op.drop_index("ix_recovery_runs_state", table_name="recovery_runs")
    op.drop_table("recovery_runs")
    op.drop_index("ix_posts_text_hash", table_name="posts")
    op.drop_index("ix_posts_posted_at", table_name="posts")
    op.drop_index("ix_posts_conversation", table_name="posts")
    op.drop_index("ix_posts_author_posted", table_name="posts")
    op.drop_table("posts")
    op.drop_table("job_dead_letters")
    op.drop_table("job_attempts")
    op.drop_index("ix_discord_outbox_claim", table_name="discord_outbox")
    op.drop_table("discord_outbox")
    op.drop_index(
        "ix_author_metric_snapshots_author_time",
        table_name="author_metric_snapshots",
    )
    op.drop_table("author_metric_snapshots")
    op.drop_table("author_baselines")
    op.drop_table("x_usage_daily")
    op.drop_index("ix_x_rules_enabled", table_name="x_rules")
    op.drop_table("x_rules")
    op.drop_index("ix_worker_heartbeats_role_time", table_name="worker_heartbeats")
    op.drop_table("worker_heartbeats")
    op.drop_index("ix_topic_clusters_activity", table_name="topic_clusters")
    op.drop_table("topic_clusters")
    op.drop_table("stream_state")
    op.drop_table("service_leases")
    op.drop_table("score_versions")
    op.drop_table("rule_sets")
    op.drop_index("ix_raw_x_payloads_object", table_name="raw_x_payloads")
    op.drop_table("raw_x_payloads")
    op.drop_index("ix_jobs_lease_expiry", table_name="jobs")
    op.drop_index("ix_jobs_claim", table_name="jobs")
    op.drop_table("jobs")
    op.drop_index("ix_failed_events_state_received", table_name="failed_events")
    op.drop_table("failed_events")
    op.drop_table("authors")
    op.drop_table("api_rate_state")
    op.drop_index("ix_alerts_entity_created", table_name="alerts")
    op.drop_table("alerts")
