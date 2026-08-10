from __future__ import annotations

import hashlib
import json
import re
import socket
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Role = Literal["api", "backfill", "healthcheck", "migrate", "stream", "sync-rules", "worker"]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="XV_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = "development"
    database_url: str = "postgresql+psycopg://x_virality:x_virality@localhost:5432/x_virality"
    build_sha: str = "development"
    log_level: str = "INFO"
    worker_id: str = Field(default_factory=socket.gethostname)

    x_bearer_token: SecretStr | None = None
    x_api_base_url: str = "https://api.x.com"
    rules_path: Path = _PROJECT_ROOT / "config" / "rules.example.yml"
    managed_rule_prefix: str = "xv"
    enterprise_backfill_enabled: bool = False
    recovery_bootstrap_minutes: int = Field(default=15, ge=0, le=10_080)
    recovery_overlap_seconds: int = Field(default=30, ge=0, le=300)
    usage_soft_limit_percent: int = Field(default=80, ge=1, le=99)

    discord_bot_token: SecretStr | None = None
    discord_application_id: str | None = None
    discord_public_key: SecretStr | None = None
    discord_alert_channel_id: str | None = None
    discord_component_secret: SecretStr | None = None
    admin_token: SecretStr | None = None

    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8080, ge=1, le=65_535)
    http_timeout_seconds: float = Field(default=20.0, gt=0, le=120)
    stream_stall_seconds: float = Field(default=90.0, ge=20, le=600)
    stream_lease_seconds: int = Field(default=45, ge=15, le=600)
    job_lease_seconds: int = Field(default=120, ge=15, le=3600)
    worker_concurrency: int = Field(default=8, ge=1, le=100)
    outbox_concurrency: int = Field(default=2, ge=1, le=100)
    worker_retry_base_seconds: float = Field(default=2.0, gt=0, le=300)
    worker_retry_max_seconds: float = Field(default=300.0, gt=0, le=86_400)
    worker_shutdown_grace_seconds: float = Field(default=30.0, ge=0, le=600)
    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=60)
    heartbeat_interval_seconds: int = Field(default=15, ge=5, le=120)
    health_stale_seconds: int = Field(default=90, ge=15, le=900)
    retention_days: int = Field(default=30, ge=1, le=3650)

    @field_validator("database_url")
    @classmethod
    def require_postgresql(cls, value: str) -> str:
        if not value.startswith(("postgresql+psycopg://", "postgresql://")):
            raise ValueError("XV_DATABASE_URL must use PostgreSQL with psycopg")
        return value

    @field_validator("managed_rule_prefix", "environment")
    @classmethod
    def safe_identifier(cls, value: str) -> str:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", value):
            raise ValueError("must contain only letters, digits, underscores, or hyphens")
        return value

    @field_validator("x_api_base_url")
    @classmethod
    def normalize_url(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError("XV_X_API_BASE_URL must use HTTPS")
        return value.rstrip("/")

    @field_validator("discord_application_id", "discord_alert_channel_id")
    @classmethod
    def discord_snowflake(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[0-9]{5,32}", value):
            raise ValueError("Discord IDs must contain 5-32 digits")
        return value

    @field_validator("discord_public_key")
    @classmethod
    def discord_verify_key(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not re.fullmatch(r"[0-9a-fA-F]{64}", value.get_secret_value()):
            raise ValueError("XV_DISCORD_PUBLIC_KEY must be a 64-character hexadecimal key")
        return value

    @field_validator("discord_component_secret", "admin_token")
    @classmethod
    def long_secret(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and len(value.get_secret_value()) < 32:
            raise ValueError("secret must contain at least 32 characters")
        return value

    @model_validator(mode="after")
    def valid_worker_retry_range(self) -> Settings:
        if self.worker_retry_max_seconds < self.worker_retry_base_seconds:
            raise ValueError(
                "XV_WORKER_RETRY_MAX_SECONDS must be greater than or equal to "
                "XV_WORKER_RETRY_BASE_SECONDS"
            )
        return self

    def validate_role(self, role: Role) -> None:
        required: dict[Role, tuple[str, ...]] = {
            "api": (
                "discord_application_id",
                "discord_public_key",
                "discord_component_secret",
                "admin_token",
            ),
            "backfill": ("x_bearer_token",),
            "healthcheck": (),
            "migrate": (),
            "stream": ("x_bearer_token",),
            "sync-rules": ("x_bearer_token",),
            "worker": (
                "x_bearer_token",
                "discord_bot_token",
                "discord_application_id",
                "discord_alert_channel_id",
                "discord_component_secret",
            ),
        }

        def is_missing(name: str) -> bool:
            value: object = getattr(self, name)
            if value is None:
                return True
            if isinstance(value, SecretStr):
                return not value.get_secret_value()
            return value == ""

        missing = [name for name in required[role] if is_missing(name)]
        if role in {"backfill", "stream", "sync-rules", "worker"} and not self.rules_path.is_file():
            missing.append("rules_path")
        if missing:
            formatted = ", ".join(f"XV_{name.upper()}" for name in missing)
            raise ValueError(f"missing configuration for {role}: {formatted}")


class EngagementWeights(BaseModel):
    likes: Decimal = Decimal("1")
    reposts: Decimal = Decimal("2")
    quotes: Decimal = Decimal("3")
    replies: Decimal = Decimal("1.5")
    bookmarks: Decimal = Decimal("0.25")


class PostScoreWeights(BaseModel):
    velocity_percentile: Decimal = Decimal("0.30")
    acceleration_percentile: Decimal = Decimal("0.15")
    author_lift_percentile: Decimal = Decimal("0.20")
    topic_spike_percentile: Decimal = Decimal("0.20")
    conversation_spread_percentile: Decimal = Decimal("0.10")
    impression_velocity_percentile: Decimal = Decimal("0.05")

    @model_validator(mode="after")
    def weights_sum_to_one(self) -> PostScoreWeights:
        if sum(self.model_dump().values(), Decimal("0")) != Decimal("1"):
            raise ValueError("post score weights must sum to 1")
        return self


class ScoringConfig(BaseModel):
    version: str = "rules-v1"
    engagement: EngagementWeights = Field(default_factory=EngagementWeights)
    post_weights: PostScoreWeights = Field(default_factory=PostScoreWeights)


class BreakoutPolicy(BaseModel):
    score_min: Decimal = Decimal("85")
    velocity_percentile_min: Decimal = Decimal("90")
    author_lift_min: Decimal = Decimal("2")


class EmergingPolicy(BaseModel):
    score_min: Decimal = Decimal("82")
    unique_authors_min: int = Field(default=6, ge=2)
    topic_spike_min: Decimal = Decimal("3")


class AlertPolicy(BaseModel):
    min_cohort_size: int = Field(default=30, ge=1)
    cooldown_minutes: int = Field(default=180, ge=0)
    breakout: BreakoutPolicy = Field(default_factory=BreakoutPolicy)
    emerging_conversation: EmergingPolicy = Field(default_factory=EmergingPolicy)


class TopicConfig(BaseModel):
    key: str
    niche: str
    label: str

    @field_validator("key", "niche")
    @classmethod
    def stable_key(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", value):
            raise ValueError("must be a lowercase stable key")
        return value


class RuleConfig(BaseModel):
    key: str
    topic: str
    niche: str
    enabled: bool = True
    query: str

    @field_validator("key", "topic", "niche")
    @classmethod
    def stable_key(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", value):
            raise ValueError("must be a lowercase stable key")
        return value

    @field_validator("query")
    @classmethod
    def bounded_query(cls, value: str) -> str:
        query = value.strip()
        if not query or len(query) > 512:
            raise ValueError("query must contain 1-512 characters")
        positive = re.sub(r"(?:^|\s)-?\w+:[^\s)]+", " ", query)
        has_positive_operator = bool(re.search(r"(?<!-)\b(?:from|to|url|domain):", query))
        if not has_positive_operator and not re.search(r"[#@$\w]{2,}", positive, re.UNICODE):
            raise ValueError("query requires a positive term, phrase, account, or entity")
        return query


class RulesConfig(BaseModel):
    version: int = Field(ge=1)
    environment: str
    alert_policy: AlertPolicy = Field(default_factory=AlertPolicy)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    topics: list[TopicConfig]
    rules: list[RuleConfig]

    @model_validator(mode="after")
    def validate_graph(self) -> RulesConfig:
        topic_by_key = {topic.key: topic for topic in self.topics}
        if len(topic_by_key) != len(self.topics):
            raise ValueError("topic keys must be unique")
        rule_keys = {rule.key for rule in self.rules}
        if len(rule_keys) != len(self.rules):
            raise ValueError("rule keys must be unique")
        if sum(rule.enabled for rule in self.rules) > 25:
            raise ValueError("at most 25 managed rules may be enabled")
        for rule in self.rules:
            topic = topic_by_key.get(rule.topic)
            if topic is None:
                raise ValueError(f"rule {rule.key} references unknown topic {rule.topic}")
            if topic.niche != rule.niche:
                raise ValueError(f"rule {rule.key} niche differs from its topic")
        return self

    def canonical_hash(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def managed_tag(self, prefix: str, rule: RuleConfig) -> str:
        return f"{prefix}:{self.environment}:{rule.key}:{self.canonical_hash()[:8]}"


def load_rules(path: Path) -> RulesConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"cannot load rule configuration {path}: {error}") from error
    if not isinstance(raw, dict):
        raise ValueError(f"rule configuration {path} must be a YAML object")
    return RulesConfig.model_validate(raw)
