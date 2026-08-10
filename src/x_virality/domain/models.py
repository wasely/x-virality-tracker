from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

SNAPSHOT_HORIZONS_SECONDS: tuple[int, ...] = (120, 300, 600, 1200, 2400, 3600)


@dataclass(frozen=True, slots=True)
class PublicMetrics:
    likes: int = 0
    reposts: int = 0
    replies: int = 0
    quotes: int = 0
    bookmarks: int | None = None
    impressions: int | None = None


@dataclass(frozen=True, slots=True)
class AuthorData:
    id: str
    username: str | None = None
    name: str | None = None
    created_at: datetime | None = None
    verified: bool | None = None
    followers: int | None = None
    following: int | None = None
    posts: int | None = None
    listed: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PostReference:
    type: Literal["replied_to", "quoted", "retweeted"]
    id: str


@dataclass(frozen=True, slots=True)
class PostData:
    id: str
    author_id: str
    text: str
    created_at: datetime
    conversation_id: str | None
    language: str | None
    references: tuple[PostReference, ...]
    entities: dict[str, Any]
    context_annotations: tuple[dict[str, Any], ...]
    metrics: PublicMetrics
    author: AuthorData | None
    matching_rule_ids: tuple[str, ...]
    raw: dict[str, Any]

    def reference_id(self, reference_type: str) -> str | None:
        return next(
            (reference.id for reference in self.references if reference.type == reference_type),
            None,
        )


@dataclass(frozen=True, slots=True)
class RemoteRule:
    id: str
    value: str
    tag: str


@dataclass(frozen=True, slots=True)
class RateLimitObservation:
    endpoint: str
    status_code: int
    limit: int | None
    remaining: int | None
    reset_at: datetime | None
    retry_after_seconds: float | None


@dataclass(frozen=True, slots=True)
class AlertCandidate:
    kind: Literal["breakout_post", "emerging_conversation"]
    entity_id: str
    score_id: str
    score: str
    evidence: dict[str, Any]
    policy_hash: str
    dedupe_key: str
