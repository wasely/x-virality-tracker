from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import RuleConfig, RulesConfig
from x_virality.db.models import (
    Alert,
    ApiRateState,
    DiscordOutbox,
    FailedEvent,
    Job,
    RawXPayload,
    RuleSet,
    StreamState,
    XRule,
    XUsageDaily,
)
from x_virality.db.session import Database, utcnow
from x_virality.domain.models import RateLimitObservation


def canonical_json_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def record_rule_set(
    session: AsyncSession, rules: RulesConfig, *, state: str = "validated"
) -> UUID:
    config_hash = rules.canonical_hash()
    statement = (
        pg_insert(RuleSet)
        .values(
            id=uuid4(),
            version=rules.version,
            environment=rules.environment,
            config_hash=config_hash,
            config=rules.model_dump(mode="json"),
            state=state,
        )
        .on_conflict_do_update(
            index_elements=[RuleSet.config_hash],
            set_={"state": state, "validation_error": None},
        )
        .returning(RuleSet.id)
    )
    rule_set_id = await session.scalar(statement)
    if rule_set_id is None:
        raise RuntimeError("failed to store rule set")
    return rule_set_id


async def upsert_rule(
    session: AsyncSession,
    *,
    rule: RuleConfig,
    managed_tag: str,
    config_hash: str,
    remote_rule_id: str | None,
    error: str | None = None,
) -> UUID:
    now = utcnow()
    statement = (
        pg_insert(XRule)
        .values(
            id=uuid4(),
            local_key=rule.key,
            remote_rule_id=remote_rule_id,
            query=rule.query,
            managed_tag=managed_tag,
            topic_key=rule.topic,
            niche_key=rule.niche,
            enabled=rule.enabled,
            config_hash=config_hash,
            last_synced_at=now if remote_rule_id else None,
            last_error=error,
            created_at=now,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=[XRule.local_key],
            set_={
                "remote_rule_id": remote_rule_id,
                "query": rule.query,
                "managed_tag": managed_tag,
                "topic_key": rule.topic,
                "niche_key": rule.niche,
                "enabled": rule.enabled,
                "config_hash": config_hash,
                "last_synced_at": now if remote_rule_id else None,
                "last_error": error,
                "updated_at": now,
            },
        )
        .returning(XRule.id)
    )
    local_id = await session.scalar(statement)
    if local_id is None:
        raise RuntimeError(f"failed to store X rule {rule.key}")
    return local_id


async def rules_by_remote_id(session: AsyncSession) -> dict[str, XRule]:
    rows = await session.scalars(select(XRule).where(XRule.remote_rule_id.is_not(None)))
    return {str(rule.remote_rule_id): rule for rule in rows if rule.remote_rule_id}


async def store_raw_payload(
    session: AsyncSession,
    *,
    source: str,
    object_type: str,
    object_id: str,
    payload: dict[str, Any],
    seen_at: Any,
) -> UUID:
    payload_hash = canonical_json_hash(payload)
    statement = (
        pg_insert(RawXPayload)
        .values(
            id=uuid4(),
            source=source,
            object_type=object_type,
            object_id=object_id,
            payload_hash=payload_hash,
            payload=payload,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
            seen_count=1,
        )
        .on_conflict_do_update(
            index_elements=[
                RawXPayload.source,
                RawXPayload.object_type,
                RawXPayload.object_id,
                RawXPayload.payload_hash,
            ],
            set_={
                "last_seen_at": seen_at,
                "seen_count": RawXPayload.seen_count + 1,
            },
        )
        .returning(RawXPayload.id)
    )
    payload_id = await session.scalar(statement)
    if payload_id is None:
        raise RuntimeError("failed to store raw X payload")
    return payload_id


async def store_failed_event(
    session: AsyncSession,
    *,
    source: str,
    payload: dict[str, Any],
    error: Exception,
) -> UUID:
    failed = FailedEvent(
        source=source,
        received_at=utcnow(),
        payload=payload,
        error_class=type(error).__name__,
        error_detail=str(error)[:4000],
        state="unresolved",
    )
    session.add(failed)
    await session.flush()
    return failed.id


async def upsert_rate_limit(
    session: AsyncSession,
    observation: RateLimitObservation,
    *,
    provider: str = "x",
    auth_scope: str = "app",
    headers: dict[str, Any] | None = None,
) -> None:
    await session.execute(
        pg_insert(ApiRateState)
        .values(
            provider=provider,
            endpoint=observation.endpoint,
            auth_scope=auth_scope,
            limit_value=observation.limit,
            remaining=observation.remaining,
            reset_at=observation.reset_at,
            last_status=observation.status_code,
            observed_at=utcnow(),
            headers=headers or {},
        )
        .on_conflict_do_update(
            index_elements=[
                ApiRateState.provider,
                ApiRateState.endpoint,
                ApiRateState.auth_scope,
            ],
            set_={
                "limit_value": observation.limit,
                "remaining": observation.remaining,
                "reset_at": observation.reset_at,
                "last_status": observation.status_code,
                "observed_at": utcnow(),
                "headers": headers or {},
            },
        )
    )


async def upsert_usage(
    session: AsyncSession,
    *,
    usage_date: Any,
    project_id: str,
    app_id: str,
    posts_consumed: int,
    project_cap: int | None,
    raw: dict[str, Any],
) -> None:
    now = utcnow()
    await session.execute(
        pg_insert(XUsageDaily)
        .values(
            usage_date=usage_date,
            project_id=project_id,
            app_id=app_id,
            posts_consumed=posts_consumed,
            project_cap=project_cap,
            observed_at=now,
            raw=raw,
        )
        .on_conflict_do_update(
            index_elements=[
                XUsageDaily.usage_date,
                XUsageDaily.project_id,
                XUsageDaily.app_id,
            ],
            set_={
                "posts_consumed": posts_consumed,
                "project_cap": project_cap,
                "observed_at": now,
                "raw": raw,
            },
        )
    )


async def update_stream_state(session: AsyncSession, **values: Any) -> None:
    now = utcnow()
    payload = {"id": "primary", "updated_at": now, **values}
    update_values = {"updated_at": now, **values}
    await session.execute(
        pg_insert(StreamState)
        .values(**payload)
        .on_conflict_do_update(index_elements=[StreamState.id], set_=update_values)
    )


@dataclass(frozen=True, slots=True)
class ClaimedOutbox:
    id: UUID
    alert_id: UUID
    channel_id: str
    nonce: str
    payload: dict[str, Any]
    chart_spec: dict[str, Any] | None
    attempt_number: int
    fencing_token: UUID
    lease_expires_at: Any


async def claim_outbox(
    database: Database,
    *,
    worker_id: str,
    lease_seconds: int,
    limit: int,
) -> list[ClaimedOutbox]:
    now = utcnow()
    async with database.transaction() as session:
        ready = or_(
            and_(DiscordOutbox.state.in_(("queued", "retry")), DiscordOutbox.run_at <= now),
            and_(DiscordOutbox.state == "leased", DiscordOutbox.lease_expires_at < now),
        )
        rows = list(
            (
                await session.scalars(
                    select(DiscordOutbox)
                    .where(ready)
                    .order_by(DiscordOutbox.run_at, DiscordOutbox.created_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        claimed: list[ClaimedOutbox] = []
        for row in rows:
            if row.state == "leased":
                row.attempts += 1
                row.last_error = "worker lease expired before Discord delivery completed"
                if row.attempts >= row.max_attempts:
                    row.state = "dead"
                    row.dead_at = now
                    row.lease_owner = None
                    row.fencing_token = None
                    row.lease_expires_at = None
                    row.updated_at = now
                    await session.execute(
                        update(Alert)
                        .where(Alert.id == row.alert_id)
                        .values(state="failed", updated_at=now)
                    )
                    continue
            token = uuid4()
            expires_at = now + timedelta(seconds=lease_seconds)
            row.state = "leased"
            row.lease_owner = worker_id
            row.fencing_token = token
            row.lease_expires_at = expires_at
            row.updated_at = now
            claimed.append(
                ClaimedOutbox(
                    id=row.id,
                    alert_id=row.alert_id,
                    channel_id=row.channel_id,
                    nonce=row.nonce,
                    payload=dict(row.payload),
                    chart_spec=dict(row.chart_spec) if row.chart_spec else None,
                    attempt_number=row.attempts + 1,
                    fencing_token=token,
                    lease_expires_at=expires_at,
                )
            )
        return claimed


async def renew_outbox(
    database: Database,
    item: ClaimedOutbox,
    *,
    worker_id: str,
    lease_seconds: int,
) -> bool:
    """Extend an outbox lease only while its fencing token is current."""

    now = utcnow()
    async with database.transaction() as session:
        result = await session.execute(
            update(DiscordOutbox)
            .where(
                DiscordOutbox.id == item.id,
                DiscordOutbox.state == "leased",
                DiscordOutbox.lease_owner == worker_id,
                DiscordOutbox.fencing_token == item.fencing_token,
            )
            .values(
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
        )
        return bool(getattr(result, "rowcount", 0))


async def complete_outbox(
    database: Database,
    item: ClaimedOutbox,
    *,
    worker_id: str,
    discord_message_id: str,
    provider_response: dict[str, Any],
) -> bool:
    now = utcnow()
    async with database.transaction() as session:
        row = await session.scalar(
            select(DiscordOutbox)
            .where(
                DiscordOutbox.id == item.id,
                DiscordOutbox.state == "leased",
                DiscordOutbox.lease_owner == worker_id,
                DiscordOutbox.fencing_token == item.fencing_token,
            )
            .with_for_update()
        )
        if row is None:
            return False
        row.state = "sent"
        row.discord_message_id = discord_message_id
        row.provider_response = provider_response
        row.sent_at = now
        row.last_error = None
        row.lease_owner = None
        row.fencing_token = None
        row.lease_expires_at = None
        row.updated_at = now
        await session.execute(
            update(Alert).where(Alert.id == row.alert_id).values(state="sent", updated_at=now)
        )
        return True


async def fail_outbox(
    database: Database,
    item: ClaimedOutbox,
    *,
    worker_id: str,
    error: str,
    retry_delay_seconds: float,
    permanent: bool = False,
    rate_limited: bool = False,
) -> str | None:
    now = utcnow()
    async with database.transaction() as session:
        row = await session.scalar(
            select(DiscordOutbox)
            .where(
                DiscordOutbox.id == item.id,
                DiscordOutbox.state == "leased",
                DiscordOutbox.lease_owner == worker_id,
                DiscordOutbox.fencing_token == item.fencing_token,
            )
            .with_for_update()
        )
        if row is None:
            return None
        attempts = row.attempts if rate_limited else row.attempts + 1
        dead = permanent or attempts >= row.max_attempts
        row.attempts = attempts
        row.last_error = error[:4000]
        row.lease_owner = None
        row.fencing_token = None
        row.lease_expires_at = None
        row.updated_at = now
        if dead:
            row.state = "dead"
            row.dead_at = now
            await session.execute(
                update(Alert).where(Alert.id == row.alert_id).values(state="failed", updated_at=now)
            )
            return "dead"
        row.state = "retry"
        row.run_at = now + timedelta(seconds=max(0, retry_delay_seconds))
        return "rate_limited" if rate_limited else "retry"


async def defer_outbox(
    database: Database,
    item: ClaimedOutbox,
    *,
    worker_id: str,
    retry_delay_seconds: float = 0,
    error: str = "worker deferred Discord delivery",
) -> bool:
    """Fenced requeue that preserves the outbox attempt budget."""

    now = utcnow()
    async with database.transaction() as session:
        result = await session.execute(
            update(DiscordOutbox)
            .where(
                DiscordOutbox.id == item.id,
                DiscordOutbox.state == "leased",
                DiscordOutbox.lease_owner == worker_id,
                DiscordOutbox.fencing_token == item.fencing_token,
            )
            .values(
                state="retry",
                run_at=now + timedelta(seconds=max(0, retry_delay_seconds)),
                last_error=error[:4000],
                lease_owner=None,
                fencing_token=None,
                lease_expires_at=None,
                updated_at=now,
            )
        )
        return bool(getattr(result, "rowcount", 0))


async def active_rate_limit_delay(
    session: AsyncSession,
    *,
    provider: str,
    endpoint: str,
    auth_scope: str = "app",
    as_of: Any | None = None,
) -> float | None:
    """Return seconds until a persisted exhausted/429 gate resets, if active."""

    state = await session.get(ApiRateState, (provider, endpoint, auth_scope))
    if state is None or state.reset_at is None:
        return None
    if state.remaining != 0 and state.last_status != 429:
        return None
    delay = (state.reset_at - (as_of or utcnow())).total_seconds()
    return delay if delay > 0 else None


async def count_open_work(session: AsyncSession) -> dict[str, int]:
    now = utcnow()
    due_jobs = int(
        await session.scalar(
            select(func.count())
            .select_from(Job)
            .where(
                Job.state.in_(("queued", "retry")),
                Job.run_at <= now,
            )
        )
        or 0
    )
    dead_outbox = int(
        await session.scalar(
            select(func.count()).select_from(DiscordOutbox).where(DiscordOutbox.state == "dead")
        )
        or 0
    )
    return {"due_jobs": due_jobs, "dead_outbox": dead_outbox}
