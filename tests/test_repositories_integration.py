from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from x_virality.config import load_rules
from x_virality.db.models import Alert, ApiRateState, DiscordOutbox, RawXPayload, XRule
from x_virality.db.repositories import (
    claim_outbox,
    complete_outbox,
    fail_outbox,
    record_rule_set,
    store_raw_payload,
    upsert_rate_limit,
    upsert_rule,
)
from x_virality.db.session import Database, utcnow
from x_virality.domain.models import RateLimitObservation

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_rule_and_raw_payload_upserts_are_idempotent(database: Database) -> None:
    rules = load_rules(Path(__file__).parents[1] / "config" / "rules.example.yml")
    rule = rules.rules[0]
    seen_at = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)

    async with database.transaction() as session:
        first_set = await record_rule_set(session, rules)
        second_set = await record_rule_set(session, rules)
        first_rule = await upsert_rule(
            session,
            rule=rule,
            managed_tag=rules.managed_tag("xv", rule),
            config_hash=rules.canonical_hash(),
            remote_rule_id="remote-1",
        )
        second_rule = await upsert_rule(
            session,
            rule=rule,
            managed_tag=rules.managed_tag("xv", rule),
            config_hash=rules.canonical_hash(),
            remote_rule_id="remote-1",
        )
        first_payload = await store_raw_payload(
            session,
            source="stream",
            object_type="post",
            object_id="123",
            payload={"data": {"id": "123"}},
            seen_at=seen_at,
        )
        second_payload = await store_raw_payload(
            session,
            source="stream",
            object_type="post",
            object_id="123",
            payload={"data": {"id": "123"}},
            seen_at=seen_at + timedelta(seconds=1),
        )

    assert first_set == second_set
    assert first_rule == second_rule
    assert first_payload == second_payload
    async with database.session() as session:
        payload = await session.scalar(select(RawXPayload))
        rule_count = await session.scalar(select(func.count()).select_from(XRule))
    assert payload is not None
    assert payload.seen_count == 2
    assert payload.last_seen_at == seen_at + timedelta(seconds=1)
    assert rule_count == 1


@pytest.mark.asyncio
async def test_rate_limit_state_keeps_latest_observation(database: Database) -> None:
    reset_at = utcnow() + timedelta(minutes=15)
    async with database.transaction() as session:
        await upsert_rate_limit(
            session,
            RateLimitObservation(
                endpoint="/2/tweets",
                status_code=200,
                limit=300,
                remaining=299,
                reset_at=reset_at,
                retry_after_seconds=None,
            ),
        )
        await upsert_rate_limit(
            session,
            RateLimitObservation(
                endpoint="/2/tweets",
                status_code=429,
                limit=300,
                remaining=0,
                reset_at=reset_at,
                retry_after_seconds=30,
            ),
        )

    async with database.session() as session:
        state = await session.get(ApiRateState, ("x", "/2/tweets", "app"))
    assert state is not None
    assert state.last_status == 429
    assert state.remaining == 0


async def add_outbox(database: Database, *, max_attempts: int = 2) -> None:
    now = utcnow()
    alert_id = uuid4()
    async with database.transaction() as session:
        session.add(
            Alert(
                id=alert_id,
                kind="breakout_post",
                entity_type="post",
                entity_id="123",
                score_id=uuid4(),
                score=Decimal("91.25"),
                evidence={},
                policy_hash="a" * 64,
                dedupe_key=f"alert:{alert_id}",
                state="queued",
            )
        )
        session.add(
            DiscordOutbox(
                alert_id=alert_id,
                channel_id="999",
                nonce=f"nonce-{alert_id}",
                payload={"embeds": []},
                state="queued",
                attempts=0,
                max_attempts=max_attempts,
                run_at=now,
            )
        )


@pytest.mark.asyncio
async def test_outbox_completion_is_fenced_and_marks_alert_sent(database: Database) -> None:
    await add_outbox(database)
    (stale,) = await claim_outbox(database, worker_id="worker-a", lease_seconds=30, limit=1)
    async with database.transaction() as session:
        await session.execute(
            update(DiscordOutbox)
            .where(DiscordOutbox.id == stale.id)
            .values(lease_expires_at=utcnow() - timedelta(seconds=1))
        )
    (fresh,) = await claim_outbox(database, worker_id="worker-b", lease_seconds=30, limit=1)

    assert not await complete_outbox(
        database,
        stale,
        worker_id="worker-a",
        discord_message_id="message-stale",
        provider_response={},
    )
    assert await complete_outbox(
        database,
        fresh,
        worker_id="worker-b",
        discord_message_id="message-1",
        provider_response={"id": "message-1"},
    )

    async with database.session() as session:
        outbox = await session.get(DiscordOutbox, fresh.id)
        alert = await session.get(Alert, fresh.alert_id)
    assert outbox is not None and outbox.state == "sent"
    assert alert is not None and alert.state == "sent"


@pytest.mark.asyncio
async def test_outbox_rate_limit_does_not_burn_attempt(database: Database) -> None:
    await add_outbox(database)
    (item,) = await claim_outbox(database, worker_id="worker", lease_seconds=30, limit=1)

    outcome = await fail_outbox(
        database,
        item,
        worker_id="worker",
        error="rate limited",
        retry_delay_seconds=0,
        rate_limited=True,
    )

    assert outcome == "rate_limited"
    async with database.session() as session:
        outbox = await session.get(DiscordOutbox, item.id)
    assert outbox is not None
    assert outbox.attempts == 0
    assert outbox.state == "retry"
