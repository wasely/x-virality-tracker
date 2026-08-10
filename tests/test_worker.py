from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

import x_virality.workers.jobs as worker_jobs
from x_virality.config import Settings
from x_virality.db.jobs import ClaimedJob
from x_virality.db.repositories import ClaimedOutbox
from x_virality.db.session import utcnow
from x_virality.providers.discord import DiscordApiError, DiscordRateLimit
from x_virality.workers.jobs import DurableWorker, LeaseLostError, run_with_lease_renewal


class FakeDatabase:
    @asynccontextmanager
    async def session(self):
        yield object()

    @asynccontextmanager
    async def transaction(self):
        yield object()


class FakeDiscord:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, object]] = []

    async def send_message(self, channel_id, payload, **kwargs):
        self.calls.append({"channel_id": channel_id, "payload": payload, **kwargs})
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            id="123456",
            raw={"id": "123456", "channel_id": channel_id},
            rate_limit=DiscordRateLimit(limit=5, remaining=4, reset_after_seconds=1.0),
        )


def settings(**values: object) -> Settings:
    return Settings(
        _env_file=None,
        poll_interval_seconds=0.1,
        worker_concurrency=1,
        outbox_concurrency=1,
        worker_shutdown_grace_seconds=0.01,
        **values,
    )


def claimed_job(kind: str = "snapshot") -> ClaimedJob:
    return ClaimedJob(
        id=uuid4(),
        kind=kind,
        payload={"post_id": "123"},
        attempt_id=1,
        attempt_number=1,
        fencing_token=uuid4(),
        lease_expires_at=utcnow() + timedelta(seconds=30),
    )


def claimed_outbox() -> ClaimedOutbox:
    return ClaimedOutbox(
        id=uuid4(),
        alert_id=uuid4(),
        channel_id="99999",
        nonce="stable-nonce",
        payload={"embeds": []},
        chart_spec=None,
        attempt_number=1,
        fencing_token=uuid4(),
        lease_expires_at=utcnow() + timedelta(seconds=30),
    )


@pytest.mark.asyncio
async def test_lease_guard_renews_and_returns_operation_result() -> None:
    renewed = asyncio.Event()

    async def renew() -> bool:
        renewed.set()
        return True

    async def operation() -> str:
        await renewed.wait()
        return "done"

    result = await run_with_lease_renewal(operation(), renew=renew, interval_seconds=0.001)

    assert result == "done"


@pytest.mark.asyncio
async def test_lease_guard_cancels_operation_after_fence_loss() -> None:
    cancelled = asyncio.Event()

    async def renew() -> bool:
        return False

    async def operation() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with pytest.raises(LeaseLostError):
        await run_with_lease_renewal(operation(), renew=renew, interval_seconds=0.001)

    assert cancelled.is_set()


def test_usage_soft_limit_pauses_recovery_but_not_snapshots() -> None:
    async def handler(_job: ClaimedJob) -> dict[str, object]:
        return {}

    worker = DurableWorker(
        settings(),
        FakeDatabase(),  # type: ignore[arg-type]
        FakeDiscord(),
        {"snapshot": handler, "recovery": handler},
    )
    worker._usage_soft_limited = True

    assert worker._claimable_job_kinds() == {"snapshot"}


@pytest.mark.asyncio
async def test_usage_loop_refreshes_immediately_and_hourly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    calls = 0

    async def refresh() -> bool:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()
        return True

    worker = DurableWorker(
        settings(),
        FakeDatabase(),  # type: ignore[arg-type]
        FakeDiscord(),
        {},
        usage_refresh=refresh,
    )
    monkeypatch.setattr(worker_jobs, "_USAGE_REFRESH_SECONDS", 0.001)

    await worker._usage_loop(stop)

    assert calls == 2
    assert worker.usage_soft_limited


@pytest.mark.asyncio
async def test_outbox_success_records_provider_state_and_completes_fenced_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed: list[dict[str, object]] = []
    observations: list[object] = []

    async def no_gate(*_args, **_kwargs):
        return None

    async def persist_observation(_session, observation, **_kwargs):
        observations.append(observation)

    async def complete(_database, _item, **kwargs):
        completed.append(kwargs)
        return True

    async def renew(*_args, **_kwargs):
        return True

    monkeypatch.setattr(worker_jobs, "active_rate_limit_delay", no_gate)
    monkeypatch.setattr(worker_jobs, "upsert_rate_limit", persist_observation)
    monkeypatch.setattr(worker_jobs, "complete_outbox", complete)
    monkeypatch.setattr(worker_jobs, "renew_outbox", renew)
    discord = FakeDiscord()
    worker = DurableWorker(
        settings(),
        FakeDatabase(),
        discord,
        {},  # type: ignore[arg-type]
    )

    await worker.process_outbox(claimed_outbox())

    assert len(discord.calls) == 1
    assert completed[0]["discord_message_id"] == "123456"
    assert observations[0].endpoint == "/channels/99999/messages"
    assert worker._stats["outbox_sent"] == 1


@pytest.mark.asyncio
async def test_discord_rate_limit_defers_without_permanent_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failures: list[dict[str, object]] = []

    async def no_gate(*_args, **_kwargs):
        return None

    async def persist_observation(*_args, **_kwargs):
        return None

    async def fail(_database, _item, **kwargs):
        failures.append(kwargs)
        return "rate_limited"

    monkeypatch.setattr(worker_jobs, "active_rate_limit_delay", no_gate)
    monkeypatch.setattr(worker_jobs, "upsert_rate_limit", persist_observation)
    monkeypatch.setattr(worker_jobs, "fail_outbox", fail)
    discord = FakeDiscord(
        DiscordApiError(
            "limited",
            status_code=429,
            retry_after_seconds=12,
            rate_limited=True,
        )
    )
    worker = DurableWorker(
        settings(),
        FakeDatabase(),
        discord,
        {},  # type: ignore[arg-type]
    )

    await worker.process_outbox(claimed_outbox())

    assert failures[0]["rate_limited"] is True
    assert failures[0]["permanent"] is False
    assert failures[0]["retry_delay_seconds"] >= 12


@pytest.mark.asyncio
async def test_graceful_shutdown_stops_claiming_and_defers_unfinished_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = claimed_job()
    claimed_once = False
    handler_started = asyncio.Event()
    deferred = asyncio.Event()
    heartbeat_states: list[str] = []

    async def claim(*_args, **_kwargs):
        nonlocal claimed_once
        if claimed_once:
            return []
        claimed_once = True
        return [job]

    async def claim_no_outbox(*_args, **_kwargs):
        return []

    async def handler(_job: ClaimedJob) -> dict[str, object]:
        handler_started.set()
        await asyncio.Event().wait()
        return {}

    async def defer(*_args, **_kwargs):
        deferred.set()
        return True

    async def heartbeat(_session, **kwargs):
        heartbeat_states.append(kwargs["state"])

    async def no_gate(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_jobs, "claim_jobs", claim)
    monkeypatch.setattr(worker_jobs, "claim_outbox", claim_no_outbox)
    monkeypatch.setattr(worker_jobs, "defer_job", defer)
    monkeypatch.setattr(worker_jobs, "upsert_heartbeat", heartbeat)
    monkeypatch.setattr(worker_jobs, "active_rate_limit_delay", no_gate)
    worker = DurableWorker(
        settings(),
        FakeDatabase(),  # type: ignore[arg-type]
        FakeDiscord(),
        {"snapshot": handler},
    )
    stop = asyncio.Event()
    run_task = asyncio.create_task(worker.run(stop))

    await asyncio.wait_for(handler_started.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(run_task, timeout=1)

    assert deferred.is_set()
    assert heartbeat_states[0] == "starting"
    assert "draining" in heartbeat_states
    assert heartbeat_states[-1] == "stopped"
