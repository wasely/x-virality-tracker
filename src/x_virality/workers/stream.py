from __future__ import annotations

import asyncio
import hashlib
import math
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from uuid import uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import RulesConfig, Settings, load_rules
from x_virality.db.jobs import (
    Lease,
    acquire_service_lease,
    release_service_lease,
    renew_service_lease,
    upsert_heartbeat,
)
from x_virality.db.models import ServiceLease, StreamState
from x_virality.db.repositories import update_stream_state
from x_virality.db.session import Database, create_database, utcnow
from x_virality.logging import get_logger
from x_virality.providers.x_api import StreamMessage, XApiClient, XApiError
from x_virality.services.ingestion import (
    STREAM_SOURCE,
    ingest_post_in_transaction,
    record_failed_ingestion,
)
from x_virality.services.recovery import enqueue_disconnect_recovery
from x_virality.services.rules import RuleApi, sync_rules
from x_virality.services.usage import (
    enqueue_usage_refresh_job,
    persist_x_api_error_rate_limit,
)

STREAM_LEASE_NAME = "stream"
MAX_STREAM_BACKOFF_SECONDS = 300.0
logger = get_logger("workers.stream")


class StreamApi(RuleApi, Protocol):
    def stream(self, *, backfill_minutes: int | None = None) -> AsyncIterator[StreamMessage]: ...


class StreamLeaseLost(RuntimeError):
    pass


class StreamStalled(RuntimeError):
    pass


class StreamDisconnected(RuntimeError):
    pass


class StreamStopRequested(RuntimeError):
    pass


@dataclass(slots=True)
class _ConnectionProgress:
    last_activity_at: datetime | None = None
    recovery_enqueued: bool = False


@dataclass(frozen=True, slots=True)
class _StartupState:
    gap_start: datetime
    failure_count: int


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def stream_backoff_seconds(
    failure_count: int,
    *,
    retry_after_seconds: float | None = None,
    cap_seconds: float = MAX_STREAM_BACKOFF_SECONDS,
) -> float:
    """Return deterministic full-jitter-like exponential delay, bounded by cap."""

    if failure_count < 1:
        raise ValueError("failure_count must be at least 1")
    if cap_seconds <= 0:
        raise ValueError("cap_seconds must be positive")
    if retry_after_seconds is not None:
        return max(0.0, retry_after_seconds)
    window = min(cap_seconds, 2.0 ** min(failure_count - 1, 16))
    digest = hashlib.sha256(f"stream:{failure_count}".encode("ascii")).digest()
    fraction = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
    return min(cap_seconds, max(0.25, window * fraction))


def enterprise_backfill_minutes(
    settings: Settings,
    *,
    gap_start: datetime,
    connecting_at: datetime,
) -> int | None:
    if not settings.enterprise_backfill_enabled:
        return None
    start = _aware_utc(gap_start, field="gap_start")
    end = _aware_utc(connecting_at, field="connecting_at")
    seconds = max(0.0, (end - start).total_seconds())
    return min(5, max(0, math.ceil(seconds / 60)))


async def _lease_row(
    session: AsyncSession,
    lease: Lease,
    *,
    lock: bool,
) -> ServiceLease | None:
    statement = select(ServiceLease).where(
        ServiceLease.name == lease.name,
        ServiceLease.owner_id == lease.owner_id,
        ServiceLease.fencing_token == lease.fencing_token,
        ServiceLease.expires_at > utcnow(),
    )
    if lock:
        statement = statement.with_for_update()
    return cast(ServiceLease | None, await session.scalar(statement))


async def _require_lease(session: AsyncSession, lease: Lease) -> None:
    if await _lease_row(session, lease, lock=True) is None:
        raise StreamLeaseLost("stream service lease was lost")


async def _validate_stream_lease(database: Database, lease: Lease) -> None:
    async with database.transaction() as session:
        await _require_lease(session, lease)


async def _update_stream_state_fenced(
    database: Database,
    lease: Lease,
    **values: Any,
) -> None:
    async with database.transaction() as session:
        await _require_lease(session, lease)
        await update_stream_state(session, **values)


async def _heartbeat(
    database: Database,
    settings: Settings,
    *,
    instance_id: str,
    state: str,
    lease: Lease | None,
    details: dict[str, Any] | None = None,
) -> None:
    payload = dict(details or {})
    if lease is not None:
        payload["lease_name"] = lease.name
        payload["fencing_token"] = lease.fencing_token
    async with database.transaction() as session:
        if lease is not None:
            await _require_lease(session, lease)
        await upsert_heartbeat(
            session,
            role="stream",
            instance_id=instance_id,
            build_sha=settings.build_sha,
            state=state[:24],
            details=payload,
        )
        await enqueue_usage_refresh_job(session, at=utcnow())


async def _lease_guard(
    database: Database,
    settings: Settings,
    *,
    lease: Lease,
    instance_id: str,
    stop_event: asyncio.Event,
    lease_lost: asyncio.Event,
) -> None:
    interval = min(
        float(settings.heartbeat_interval_seconds),
        max(1.0, settings.stream_lease_seconds / 3),
    )
    current = lease
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            return
        except TimeoutError:
            pass
        try:
            renewed = await renew_service_lease(
                database,
                current,
                lease_seconds=settings.stream_lease_seconds,
            )
            if renewed is None:
                raise StreamLeaseLost("stream service lease renewal was fenced")
            current = renewed
            await _heartbeat(
                database,
                settings,
                instance_id=instance_id,
                state="running",
                lease=current,
                details={"lease_expires_at": str(current.expires_at)},
            )
        except Exception as error:
            logger.error(
                "stream_lease_guard_failed",
                error_class=type(error).__name__,
                error=str(error),
            )
            lease_lost.set()
            return


async def _cancel_tasks(*tasks: asyncio.Future[Any]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _next_message(
    iterator: AsyncIterator[StreamMessage],
    *,
    stall_seconds: float,
    stop_event: asyncio.Event,
    lease_lost: asyncio.Event,
) -> StreamMessage:
    message_task: asyncio.Future[StreamMessage] = asyncio.ensure_future(anext(iterator))
    stop_task = asyncio.create_task(stop_event.wait())
    lease_task = asyncio.create_task(lease_lost.wait())
    waiters: set[asyncio.Future[Any]] = {message_task, stop_task, lease_task}
    try:
        done, _pending = await asyncio.wait(
            waiters,
            timeout=stall_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if lease_task in done and lease_task.result():
            raise StreamLeaseLost("stream service lease guard lost ownership")
        if stop_task in done and stop_task.result():
            raise StreamStopRequested
        if message_task not in done:
            raise StreamStalled(f"Filtered Stream was silent for {stall_seconds:g} seconds")
        try:
            return message_task.result()
        except StopAsyncIteration as error:
            raise StreamDisconnected("Filtered Stream closed the response") from error
    finally:
        await _cancel_tasks(message_task, stop_task, lease_task)


async def _close_iterator(iterator: AsyncIterator[StreamMessage]) -> None:
    close = getattr(iterator, "aclose", None)
    if close is None or not callable(close):
        return
    await cast(Callable[[], Awaitable[None]], close)()


async def _load_startup_state(
    database: Database,
    settings: Settings,
    *,
    now: datetime,
) -> _StartupState:
    async with database.session() as session:
        state = await session.get(StreamState, "primary")
    if state is None:
        return _StartupState(
            gap_start=now - timedelta(minutes=settings.recovery_bootstrap_minutes),
            failure_count=0,
        )
    candidates = (
        state.recovery_covered_through,
        state.last_event_at,
        state.last_keepalive_at,
        state.connected_at,
    )
    aware_candidates = tuple(
        _aware_utc(value, field="stream activity") for value in candidates if value is not None
    )
    gap_start = max(
        aware_candidates,
        default=now - timedelta(minutes=settings.recovery_bootstrap_minutes),
    )
    return _StartupState(
        gap_start=min(gap_start, now),
        failure_count=max(0, state.failure_count),
    )


async def _record_keepalive(
    database: Database,
    lease: Lease,
    *,
    observed_at: datetime,
) -> None:
    async with database.transaction() as session:
        await _require_lease(session, lease)
        await update_stream_state(
            session,
            connected=True,
            last_keepalive_at=observed_at,
            failure_count=0,
            next_connect_at=None,
            degraded_reason=None,
        )


async def _ingest_stream_post(
    database: Database,
    lease: Lease,
    message: StreamMessage,
    *,
    observed_at: datetime,
    rules: RulesConfig,
) -> None:
    post = message.post
    if post is None:
        raise ValueError("Filtered Stream post message omitted its Post")
    payload = message.raw or post.raw
    try:
        async with database.transaction() as session:
            await _require_lease(session, lease)
            result = await ingest_post_in_transaction(
                session,
                post,
                source=STREAM_SOURCE,
                observed_at=observed_at,
                raw_payload=payload,
                rules=rules,
            )
            await update_stream_state(
                session,
                connected=True,
                last_event_at=observed_at,
                failure_count=0,
                next_connect_at=None,
                degraded_reason=None,
            )
        if result.unknown_remote_rule_ids:
            logger.warning(
                "stream_post_unknown_rule_matches",
                post_id=post.id,
                remote_rule_ids=result.unknown_remote_rule_ids,
            )
    except StreamLeaseLost:
        raise
    except Exception as error:
        await record_failed_ingestion(
            database,
            source=STREAM_SOURCE,
            payload=payload,
            error=error,
        )
        raise


async def _consume_connection(
    database: Database,
    client: StreamApi,
    settings: Settings,
    *,
    lease: Lease,
    stop_event: asyncio.Event,
    lease_lost: asyncio.Event,
    gap_start: datetime,
    backfill_minutes: int | None,
    rules: RulesConfig,
    progress: _ConnectionProgress,
) -> None:
    iterator = client.stream(backfill_minutes=backfill_minutes)
    try:
        while True:
            message = await _next_message(
                iterator,
                stall_seconds=settings.stream_stall_seconds,
                stop_event=stop_event,
                lease_lost=lease_lost,
            )
            observed_at = utcnow()
            if not progress.recovery_enqueued:
                await enqueue_disconnect_recovery(
                    database,
                    settings,
                    coverage_start=gap_start,
                    reconnected_at=observed_at,
                    reason="stream_reconnect",
                    now=observed_at,
                )
                await _update_stream_state_fenced(
                    database,
                    lease,
                    connected=True,
                    connected_at=observed_at,
                    next_connect_at=None,
                    degraded_reason=None,
                )
                progress.recovery_enqueued = True
            if message.kind == "keepalive":
                await _record_keepalive(database, lease, observed_at=observed_at)
            elif message.kind == "post":
                await _ingest_stream_post(
                    database,
                    lease,
                    message,
                    observed_at=observed_at,
                    rules=rules,
                )
            else:
                raise ValueError(f"unsupported Filtered Stream message kind: {message.kind}")
            progress.last_activity_at = observed_at
    finally:
        await _close_iterator(iterator)


async def _wait_before_reconnect(
    delay: float,
    *,
    stop_event: asyncio.Event,
    lease_lost: asyncio.Event,
) -> None:
    stop_task = asyncio.create_task(stop_event.wait())
    lease_task = asyncio.create_task(lease_lost.wait())
    done, _pending = await asyncio.wait(
        {stop_task, lease_task},
        timeout=delay,
        return_when=asyncio.FIRST_COMPLETED,
    )
    if lease_task in done and lease_task.result():
        await _cancel_tasks(stop_task)
        raise StreamLeaseLost("stream service lease guard lost ownership")
    await _cancel_tasks(stop_task, lease_task)


async def run_stream_worker(
    settings: Settings,
    *,
    database: Database | None = None,
    client: StreamApi | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run the singleton fenced Filtered Stream collector until stopped or fatal."""

    settings.validate_role("stream")
    own_database = database is None
    own_client = client is None
    if database is None:
        database = create_database(settings)
    if client is None:
        client = XApiClient(settings)
    shutdown = stop_event or asyncio.Event()
    lease_lost = asyncio.Event()
    instance_id = f"{settings.worker_id}:{os.getpid()}:{uuid4().hex[:12]}"
    lease: Lease | None = None
    guard_task: asyncio.Task[None] | None = None
    terminal_state = "stopped"
    terminal_reason = "stream worker stopped"

    try:
        lease = await acquire_service_lease(
            database,
            name=STREAM_LEASE_NAME,
            owner_id=instance_id,
            lease_seconds=settings.stream_lease_seconds,
        )
        if lease is None:
            await _heartbeat(
                database,
                settings,
                instance_id=instance_id,
                state="stopped",
                lease=None,
                details={"reason": "singleton lease held by another instance"},
            )
            raise RuntimeError("another Filtered Stream worker owns the singleton lease")
        await _heartbeat(
            database,
            settings,
            instance_id=instance_id,
            state="starting",
            lease=lease,
        )
        guard_task = asyncio.create_task(
            _lease_guard(
                database,
                settings,
                lease=lease,
                instance_id=instance_id,
                stop_event=shutdown,
                lease_lost=lease_lost,
            )
        )

        async def rule_sync_fence() -> None:
            if lease is None:
                raise StreamLeaseLost("stream lease disappeared during rule sync")
            await _validate_stream_lease(database, lease)

        sync_result = await sync_rules(
            settings,
            dry_run=False,
            database=database,
            client=client,
            fence=rule_sync_fence,
        )
        rules = load_rules(settings.rules_path)
        if lease_lost.is_set():
            raise StreamLeaseLost("stream lease was lost during rule reconciliation")
        current = utcnow()
        startup = await _load_startup_state(database, settings, now=current)
        gap_start = startup.gap_start
        failures = startup.failure_count
        await _update_stream_state_fenced(
            database,
            lease,
            connected=False,
            rules_hash=sync_result.config_hash,
            next_connect_at=current,
            degraded_reason=None,
        )
        await _heartbeat(
            database,
            settings,
            instance_id=instance_id,
            state="running",
            lease=lease,
            details={"rules_hash": sync_result.config_hash},
        )

        while not shutdown.is_set():
            if lease_lost.is_set():
                raise StreamLeaseLost("stream service lease guard lost ownership")
            connecting_at = utcnow()
            backfill_minutes = enterprise_backfill_minutes(
                settings,
                gap_start=gap_start,
                connecting_at=connecting_at,
            )
            await _update_stream_state_fenced(
                database,
                lease,
                connected=False,
                next_connect_at=None,
                rules_hash=sync_result.config_hash,
                degraded_reason=None,
            )
            progress = _ConnectionProgress()
            retry_after: float | None = None
            permanent = False
            reason = "Filtered Stream disconnected"
            try:
                await _consume_connection(
                    database,
                    client,
                    settings,
                    lease=lease,
                    stop_event=shutdown,
                    lease_lost=lease_lost,
                    gap_start=gap_start,
                    backfill_minutes=backfill_minutes,
                    rules=rules,
                    progress=progress,
                )
                raise StreamDisconnected("Filtered Stream iterator returned")
            except StreamStopRequested:
                break
            except StreamLeaseLost:
                raise
            except XApiError as error:
                await persist_x_api_error_rate_limit(database, error)
                retry_after = error.retry_after_seconds
                permanent = error.permanent
                reason = str(error)
            except (StreamStalled, StreamDisconnected, httpx.RequestError) as error:
                reason = str(error)
            except Exception as error:
                reason = f"{type(error).__name__}: {error}"
                logger.exception("stream_connection_failed", error=reason)

            if progress.last_activity_at is not None:
                failures = 0
                gap_start = progress.last_activity_at
            failures += 1
            delay = stream_backoff_seconds(
                failures,
                retry_after_seconds=retry_after,
            )
            disconnected_at = utcnow()
            next_connect_at = disconnected_at + timedelta(seconds=delay)
            await _update_stream_state_fenced(
                database,
                lease,
                connected=False,
                last_disconnect_at=disconnected_at,
                last_disconnect_reason=reason[:4000],
                failure_count=failures,
                next_connect_at=None if permanent else next_connect_at,
                degraded_reason=reason[:4000] if permanent else None,
            )
            logger.warning(
                "stream_disconnected",
                reason=reason,
                failure_count=failures,
                retry_seconds=None if permanent else round(delay, 3),
                permanent=permanent,
            )
            if permanent:
                terminal_state = "failed"
                terminal_reason = reason
                raise RuntimeError(f"permanent Filtered Stream failure: {reason}")
            await _wait_before_reconnect(
                delay,
                stop_event=shutdown,
                lease_lost=lease_lost,
            )
    except asyncio.CancelledError:
        terminal_reason = "stream worker cancelled"
        raise
    except Exception as error:
        terminal_state = "failed"
        terminal_reason = f"{type(error).__name__}: {error}"
        raise
    finally:
        if lease is not None and not lease_lost.is_set():
            with suppress(Exception):
                await _update_stream_state_fenced(
                    database,
                    lease,
                    connected=False,
                    last_disconnect_at=utcnow(),
                    last_disconnect_reason=terminal_reason[:4000],
                    next_connect_at=None,
                    degraded_reason=(
                        terminal_reason[:4000] if terminal_state == "failed" else None
                    ),
                )
        shutdown.set()
        if guard_task is not None:
            guard_task.cancel()
            with suppress(asyncio.CancelledError):
                await guard_task
        with suppress(Exception):
            await _heartbeat(
                database,
                settings,
                instance_id=instance_id,
                state=terminal_state,
                lease=lease,
                details={"reason": terminal_reason[:4000]},
            )
        if lease is not None:
            with suppress(Exception):
                await release_service_lease(database, lease)
        if own_client:
            await client.aclose()
        if own_database:
            await database.dispose()


reconnect_delay_seconds = stream_backoff_seconds
