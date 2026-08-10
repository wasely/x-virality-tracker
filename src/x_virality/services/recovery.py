from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import Settings, load_rules
from x_virality.db.jobs import ClaimedJob, enqueue_job, renew_job
from x_virality.db.models import Job, RecoveryRun, StreamState, XRule
from x_virality.db.repositories import (
    store_failed_event,
    store_raw_payload,
    update_stream_state,
)
from x_virality.db.session import Database, create_database, utcnow
from x_virality.logging import get_logger
from x_virality.providers.x_api import XApiError, XPage
from x_virality.services.ingestion import (
    RECENT_SEARCH_SOURCE,
    ingest_post_in_transaction,
)
from x_virality.services.usage import persist_rate_limit

RECOVERY_JOB_KIND = "recovery"
RECENT_SEARCH_AVAILABILITY = timedelta(days=7)
MAX_RECOVERY_PAGES = 10_000

RecoveryState = Literal["completed", "unrecoverable"]
logger = get_logger("services.recovery")


class RecoveryApi(Protocol):
    async def recent_search(
        self,
        *,
        query: str,
        start_time: datetime,
        end_time: datetime,
        next_token: str | None = None,
    ) -> XPage: ...


class RecoveryFenceLost(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BackfillEnqueueResult:
    requested_start: datetime
    requested_end: datetime
    recovery_run_ids: tuple[UUID, ...]
    jobs_created: int
    duplicate_jobs: int
    unrecoverable_runs: int


@dataclass(frozen=True, slots=True)
class RecoveryProcessResult:
    recovery_run_id: UUID
    state: RecoveryState
    pages_processed: int
    posts_recovered: int
    recovered_through: datetime | None


@dataclass(frozen=True, slots=True)
class _RecoveryPayload:
    recovery_run_id: UUID
    rule_id: UUID
    query: str
    config_hash: str
    managed_tag: str
    requested_start: datetime
    requested_end: datetime


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_utc_datetime(value: str | datetime, *, field: str = "timestamp") -> datetime:
    if isinstance(value, datetime):
        return _aware_utc(value, field=field)
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from error
    return _aware_utc(parsed, field=field)


def _bounded_window(
    settings: Settings,
    *,
    since: str | datetime | None,
    until: str | datetime | None,
    now: datetime,
) -> tuple[datetime, datetime]:
    end = parse_utc_datetime(until, field="until") if until is not None else now
    start = (
        parse_utc_datetime(since, field="since")
        if since is not None
        else end - timedelta(minutes=settings.recovery_bootstrap_minutes)
    )
    if start >= end:
        raise ValueError("backfill since must be earlier than until")
    if end > now + timedelta(seconds=5):
        raise ValueError("backfill until must not be in the future")
    return start, end


def recovery_dedupe_key(
    *,
    rule_id: UUID,
    config_hash: str,
    requested_start: datetime,
    requested_end: datetime,
) -> str:
    identity = "|".join(
        (
            str(rule_id),
            config_hash,
            _iso(requested_start),
            _iso(requested_end),
        )
    )
    return f"recovery:v1:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def _recovery_advisory_lock_key(
    *,
    rule_id: UUID,
    requested_start: datetime,
    requested_end: datetime,
) -> int:
    identity = f"{rule_id}|{_iso(requested_start)}|{_iso(requested_end)}"
    unsigned = int.from_bytes(
        hashlib.sha256(identity.encode("utf-8")).digest()[:8],
        "big",
    )
    return unsigned if unsigned < 1 << 63 else unsigned - (1 << 64)


def recovery_job_payload(
    *,
    recovery_run_id: UUID,
    rule: XRule,
    requested_start: datetime,
    requested_end: datetime,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "recovery_run_id": str(recovery_run_id),
        "rule_id": str(rule.id),
        "query": rule.query,
        "config_hash": rule.config_hash,
        "managed_tag": rule.managed_tag,
        "requested_start": _iso(requested_start),
        "requested_end": _iso(requested_end),
        "reason": reason[:64],
    }


def _parse_recovery_payload(payload: Mapping[str, Any]) -> _RecoveryPayload:
    if payload.get("schema_version") != 1:
        raise ValueError("unsupported recovery job schema_version")
    try:
        recovery_run_id = UUID(str(payload["recovery_run_id"]))
        rule_id = UUID(str(payload["rule_id"]))
        query = str(payload["query"]).strip()
        config_hash = str(payload["config_hash"]).strip()
        managed_tag = str(payload["managed_tag"]).strip()
        requested_start = parse_utc_datetime(
            str(payload["requested_start"]), field="requested_start"
        )
        requested_end = parse_utc_datetime(str(payload["requested_end"]), field="requested_end")
    except (KeyError, ValueError) as error:
        raise ValueError("invalid recovery job payload") from error
    if not query or len(query) > 512:
        raise ValueError("recovery query must contain 1-512 characters")
    if not config_hash or len(config_hash) > 64:
        raise ValueError("invalid recovery config_hash")
    if not managed_tag or len(managed_tag) > 255:
        raise ValueError("invalid recovery managed_tag")
    if requested_start >= requested_end:
        raise ValueError("recovery requested_start must precede requested_end")
    return _RecoveryPayload(
        recovery_run_id=recovery_run_id,
        rule_id=rule_id,
        query=query,
        config_hash=config_hash,
        managed_tag=managed_tag,
        requested_start=requested_start,
        requested_end=requested_end,
    )


async def _enqueue_window(
    database: Database,
    *,
    requested_start: datetime,
    requested_end: datetime,
    reason: str,
    now: datetime,
) -> BackfillEnqueueResult:
    cutoff = now - RECENT_SEARCH_AVAILABILITY
    unavailable = requested_start < cutoff
    run_ids: list[UUID] = []
    jobs_created = 0
    duplicates = 0
    unrecoverable = 0
    async with database.transaction() as session:
        rules = tuple(
            (
                await session.scalars(
                    select(XRule).where(XRule.enabled.is_(True)).order_by(XRule.local_key)
                )
            ).all()
        )
        if not rules:
            raise RuntimeError("no enabled local X rules are available for recovery")
        for rule in rules:
            if unavailable:
                await session.scalar(
                    select(
                        func.pg_advisory_xact_lock(
                            _recovery_advisory_lock_key(
                                rule_id=rule.id,
                                requested_start=requested_start,
                                requested_end=requested_end,
                            )
                        )
                    )
                )
                existing = await session.scalar(
                    select(RecoveryRun.id).where(
                        RecoveryRun.rule_id == rule.id,
                        RecoveryRun.requested_start == requested_start,
                        RecoveryRun.requested_end == requested_end,
                        RecoveryRun.state == "unrecoverable",
                    )
                )
                if existing is not None:
                    duplicates += 1
                    continue
                run = RecoveryRun(
                    id=uuid4(),
                    rule_id=rule.id,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    recovered_through=None,
                    next_token=None,
                    state="unrecoverable",
                    pages_processed=0,
                    posts_recovered=0,
                    last_error=(
                        "requested start precedes the seven-day Recent Search availability window"
                    ),
                    created_at=now,
                    updated_at=now,
                )
                session.add(run)
                run_ids.append(run.id)
                unrecoverable += 1
                continue

            run_id = uuid4()
            job_id = await enqueue_job(
                session,
                kind=RECOVERY_JOB_KIND,
                dedupe_key=recovery_dedupe_key(
                    rule_id=rule.id,
                    config_hash=rule.config_hash,
                    requested_start=requested_start,
                    requested_end=requested_end,
                ),
                payload=recovery_job_payload(
                    recovery_run_id=run_id,
                    rule=rule,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    reason=reason,
                ),
                run_at=now,
                priority=25,
                max_attempts=12,
            )
            if job_id is None:
                duplicates += 1
                continue
            run = RecoveryRun(
                id=run_id,
                rule_id=rule.id,
                requested_start=requested_start,
                requested_end=requested_end,
                recovered_through=None,
                next_token=None,
                state="queued",
                pages_processed=0,
                posts_recovered=0,
                last_error=None,
                created_at=now,
                updated_at=now,
            )
            session.add(run)
            run_ids.append(run_id)
            jobs_created += 1
    logger.info(
        "recovery_window_enqueued",
        reason=reason,
        requested_start=_iso(requested_start),
        requested_end=_iso(requested_end),
        jobs_created=jobs_created,
        duplicate_jobs=duplicates,
        unrecoverable_runs=unrecoverable,
    )
    return BackfillEnqueueResult(
        requested_start=requested_start,
        requested_end=requested_end,
        recovery_run_ids=tuple(run_ids),
        jobs_created=jobs_created,
        duplicate_jobs=duplicates,
        unrecoverable_runs=unrecoverable,
    )


async def enqueue_recovery_window(
    database: Database,
    *,
    requested_start: datetime,
    requested_end: datetime,
    reason: str,
    now: datetime | None = None,
) -> BackfillEnqueueResult:
    current = _aware_utc(now or utcnow(), field="now")
    start = _aware_utc(requested_start, field="requested_start")
    end = _aware_utc(requested_end, field="requested_end")
    if start >= end:
        raise ValueError("recovery requested_start must precede requested_end")
    if end > current + timedelta(seconds=5):
        raise ValueError("recovery requested_end must not be in the future")
    return await _enqueue_window(
        database,
        requested_start=start,
        requested_end=end,
        reason=reason,
        now=current,
    )


async def enqueue_disconnect_recovery(
    database: Database,
    settings: Settings,
    *,
    coverage_start: datetime,
    reconnected_at: datetime,
    reason: str = "stream_disconnect",
    now: datetime | None = None,
) -> BackfillEnqueueResult | None:
    current = _aware_utc(now or utcnow(), field="now")
    start = _aware_utc(coverage_start, field="coverage_start") - timedelta(
        seconds=settings.recovery_overlap_seconds
    )
    end = _aware_utc(reconnected_at, field="reconnected_at")
    if start >= end:
        return None
    return await enqueue_recovery_window(
        database,
        requested_start=start,
        requested_end=end,
        reason=reason,
        now=current,
    )


async def enqueue_manual_backfill(
    settings: Settings,
    *,
    since: str | datetime | None = None,
    until: str | datetime | None = None,
    database: Database | None = None,
    now: datetime | None = None,
) -> BackfillEnqueueResult:
    settings.validate_role("backfill")
    current = _aware_utc(now or utcnow(), field="now")
    start, end = _bounded_window(
        settings,
        since=since,
        until=until,
        now=current,
    )
    own_database = database is None
    if database is None:
        database = create_database(settings)
    try:
        return await _enqueue_window(
            database,
            requested_start=start,
            requested_end=end,
            reason="manual",
            now=current,
        )
    finally:
        if own_database:
            await database.dispose()


async def _lock_owned_job(
    session: AsyncSession,
    claimed: ClaimedJob,
    *,
    worker_id: str,
) -> Job:
    job = await session.scalar(
        select(Job)
        .where(
            Job.id == claimed.id,
            Job.kind == RECOVERY_JOB_KIND,
            Job.state == "leased",
            Job.lease_owner == worker_id,
            Job.fencing_token == claimed.fencing_token,
        )
        .with_for_update()
    )
    if job is None:
        raise RecoveryFenceLost("recovery job lease is no longer owned by this worker")
    return job


async def _lock_recovery_run(
    session: AsyncSession,
    payload: _RecoveryPayload,
) -> RecoveryRun:
    run = await session.scalar(
        select(RecoveryRun)
        .where(
            RecoveryRun.id == payload.recovery_run_id,
            RecoveryRun.rule_id == payload.rule_id,
        )
        .with_for_update()
    )
    if run is None:
        raise ValueError("recovery run referenced by the job does not exist")
    if (
        _aware_utc(run.requested_start, field="RecoveryRun.requested_start")
        != payload.requested_start
        or _aware_utc(run.requested_end, field="RecoveryRun.requested_end") != payload.requested_end
    ):
        raise ValueError("recovery job boundaries differ from its durable run")
    return run


def _result(run: RecoveryRun, state: RecoveryState) -> RecoveryProcessResult:
    return RecoveryProcessResult(
        recovery_run_id=run.id,
        state=state,
        pages_processed=run.pages_processed,
        posts_recovered=run.posts_recovered,
        recovered_through=(
            _aware_utc(run.recovered_through, field="RecoveryRun.recovered_through")
            if run.recovered_through is not None
            else None
        ),
    )


async def _advance_stream_coverage(
    session: AsyncSession,
    *,
    requested_start: datetime,
    requested_end: datetime,
) -> None:
    enabled_rule_ids = tuple(
        (await session.scalars(select(XRule.id).where(XRule.enabled.is_(True)))).all()
    )
    if not enabled_rule_ids:
        return
    covered = int(
        await session.scalar(
            select(func.count(func.distinct(RecoveryRun.rule_id))).where(
                RecoveryRun.rule_id.in_(enabled_rule_ids),
                RecoveryRun.state == "completed",
                RecoveryRun.requested_start <= requested_start,
                RecoveryRun.recovered_through >= requested_end,
            )
        )
        or 0
    )
    if covered != len(enabled_rule_ids):
        return
    state = await session.get(StreamState, "primary")
    current = (
        _aware_utc(state.recovery_covered_through, field="recovery_covered_through")
        if state is not None and state.recovery_covered_through is not None
        else None
    )
    if current is None or requested_end > current:
        await update_stream_state(
            session,
            recovery_covered_through=requested_end,
        )


async def _mark_request_failure(
    database: Database,
    *,
    claimed: ClaimedJob,
    worker_id: str,
    payload: _RecoveryPayload,
    error: Exception,
) -> None:
    async with database.transaction() as session:
        await _lock_owned_job(session, claimed, worker_id=worker_id)
        run = await _lock_recovery_run(session, payload)
        if isinstance(error, XApiError) and error.rate_limit is not None:
            await persist_rate_limit(session, error.rate_limit)
        run.state = "failed"
        run.last_error = f"{type(error).__name__}: {error}"[:4000]
        run.updated_at = utcnow()


async def _mark_page_failure(
    database: Database,
    *,
    claimed: ClaimedJob,
    worker_id: str,
    payload: _RecoveryPayload,
    page: XPage,
    error: Exception,
) -> None:
    try:
        async with database.transaction() as session:
            await _lock_owned_job(session, claimed, worker_id=worker_id)
            run = await _lock_recovery_run(session, payload)
            await persist_rate_limit(session, page.rate_limit)
            await store_failed_event(
                session,
                source=RECENT_SEARCH_SOURCE,
                payload=page.raw,
                error=error,
            )
            run.state = "failed"
            run.last_error = f"{type(error).__name__}: {error}"[:4000]
            run.updated_at = utcnow()
    except RecoveryFenceLost:
        raise
    except Exception as persistence_error:
        logger.error(
            "recovery_failure_persistence_failed",
            recovery_run_id=str(payload.recovery_run_id),
            error_class=type(persistence_error).__name__,
            error=str(persistence_error),
        )


async def process_recovery_job(
    settings: Settings,
    database: Database,
    client: RecoveryApi,
    claimed: ClaimedJob,
    *,
    worker_id: str,
    now: datetime | None = None,
) -> RecoveryProcessResult:
    """Resume a fenced recovery run; each page and cursor commit atomically."""

    if claimed.kind != RECOVERY_JOB_KIND:
        raise ValueError(f"expected {RECOVERY_JOB_KIND} job, got {claimed.kind}")
    payload = _parse_recovery_payload(claimed.payload)
    rules = load_rules(settings.rules_path)
    current = _aware_utc(now or utcnow(), field="now")
    cutoff = current - RECENT_SEARCH_AVAILABILITY

    async with database.transaction() as session:
        await _lock_owned_job(session, claimed, worker_id=worker_id)
        run = await _lock_recovery_run(session, payload)
        if run.state == "completed":
            return _result(run, "completed")
        if run.state == "unrecoverable":
            return _result(run, "unrecoverable")
        if payload.requested_start < cutoff:
            run.state = "unrecoverable"
            run.last_error = (
                "requested start precedes the seven-day Recent Search availability window"
            )
            run.updated_at = current
            return _result(run, "unrecoverable")
        run.state = "running"
        run.last_error = None
        run.updated_at = current
        await renew_job(
            session,
            claimed,
            worker_id=worker_id,
            lease_seconds=settings.job_lease_seconds,
        )
        next_token = run.next_token

    seen_tokens: set[str] = set()
    while True:
        if next_token is not None:
            if next_token in seen_tokens:
                cycle_error = RuntimeError("X Recent Search returned a pagination token cycle")
                await _mark_request_failure(
                    database,
                    claimed=claimed,
                    worker_id=worker_id,
                    payload=payload,
                    error=cycle_error,
                )
                raise cycle_error
            seen_tokens.add(next_token)
        try:
            page = await client.recent_search(
                query=payload.query,
                start_time=payload.requested_start,
                end_time=payload.requested_end,
                next_token=next_token,
            )
        except XApiError as error:
            await _mark_request_failure(
                database,
                claimed=claimed,
                worker_id=worker_id,
                payload=payload,
                error=error,
            )
            raise
        except Exception as error:
            await _mark_request_failure(
                database,
                claimed=claimed,
                worker_id=worker_id,
                payload=payload,
                error=error,
            )
            raise

        if page.next_token is not None and (
            page.next_token == next_token or page.next_token in seen_tokens
        ):
            pagination_error = RuntimeError("X Recent Search pagination token did not advance")
            await _mark_page_failure(
                database,
                claimed=claimed,
                worker_id=worker_id,
                payload=payload,
                page=page,
                error=pagination_error,
            )
            raise pagination_error
        observed_at = utcnow()
        try:
            async with database.transaction() as session:
                await _lock_owned_job(session, claimed, worker_id=worker_id)
                run = await _lock_recovery_run(session, payload)
                if run.next_token != next_token:
                    raise RecoveryFenceLost(
                        "recovery cursor changed while this page was being fetched"
                    )
                if run.pages_processed >= MAX_RECOVERY_PAGES:
                    raise RuntimeError("recovery page limit exceeded")
                await store_raw_payload(
                    session,
                    source=RECENT_SEARCH_SOURCE,
                    object_type="search_page",
                    object_id=f"{run.id}:{run.pages_processed + 1}",
                    payload=page.raw,
                    seen_at=observed_at,
                )
                for post in page.posts:
                    await ingest_post_in_transaction(
                        session,
                        post,
                        source=RECENT_SEARCH_SOURCE,
                        observed_at=observed_at,
                        raw_payload=post.raw,
                        local_rule_id=payload.rule_id,
                        local_matched_tag=payload.managed_tag,
                        rules=rules,
                    )
                await persist_rate_limit(session, page.rate_limit)
                run.pages_processed += 1
                run.posts_recovered += len(page.posts)
                run.next_token = page.next_token
                run.updated_at = observed_at
                if page.next_token is None:
                    run.state = "completed"
                    run.recovered_through = payload.requested_end
                    run.last_error = None
                    await _advance_stream_coverage(
                        session,
                        requested_start=payload.requested_start,
                        requested_end=payload.requested_end,
                    )
                else:
                    run.state = "running"
                renewed = await renew_job(
                    session,
                    claimed,
                    worker_id=worker_id,
                    lease_seconds=settings.job_lease_seconds,
                )
                if not renewed:
                    raise RecoveryFenceLost("recovery job lease could not be renewed")
                result = _result(run, "completed")
        except RecoveryFenceLost:
            raise
        except Exception as error:
            await _mark_page_failure(
                database,
                claimed=claimed,
                worker_id=worker_id,
                payload=payload,
                page=page,
                error=error,
            )
            raise
        if page.next_token is None:
            return result
        next_token = page.next_token


process_recovery = process_recovery_job
