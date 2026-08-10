from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.db.models import (
    Job,
    JobAttempt,
    JobDeadLetter,
    ServiceLease,
    WorkerHeartbeat,
)
from x_virality.db.session import Database, utcnow

JobOutcome = Literal["completed", "retry", "failed", "dead", "rate_limited"]


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    id: UUID
    kind: str
    payload: dict[str, Any]
    attempt_id: int
    attempt_number: int
    fencing_token: UUID
    lease_expires_at: Any


@dataclass(frozen=True, slots=True)
class Lease:
    name: str
    owner_id: str
    fencing_token: int
    expires_at: Any


async def enqueue_job(
    session: AsyncSession,
    *,
    kind: str,
    dedupe_key: str,
    payload: dict[str, Any],
    run_at: Any,
    priority: int = 100,
    max_attempts: int = 6,
) -> UUID | None:
    statement = (
        pg_insert(Job)
        .values(
            id=uuid4(),
            kind=kind,
            dedupe_key=dedupe_key,
            payload=payload,
            state="queued",
            priority=priority,
            run_at=run_at,
            max_attempts=max_attempts,
        )
        .on_conflict_do_nothing(index_elements=[Job.dedupe_key])
        .returning(Job.id)
    )
    job_id = await session.scalar(statement)
    return job_id


async def claim_jobs(
    database: Database,
    *,
    worker_id: str,
    lease_seconds: int,
    limit: int,
    kinds: set[str] | None = None,
) -> list[ClaimedJob]:
    now = utcnow()
    async with database.transaction() as session:
        ready = or_(
            and_(Job.state.in_(("queued", "retry")), Job.run_at <= now),
            and_(Job.state == "leased", Job.lease_expires_at < now),
        )
        statement = select(Job).where(ready)
        if kinds:
            statement = statement.where(Job.kind.in_(kinds))
        statement = (
            statement.order_by(Job.priority, Job.run_at, Job.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        jobs = list((await session.scalars(statement)).all())
        claimed: list[ClaimedJob] = []
        for job in jobs:
            if job.state == "leased":
                expired_attempt = await session.scalar(
                    select(JobAttempt)
                    .where(JobAttempt.job_id == job.id, JobAttempt.finished_at.is_(None))
                    .order_by(JobAttempt.attempt_number.desc())
                    .limit(1)
                    .with_for_update()
                )
                attempts = job.attempts + 1
                exhausted = attempts >= job.max_attempts
                if expired_attempt is not None:
                    expired_attempt.finished_at = now
                    expired_attempt.outcome = "dead" if exhausted else "retry"
                    expired_attempt.error_class = "LeaseExpired"
                    expired_attempt.error_detail = "worker lease expired before completion"
                job.attempts = attempts
                job.last_error_class = "LeaseExpired"
                job.last_error = "worker lease expired before completion"
                if exhausted:
                    job.state = "dead"
                    job.dead_at = now
                    job.lease_owner = None
                    job.fencing_token = None
                    job.lease_expires_at = None
                    job.updated_at = now
                    await session.execute(
                        pg_insert(JobDeadLetter)
                        .values(
                            id=uuid4(),
                            job_id=job.id,
                            kind=job.kind,
                            payload=job.payload,
                            attempts=attempts,
                            error_class="LeaseExpired",
                            error_detail="worker lease expired before completion",
                            dead_at=now,
                        )
                        .on_conflict_do_nothing(index_elements=[JobDeadLetter.job_id])
                    )
                    continue
            token = uuid4()
            expires_at = now + timedelta(seconds=lease_seconds)
            attempt_number = (
                int(
                    await session.scalar(
                        select(func.coalesce(func.max(JobAttempt.attempt_number), 0)).where(
                            JobAttempt.job_id == job.id
                        )
                    )
                    or 0
                )
                + 1
            )
            job.state = "leased"
            job.lease_owner = worker_id
            job.fencing_token = token
            job.lease_expires_at = expires_at
            job.updated_at = now
            attempt = JobAttempt(
                job_id=job.id,
                attempt_number=attempt_number,
                worker_id=worker_id,
                fencing_token=token,
                started_at=now,
            )
            session.add(attempt)
            await session.flush()
            claimed.append(
                ClaimedJob(
                    id=job.id,
                    kind=job.kind,
                    payload=dict(job.payload),
                    attempt_id=attempt.id,
                    attempt_number=attempt_number,
                    fencing_token=token,
                    lease_expires_at=expires_at,
                )
            )
        return claimed


async def renew_job(
    session: AsyncSession,
    claimed: ClaimedJob,
    *,
    worker_id: str,
    lease_seconds: int,
) -> bool:
    now = utcnow()
    result = await session.execute(
        update(Job)
        .where(
            Job.id == claimed.id,
            Job.state == "leased",
            Job.lease_owner == worker_id,
            Job.fencing_token == claimed.fencing_token,
        )
        .values(lease_expires_at=now + timedelta(seconds=lease_seconds), updated_at=now)
    )
    return bool(getattr(result, "rowcount", 0))


async def complete_job(
    database: Database,
    claimed: ClaimedJob,
    *,
    worker_id: str,
    result: dict[str, Any] | None = None,
) -> bool:
    now = utcnow()
    async with database.transaction() as session:
        job = await session.scalar(
            select(Job)
            .where(
                Job.id == claimed.id,
                Job.state == "leased",
                Job.lease_owner == worker_id,
                Job.fencing_token == claimed.fencing_token,
            )
            .with_for_update()
        )
        if job is None:
            return False
        job.state = "completed"
        job.result = result
        job.completed_at = now
        job.lease_owner = None
        job.fencing_token = None
        job.lease_expires_at = None
        job.last_error = None
        job.last_error_class = None
        job.updated_at = now
        attempt = await session.get(JobAttempt, claimed.attempt_id)
        if attempt is not None:
            attempt.finished_at = now
            attempt.outcome = "completed"
        return True


async def fail_job(
    database: Database,
    claimed: ClaimedJob,
    *,
    worker_id: str,
    error_class: str,
    error_detail: str,
    retry_delay_seconds: float = 0,
    permanent: bool = False,
    rate_limited: bool = False,
) -> JobOutcome | None:
    now = utcnow()
    async with database.transaction() as session:
        job = await session.scalar(
            select(Job)
            .where(
                Job.id == claimed.id,
                Job.state == "leased",
                Job.lease_owner == worker_id,
                Job.fencing_token == claimed.fencing_token,
            )
            .with_for_update()
        )
        if job is None:
            return None

        attempts = job.attempts if rate_limited else job.attempts + 1
        dead = permanent or attempts >= job.max_attempts
        if dead:
            outcome: JobOutcome = "dead"
            job.state = "dead"
            job.dead_at = now
            await session.execute(
                pg_insert(JobDeadLetter)
                .values(
                    id=uuid4(),
                    job_id=job.id,
                    kind=job.kind,
                    payload=job.payload,
                    attempts=attempts,
                    error_class=error_class,
                    error_detail=error_detail[:4000],
                    dead_at=now,
                )
                .on_conflict_do_nothing(index_elements=[JobDeadLetter.job_id])
            )
        else:
            outcome = "rate_limited" if rate_limited else "retry"
            job.state = "retry"
            job.run_at = now + timedelta(seconds=max(0, retry_delay_seconds))

        job.attempts = attempts
        job.last_error_class = error_class
        job.last_error = error_detail[:4000]
        job.lease_owner = None
        job.fencing_token = None
        job.lease_expires_at = None
        job.updated_at = now
        attempt = await session.get(JobAttempt, claimed.attempt_id)
        if attempt is not None:
            attempt.finished_at = now
            attempt.outcome = outcome
            attempt.error_class = error_class
            attempt.error_detail = error_detail[:4000]
        return outcome


async def defer_job(
    database: Database,
    claimed: ClaimedJob,
    *,
    worker_id: str,
    retry_delay_seconds: float = 0,
    error_class: str = "WorkerDeferred",
    error_detail: str = "worker deferred leased job",
) -> bool:
    """Fenced requeue that does not consume the job's failure budget."""

    now = utcnow()
    async with database.transaction() as session:
        job = await session.scalar(
            select(Job)
            .where(
                Job.id == claimed.id,
                Job.state == "leased",
                Job.lease_owner == worker_id,
                Job.fencing_token == claimed.fencing_token,
            )
            .with_for_update()
        )
        if job is None:
            return False
        job.state = "retry"
        job.run_at = now + timedelta(seconds=max(0, retry_delay_seconds))
        job.last_error_class = error_class[:128]
        job.last_error = error_detail[:4000]
        job.lease_owner = None
        job.fencing_token = None
        job.lease_expires_at = None
        job.updated_at = now
        attempt = await session.get(JobAttempt, claimed.attempt_id)
        if attempt is not None:
            attempt.finished_at = now
            attempt.outcome = "retry"
            attempt.error_class = error_class[:128]
            attempt.error_detail = error_detail[:4000]
        return True


async def acquire_service_lease(
    database: Database,
    *,
    name: str,
    owner_id: str,
    lease_seconds: int,
) -> Lease | None:
    now = utcnow()
    async with database.transaction() as session:
        row = await session.scalar(
            select(ServiceLease).where(ServiceLease.name == name).with_for_update()
        )
        if row is None:
            row = ServiceLease(
                name=name,
                owner_id=owner_id,
                fencing_token=1,
                expires_at=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
            session.add(row)
        elif row.owner_id == owner_id and row.expires_at > now:
            row.expires_at = now + timedelta(seconds=lease_seconds)
            row.updated_at = now
        elif row.expires_at <= now:
            row.owner_id = owner_id
            row.fencing_token += 1
            row.expires_at = now + timedelta(seconds=lease_seconds)
            row.updated_at = now
        else:
            return None
        await session.flush()
        return Lease(row.name, row.owner_id, row.fencing_token, row.expires_at)


async def renew_service_lease(
    database: Database,
    lease: Lease,
    *,
    lease_seconds: int,
) -> Lease | None:
    now = utcnow()
    async with database.transaction() as session:
        row = await session.scalar(
            select(ServiceLease)
            .where(
                ServiceLease.name == lease.name,
                ServiceLease.owner_id == lease.owner_id,
                ServiceLease.fencing_token == lease.fencing_token,
                ServiceLease.expires_at > now,
            )
            .with_for_update()
        )
        if row is None:
            return None
        row.expires_at = now + timedelta(seconds=lease_seconds)
        row.updated_at = now
        return Lease(row.name, row.owner_id, row.fencing_token, row.expires_at)


async def release_service_lease(database: Database, lease: Lease) -> bool:
    now = utcnow()
    async with database.transaction() as session:
        result = await session.execute(
            update(ServiceLease)
            .where(
                ServiceLease.name == lease.name,
                ServiceLease.owner_id == lease.owner_id,
                ServiceLease.fencing_token == lease.fencing_token,
            )
            .values(expires_at=now, updated_at=now)
        )
        return bool(getattr(result, "rowcount", 0))


async def upsert_heartbeat(
    session: AsyncSession,
    *,
    role: str,
    instance_id: str,
    build_sha: str,
    state: str,
    details: dict[str, Any] | None = None,
) -> None:
    now = utcnow()
    await session.execute(
        pg_insert(WorkerHeartbeat)
        .values(
            role=role,
            instance_id=instance_id,
            build_sha=build_sha,
            started_at=now,
            heartbeat_at=now,
            state=state,
            details=details or {},
        )
        .on_conflict_do_update(
            index_elements=[WorkerHeartbeat.role, WorkerHeartbeat.instance_id],
            set_={
                "build_sha": build_sha,
                "heartbeat_at": now,
                "state": state,
                "details": details or {},
            },
        )
    )
