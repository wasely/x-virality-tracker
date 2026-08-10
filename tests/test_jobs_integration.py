from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select, update

from x_virality.db.jobs import (
    acquire_service_lease,
    claim_jobs,
    complete_job,
    enqueue_job,
    fail_job,
    renew_service_lease,
)
from x_virality.db.models import Job, JobAttempt, JobDeadLetter, ServiceLease
from x_virality.db.session import Database, utcnow

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_skip_locked_claims_are_disjoint(database: Database) -> None:
    async with database.transaction() as session:
        for index in range(6):
            await enqueue_job(
                session,
                kind="snapshot",
                dedupe_key=f"snapshot:{index}",
                payload={"post_id": str(index)},
                run_at=utcnow(),
            )

    first, second = await asyncio.gather(
        claim_jobs(database, worker_id="worker-a", lease_seconds=30, limit=3),
        claim_jobs(database, worker_id="worker-b", lease_seconds=30, limit=3),
    )

    claimed_ids = {job.id for job in first} | {job.id for job in second}
    assert len(first) == 3
    assert len(second) == 3
    assert len(claimed_ids) == 6


@pytest.mark.asyncio
async def test_expired_job_is_reclaimed_and_stale_token_is_fenced(database: Database) -> None:
    async with database.transaction() as session:
        job_id = await enqueue_job(
            session,
            kind="snapshot",
            dedupe_key="snapshot:reclaim",
            payload={"post_id": "1"},
            run_at=utcnow(),
        )
    assert job_id is not None

    (stale_claim,) = await claim_jobs(database, worker_id="worker-a", lease_seconds=30, limit=1)
    async with database.transaction() as session:
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(lease_expires_at=utcnow() - timedelta(seconds=1))
        )

    (fresh_claim,) = await claim_jobs(database, worker_id="worker-b", lease_seconds=30, limit=1)

    assert fresh_claim.fencing_token != stale_claim.fencing_token
    assert not await complete_job(database, stale_claim, worker_id="worker-a")
    assert await complete_job(database, fresh_claim, worker_id="worker-b", result={"ok": True})

    async with database.session() as session:
        job = await session.get(Job, job_id)
        attempts = list(
            (
                await session.scalars(
                    select(JobAttempt)
                    .where(JobAttempt.job_id == job_id)
                    .order_by(JobAttempt.attempt_number)
                )
            ).all()
        )
    assert job is not None
    assert job.state == "completed"
    assert job.result == {"ok": True}
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]
    assert attempts[0].outcome == "retry"
    assert attempts[0].error_class == "LeaseExpired"
    assert attempts[1].outcome == "completed"


@pytest.mark.asyncio
async def test_rate_limit_deferral_does_not_burn_attempt(database: Database) -> None:
    async with database.transaction() as session:
        job_id = await enqueue_job(
            session,
            kind="snapshot",
            dedupe_key="snapshot:rate-limit",
            payload={"post_id": "1"},
            run_at=utcnow(),
            max_attempts=2,
        )
    assert job_id is not None

    (claim,) = await claim_jobs(database, worker_id="worker", lease_seconds=30, limit=1)
    outcome = await fail_job(
        database,
        claim,
        worker_id="worker",
        error_class="XApiError",
        error_detail="rate limited",
        retry_delay_seconds=0,
        rate_limited=True,
    )

    assert outcome == "rate_limited"
    async with database.session() as session:
        job = await session.get(Job, job_id)
        attempt = await session.get(JobAttempt, claim.attempt_id)
    assert job is not None
    assert job.attempts == 0
    assert job.state == "retry"
    assert attempt is not None
    assert attempt.outcome == "rate_limited"


@pytest.mark.asyncio
async def test_permanent_failure_creates_dead_letter(database: Database) -> None:
    async with database.transaction() as session:
        job_id = await enqueue_job(
            session,
            kind="snapshot",
            dedupe_key="snapshot:dead",
            payload={"post_id": "1"},
            run_at=utcnow(),
        )
    assert job_id is not None

    (claim,) = await claim_jobs(database, worker_id="worker", lease_seconds=30, limit=1)
    outcome = await fail_job(
        database,
        claim,
        worker_id="worker",
        error_class="InvalidPayload",
        error_detail="cannot process",
        permanent=True,
    )

    assert outcome == "dead"
    async with database.session() as session:
        dead = await session.scalar(select(JobDeadLetter).where(JobDeadLetter.job_id == job_id))
    assert dead is not None
    assert dead.error_class == "InvalidPayload"


@pytest.mark.asyncio
async def test_service_lease_fencing_increments_after_expiry(database: Database) -> None:
    first = await acquire_service_lease(
        database, name="stream", owner_id="stream-a", lease_seconds=30
    )
    assert first is not None
    assert first.fencing_token == 1
    assert (
        await acquire_service_lease(database, name="stream", owner_id="stream-b", lease_seconds=30)
        is None
    )

    async with database.transaction() as session:
        await session.execute(
            update(ServiceLease)
            .where(ServiceLease.name == "stream")
            .values(expires_at=utcnow() - timedelta(seconds=1))
        )

    second = await acquire_service_lease(
        database, name="stream", owner_id="stream-b", lease_seconds=30
    )
    assert second is not None
    assert second.fencing_token == 2
    assert await renew_service_lease(database, first, lease_seconds=30) is None
