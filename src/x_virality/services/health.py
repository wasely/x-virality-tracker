from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

from sqlalchemy import func, or_, select, text

from x_virality.config import Settings
from x_virality.db.models import (
    ApiRateState,
    DiscordOutbox,
    Job,
    StreamState,
    WorkerHeartbeat,
    XUsageDaily,
)
from x_virality.db.session import Database, create_database, utcnow

HealthStatus = Literal["ok", "degraded", "fail"]
EXPECTED_SCHEMA_REVISION = "0001_initial_schema"


@dataclass(frozen=True, slots=True)
class HealthCheck:
    status: HealthStatus
    details: dict[str, Any]

    def as_dict(self, *, include_details: bool) -> dict[str, Any]:
        result: dict[str, Any] = {"status": self.status}
        if include_details:
            result["details"] = self.details
        return result


@dataclass(frozen=True, slots=True)
class HealthReport:
    status: HealthStatus
    ready: bool
    checked_at: datetime
    environment: str
    build_sha: str
    checks: dict[str, HealthCheck]

    def as_dict(self, *, include_details: bool = True) -> dict[str, Any]:
        return {
            "status": self.status,
            "ready": self.ready,
            "checked_at": _iso(self.checked_at),
            "environment": self.environment,
            "build_sha": self.build_sha,
            "checks": {
                name: check.as_dict(include_details=include_details)
                for name, check in self.checks.items()
            },
        }


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    aware = _aware(value)
    return aware.astimezone(UTC).isoformat().replace("+00:00", "Z") if aware else None


def _age_seconds(value: datetime | None, now: datetime) -> float | None:
    aware = _aware(value)
    if aware is None:
        return None
    return max(0.0, (now - aware).total_seconds())


def _rounded_age(value: datetime | None, now: datetime) -> float | None:
    age = _age_seconds(value, now)
    return round(age, 1) if age is not None else None


def _unavailable(*, failed: bool = True) -> HealthCheck:
    return HealthCheck(status="fail" if failed else "degraded", details={"available": False})


async def _schema_check(database: Database) -> HealthCheck:
    try:
        async with database.engine.connect() as connection:
            actual = await connection.scalar(
                text("SELECT version_num FROM alembic_version LIMIT 1")
            )
    except Exception:
        return _unavailable()
    revision = str(actual) if actual is not None else None
    matches = revision == EXPECTED_SCHEMA_REVISION
    return HealthCheck(
        status="ok" if matches else "fail",
        details={
            "actual_revision": revision,
            "expected_revision": EXPECTED_SCHEMA_REVISION,
            "matches": matches,
        },
    )


async def _stream_check(database: Database, settings: Settings, now: datetime) -> HealthCheck:
    try:
        async with database.session() as session:
            state = await session.get(StreamState, "primary")
    except Exception:
        return _unavailable()
    if state is None:
        return HealthCheck(status="fail", details={"present": False})
    activity_candidates = [state.last_keepalive_at, state.last_event_at, state.updated_at]
    activity = max(
        (candidate for candidate in (_aware(item) for item in activity_candidates) if candidate),
        default=None,
    )
    age = _age_seconds(activity, now)
    fresh = age is not None and age <= settings.stream_stall_seconds
    if not state.connected or not fresh:
        status: HealthStatus = "fail"
    elif state.degraded_reason:
        status = "degraded"
    else:
        status = "ok"
    return HealthCheck(
        status=status,
        details={
            "present": True,
            "connected": state.connected,
            "fresh": fresh,
            "last_activity_at": _iso(activity),
            "last_activity_age_seconds": round(age, 1) if age is not None else None,
            "failure_count": state.failure_count,
            "next_connect_at": _iso(state.next_connect_at),
            "recovery_covered_through": _iso(state.recovery_covered_through),
            "has_degraded_reason": bool(state.degraded_reason),
        },
    )


async def _worker_check(database: Database, settings: Settings, now: datetime) -> HealthCheck:
    try:
        async with database.session() as session:
            heartbeat = await session.scalar(
                select(WorkerHeartbeat)
                .where(WorkerHeartbeat.role == "worker")
                .order_by(WorkerHeartbeat.heartbeat_at.desc())
                .limit(1)
            )
    except Exception:
        return _unavailable()
    if heartbeat is None:
        return HealthCheck(status="fail", details={"present": False})
    age = _age_seconds(heartbeat.heartbeat_at, now)
    fresh = age is not None and age <= settings.health_stale_seconds
    normalized_state = heartbeat.state.lower()
    failed_state = normalized_state in {"dead", "error", "failed", "stopped"}
    degraded_state = normalized_state in {"degraded", "draining", "starting"}
    if not fresh or failed_state:
        status: HealthStatus = "fail"
    elif degraded_state:
        status = "degraded"
    else:
        status = "ok"
    return HealthCheck(
        status=status,
        details={
            "present": True,
            "fresh": fresh,
            "heartbeat_at": _iso(heartbeat.heartbeat_at),
            "heartbeat_age_seconds": round(age, 1) if age is not None else None,
            "state": normalized_state[:24],
            "build_matches_api": heartbeat.build_sha == settings.build_sha,
        },
    )


async def _jobs_check(database: Database, settings: Settings, now: datetime) -> HealthCheck:
    try:
        async with database.session() as session:
            due_row = (
                await session.execute(
                    select(func.count(), func.min(Job.run_at)).where(
                        Job.state.in_(("queued", "retry")),
                        Job.run_at <= now,
                    )
                )
            ).one()
            dead_count = int(
                await session.scalar(
                    select(func.count()).select_from(Job).where(Job.state == "dead")
                )
                or 0
            )
            expired_lease_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(Job)
                    .where(Job.state == "leased", Job.lease_expires_at < now)
                )
                or 0
            )
    except Exception:
        return _unavailable()
    due_count = int(due_row[0] or 0)
    oldest_due = _aware(due_row[1])
    oldest_age = _age_seconds(oldest_due, now)
    stale_after = max(settings.health_stale_seconds, settings.job_lease_seconds * 2)
    stale = oldest_age is not None and oldest_age > stale_after
    status: HealthStatus = (
        "degraded" if dead_count > 0 or expired_lease_count > 0 or stale else "ok"
    )
    return HealthCheck(
        status=status,
        details={
            "due_count": due_count,
            "oldest_due_at": _iso(oldest_due),
            "oldest_due_age_seconds": round(oldest_age, 1) if oldest_age is not None else None,
            "stale_due_work": stale,
            "dead_count": dead_count,
            "expired_lease_count": expired_lease_count,
        },
    )


async def _outbox_check(database: Database, settings: Settings, now: datetime) -> HealthCheck:
    try:
        async with database.session() as session:
            due_row = (
                await session.execute(
                    select(func.count(), func.min(DiscordOutbox.run_at)).where(
                        DiscordOutbox.state.in_(("queued", "retry")),
                        DiscordOutbox.run_at <= now,
                    )
                )
            ).one()
            dead_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(DiscordOutbox)
                    .where(DiscordOutbox.state == "dead")
                )
                or 0
            )
            last_sent_at = await session.scalar(
                select(func.max(DiscordOutbox.sent_at)).where(DiscordOutbox.state == "sent")
            )
            expired_lease_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(DiscordOutbox)
                    .where(
                        DiscordOutbox.state == "leased",
                        DiscordOutbox.lease_expires_at < now,
                    )
                )
                or 0
            )
    except Exception:
        return _unavailable()
    due_count = int(due_row[0] or 0)
    oldest_due = _aware(due_row[1])
    oldest_age = _age_seconds(oldest_due, now)
    stale_after = max(settings.health_stale_seconds, settings.job_lease_seconds * 2)
    stuck = oldest_age is not None and oldest_age > stale_after
    status: HealthStatus = (
        "degraded" if dead_count > 0 or expired_lease_count > 0 or stuck else "ok"
    )
    return HealthCheck(
        status=status,
        details={
            "due_count": due_count,
            "oldest_due_at": _iso(oldest_due),
            "oldest_due_age_seconds": round(oldest_age, 1) if oldest_age is not None else None,
            "stuck_delivery": stuck,
            "dead_count": dead_count,
            "expired_lease_count": expired_lease_count,
            "last_sent_at": _iso(last_sent_at),
        },
    )


def _safe_endpoint(value: str) -> str:
    return value.split("?", 1)[0][:128]


async def _rate_check(database: Database, now: datetime) -> HealthCheck:
    try:
        async with database.session() as session:
            rows = list(
                (
                    await session.scalars(
                        select(ApiRateState)
                        .where(ApiRateState.provider.in_(("x", "discord")))
                        .order_by(ApiRateState.provider, ApiRateState.endpoint)
                        .limit(100)
                    )
                ).all()
            )
    except Exception:
        return _unavailable(failed=False)
    if not rows:
        return HealthCheck(status="ok", details={"state": "not_observed", "endpoints": []})
    endpoints: list[dict[str, Any]] = []
    degraded = False
    for row in rows:
        reset_at = _aware(row.reset_at)
        reset_pending = reset_at is None or reset_at > now
        exhausted = row.remaining == 0 and reset_pending
        unhealthy_status = row.last_status in {401, 403, 429} or row.last_status >= 500
        degraded = degraded or exhausted or unhealthy_status
        endpoints.append(
            {
                "provider": row.provider,
                "endpoint": _safe_endpoint(row.endpoint),
                "status_code": row.last_status,
                "limit": row.limit_value,
                "remaining": row.remaining,
                "reset_at": _iso(reset_at),
                "observed_at": _iso(row.observed_at),
                "exhausted": exhausted,
            }
        )
    return HealthCheck(
        status="degraded" if degraded else "ok",
        details={"state": "observed", "endpoints": endpoints},
    )


async def _budget_check(database: Database, settings: Settings, today: date) -> HealthCheck:
    try:
        async with database.session() as session:
            rows = list(
                (
                    await session.scalars(
                        select(XUsageDaily).where(XUsageDaily.usage_date == today).limit(100)
                    )
                ).all()
            )
    except Exception:
        return _unavailable(failed=False)
    if not rows:
        return HealthCheck(
            status="ok",
            details={
                "state": "not_observed",
                "usage_date": today.isoformat(),
                "soft_limit_percent": settings.usage_soft_limit_percent,
            },
        )
    posts_consumed = sum(max(0, row.posts_consumed) for row in rows)
    known_caps = [
        row.project_cap for row in rows if row.project_cap is not None and row.project_cap > 0
    ]
    ratios = [
        Decimal(max(0, row.posts_consumed)) * 100 / Decimal(row.project_cap)
        for row in rows
        if row.project_cap is not None and row.project_cap > 0
    ]
    highest_percent = max(ratios, default=None)
    over_soft_limit = highest_percent is not None and highest_percent >= Decimal(
        settings.usage_soft_limit_percent
    )
    return HealthCheck(
        status="degraded" if over_soft_limit else "ok",
        details={
            "state": "observed",
            "usage_date": today.isoformat(),
            "scope_count": len(rows),
            "posts_consumed": posts_consumed,
            "known_cap_total": sum(known_caps) if known_caps else None,
            "highest_usage_percent": (
                round(float(highest_percent), 2) if highest_percent is not None else None
            ),
            "soft_limit_percent": settings.usage_soft_limit_percent,
            "over_soft_limit": over_soft_limit,
        },
    )


async def collect_health(
    database: Database,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> HealthReport:
    checked_at = now or utcnow()
    if checked_at.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    database_ok = await database.ping()
    checks: dict[str, HealthCheck] = {
        "database": HealthCheck(
            status="ok" if database_ok else "fail",
            details={"reachable": database_ok, "engine": "postgresql"},
        )
    }
    if database_ok:
        checks.update(
            {
                "schema": await _schema_check(database),
                "stream": await _stream_check(database, settings, checked_at),
                "worker": await _worker_check(database, settings, checked_at),
                "jobs": await _jobs_check(database, settings, checked_at),
                "outbox": await _outbox_check(database, settings, checked_at),
                "rate_limits": await _rate_check(database, checked_at),
                "budget": await _budget_check(database, settings, checked_at.date()),
            }
        )
    else:
        for name in ("schema", "stream", "worker", "jobs", "outbox"):
            checks[name] = _unavailable()
        checks["rate_limits"] = _unavailable(failed=False)
        checks["budget"] = _unavailable(failed=False)

    ready = all(check.status == "ok" for check in checks.values())
    if any(check.status == "fail" for check in checks.values()):
        status: HealthStatus = "fail"
    elif any(check.status == "degraded" for check in checks.values()):
        status = "degraded"
    else:
        status = "ok"
    return HealthReport(
        status=status,
        ready=ready,
        checked_at=checked_at,
        environment=settings.environment,
        build_sha=settings.build_sha,
        checks=checks,
    )


async def heartbeat_is_fresh(settings: Settings, role: str) -> bool:
    if role not in {"api", "stream", "worker"}:
        raise ValueError("role must be api, stream, or worker")
    database = create_database(settings)
    try:
        async with database.session() as session:
            heartbeat_at = await session.scalar(
                select(func.max(WorkerHeartbeat.heartbeat_at)).where(
                    WorkerHeartbeat.role == role,
                    or_(
                        WorkerHeartbeat.state.is_(None),
                        WorkerHeartbeat.state.not_in(("dead", "error", "failed", "stopped")),
                    ),
                )
            )
        age = _age_seconds(heartbeat_at, utcnow())
        return age is not None and age <= settings.health_stale_seconds
    except Exception:
        return False
    finally:
        await database.dispose()


get_health_report = collect_health
