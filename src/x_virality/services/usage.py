from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import Settings
from x_virality.db.jobs import enqueue_job
from x_virality.db.models import XUsageDaily
from x_virality.db.repositories import upsert_rate_limit, upsert_usage
from x_virality.db.session import Database, create_database
from x_virality.domain.models import RateLimitObservation
from x_virality.providers.x_api import XApiError

_DEFAULT_SCOPE = "default"
USAGE_REFRESH_JOB_KIND = "usage_refresh"
USAGE_REFRESH_INTERVAL = timedelta(hours=1)
_DATE_KEYS = ("date", "usage_date", "day", "start_time")
_COUNT_KEYS = (
    "tweets_consumed",
    "posts_consumed",
    "usage",
    "count",
    "tweet_count",
    "project_usage",
)
_PROJECT_ID_KEYS = ("project_id", "project")
_APP_ID_KEYS = ("app_id", "client_app_id", "client_id", "application_id")
_CAP_KEYS = ("project_cap", "posts_cap", "tweets_cap", "cap")
_CONTAINER_KEYS = {
    "apps",
    "client_apps",
    "daily_client_app_usage",
    "daily_project_usage",
    "daily_usage",
    "data",
    "days",
    "results",
    "usage",
}


class UsageApi(Protocol):
    async def usage(self) -> tuple[dict[str, Any], RateLimitObservation]: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class UsageRecord:
    usage_date: date
    project_id: str
    app_id: str
    posts_consumed: int
    project_cap: int | None


@dataclass(frozen=True, slots=True)
class UsageAssessment:
    records: tuple[UsageRecord, ...]
    soft_limit_percent: int
    over_soft_limit: bool
    highest_usage_percent: Decimal | None


def _nonnegative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a non-negative integer") from error
    if parsed < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return parsed


def _optional_int(mapping: Mapping[str, object], keys: Sequence[str], *, field: str) -> int | None:
    for key in keys:
        if key not in mapping or mapping[key] is None:
            continue
        value = mapping[key]
        if isinstance(value, Mapping | Sequence) and not isinstance(value, str | bytes | bytearray):
            continue
        return _nonnegative_int(value, field=field)
    return None


def _scope_id(value: object, *, field: str, fallback: str) -> str:
    if value is None:
        return fallback
    normalized = str(value).strip()
    if not normalized or len(normalized) > 64:
        raise ValueError(f"{field} must contain 1-64 characters")
    return normalized


def _mapping_value(mapping: Mapping[str, object], keys: Sequence[str]) -> object | None:
    return next((mapping[key] for key in keys if mapping.get(key) is not None), None)


def _usage_date(value: object) -> date:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            raise ValueError("usage datetime must be timezone-aware")
        return parsed.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError("usage date is missing")
    text = value.strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"invalid usage date: {text}") from error
        if parsed.tzinfo is None:
            raise ValueError("usage datetime must be timezone-aware") from None
        return parsed.astimezone(UTC).date()


def _try_usage_date(value: object) -> date | None:
    try:
        return _usage_date(value)
    except ValueError:
        return None


def parse_usage_payload(payload: Mapping[str, Any]) -> tuple[UsageRecord, ...]:
    """Parse documented X daily project/app usage layouts without inventing zeroes."""

    records: dict[tuple[date, str, str], UsageRecord] = {}

    def add_record(
        *,
        usage_date: date,
        project_id: str,
        app_id: str,
        posts_consumed: int,
        project_cap: int | None,
    ) -> None:
        record = UsageRecord(
            usage_date=usage_date,
            project_id=project_id,
            app_id=app_id,
            posts_consumed=posts_consumed,
            project_cap=project_cap,
        )
        key = (usage_date, project_id, app_id)
        existing = records.get(key)
        if existing is not None and existing != record:
            raise ValueError(
                "X usage payload contains conflicting values for "
                f"{usage_date.isoformat()}/{project_id}/{app_id}"
            )
        records[key] = record

    def walk(
        value: object,
        *,
        inherited_date: date | None = None,
        project_id: str = _DEFAULT_SCOPE,
        app_id: str = _DEFAULT_SCOPE,
        project_cap: int | None = None,
    ) -> None:
        if isinstance(value, Mapping):
            mapping = cast(Mapping[str, object], value)
            local_project = _scope_id(
                _mapping_value(mapping, _PROJECT_ID_KEYS),
                field="project_id",
                fallback=project_id,
            )
            local_app = _scope_id(
                _mapping_value(mapping, _APP_ID_KEYS),
                field="app_id",
                fallback=app_id,
            )
            local_cap = _optional_int(mapping, _CAP_KEYS, field="project_cap")
            if local_cap is None:
                local_cap = project_cap

            local_date = inherited_date
            date_value = _mapping_value(mapping, _DATE_KEYS)
            if date_value is not None:
                local_date = _usage_date(date_value)
            count = _optional_int(mapping, _COUNT_KEYS, field="posts_consumed")
            if local_date is not None and count is not None:
                add_record(
                    usage_date=local_date,
                    project_id=local_project,
                    app_id=local_app,
                    posts_consumed=count,
                    project_cap=local_cap,
                )

            for key, child in mapping.items():
                if key == "daily_client_app_usage" and isinstance(child, Mapping):
                    app_entries = cast(Mapping[str, object], child)
                    keyed_by_app = bool(app_entries) and not any(
                        item_key in {*_DATE_KEYS, *_COUNT_KEYS}
                        or _try_usage_date(item_key) is not None
                        for item_key in app_entries
                    )
                    if keyed_by_app:
                        for app_key, app_usage in app_entries.items():
                            walk(
                                app_usage,
                                inherited_date=local_date,
                                project_id=local_project,
                                app_id=_scope_id(
                                    app_key,
                                    field="app_id",
                                    fallback=local_app,
                                ),
                                project_cap=local_cap,
                            )
                        continue
                keyed_date = _try_usage_date(key)
                if keyed_date is not None:
                    if isinstance(child, Mapping | Sequence) and not isinstance(
                        child, str | bytes | bytearray
                    ):
                        walk(
                            child,
                            inherited_date=keyed_date,
                            project_id=local_project,
                            app_id=local_app,
                            project_cap=local_cap,
                        )
                    elif child is not None:
                        add_record(
                            usage_date=keyed_date,
                            project_id=local_project,
                            app_id=local_app,
                            posts_consumed=_nonnegative_int(child, field="posts_consumed"),
                            project_cap=local_cap,
                        )
                    continue
                if key not in _CONTAINER_KEYS:
                    continue
                if isinstance(child, Mapping | Sequence) and not isinstance(
                    child, str | bytes | bytearray
                ):
                    walk(
                        child,
                        inherited_date=local_date,
                        project_id=local_project,
                        app_id=local_app,
                        project_cap=local_cap,
                    )
            return

        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            for child in value:
                walk(
                    child,
                    inherited_date=inherited_date,
                    project_id=project_id,
                    app_id=app_id,
                    project_cap=project_cap,
                )
            return

        if inherited_date is not None and value is not None:
            add_record(
                usage_date=inherited_date,
                project_id=project_id,
                app_id=app_id,
                posts_consumed=_nonnegative_int(value, field="posts_consumed"),
                project_cap=project_cap,
            )

    walk(payload)
    if not records:
        raise ValueError("X usage payload contains no explicit daily usage records")
    return tuple(records[key] for key in sorted(records))


def soft_limit_reached(
    posts_consumed: int,
    project_cap: int | None,
    *,
    soft_limit_percent: int,
) -> bool:
    if posts_consumed < 0:
        raise ValueError("posts_consumed must be non-negative")
    if not 1 <= soft_limit_percent <= 99:
        raise ValueError("soft_limit_percent must be between 1 and 99")
    if project_cap is None or project_cap <= 0:
        return False
    return posts_consumed * 100 >= project_cap * soft_limit_percent


def assess_usage(records: Sequence[UsageRecord], *, soft_limit_percent: int) -> UsageAssessment:
    if not 1 <= soft_limit_percent <= 99:
        raise ValueError("soft_limit_percent must be between 1 and 99")
    grouped: dict[tuple[date, str], list[UsageRecord]] = {}
    for record in records:
        grouped.setdefault((record.usage_date, record.project_id), []).append(record)

    project_totals: list[tuple[int, int | None]] = []
    for key in sorted(grouped):
        project_records = grouped[key]
        caps = {record.project_cap for record in project_records if record.project_cap is not None}
        if len(caps) > 1:
            raise ValueError(f"conflicting project caps for {key[0].isoformat()}/{key[1]}")
        project_cap = next(iter(caps), None)
        explicit_total = next(
            (
                record.posts_consumed
                for record in project_records
                if record.app_id == _DEFAULT_SCOPE
            ),
            None,
        )
        posts_consumed = (
            explicit_total
            if explicit_total is not None
            else sum(record.posts_consumed for record in project_records)
        )
        project_totals.append((posts_consumed, project_cap))

    ratios = tuple(
        Decimal(posts_consumed) * Decimal(100) / Decimal(project_cap)
        for posts_consumed, project_cap in project_totals
        if project_cap is not None and project_cap > 0
    )
    return UsageAssessment(
        records=tuple(records),
        soft_limit_percent=soft_limit_percent,
        over_soft_limit=any(
            soft_limit_reached(
                posts_consumed,
                project_cap,
                soft_limit_percent=soft_limit_percent,
            )
            for posts_consumed, project_cap in project_totals
        ),
        highest_usage_percent=max(ratios, default=None),
    )


async def current_usage_assessment(
    session: AsyncSession,
    *,
    usage_date: date,
    soft_limit_percent: int,
) -> UsageAssessment:
    rows = tuple(
        (
            await session.scalars(select(XUsageDaily).where(XUsageDaily.usage_date == usage_date))
        ).all()
    )
    records = tuple(
        UsageRecord(
            usage_date=row.usage_date,
            project_id=row.project_id,
            app_id=row.app_id,
            posts_consumed=row.posts_consumed,
            project_cap=row.project_cap,
        )
        for row in rows
    )
    return assess_usage(records, soft_limit_percent=soft_limit_percent)


async def usage_soft_limit_active(
    session: AsyncSession,
    settings: Settings,
    *,
    at: datetime | None = None,
) -> bool:
    observed_at = at or datetime.now(UTC)
    if observed_at.tzinfo is None:
        raise ValueError("at must be timezone-aware")
    assessment = await current_usage_assessment(
        session,
        usage_date=observed_at.astimezone(UTC).date(),
        soft_limit_percent=settings.usage_soft_limit_percent,
    )
    return assessment.over_soft_limit


def usage_refresh_hour(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("at must be timezone-aware")
    return at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


async def enqueue_usage_refresh_job(
    session: AsyncSession,
    *,
    at: datetime,
) -> bool:
    hour = usage_refresh_hour(at)
    job_id = await enqueue_job(
        session,
        kind=USAGE_REFRESH_JOB_KIND,
        dedupe_key=f"usage-refresh:v1:{hour.isoformat()}",
        payload={
            "schema_version": 1,
            "scheduled_hour": hour.isoformat().replace("+00:00", "Z"),
        },
        run_at=at,
        priority=10,
        max_attempts=12,
    )
    return job_id is not None


async def persist_rate_limit(
    session: AsyncSession,
    observation: RateLimitObservation,
    *,
    auth_scope: str = "app",
    headers: dict[str, Any] | None = None,
) -> None:
    await upsert_rate_limit(
        session,
        observation,
        provider="x",
        auth_scope=auth_scope,
        headers=headers,
    )


async def persist_rate_limit_observation(
    database: Database,
    observation: RateLimitObservation,
    *,
    auth_scope: str = "app",
    headers: dict[str, Any] | None = None,
) -> None:
    async with database.transaction() as session:
        await persist_rate_limit(
            session,
            observation,
            auth_scope=auth_scope,
            headers=headers,
        )


async def persist_x_api_error_rate_limit(
    database: Database,
    error: XApiError,
    *,
    auth_scope: str = "app",
) -> bool:
    if error.rate_limit is None:
        return False
    await persist_rate_limit_observation(
        database,
        error.rate_limit,
        auth_scope=auth_scope,
    )
    return True


async def persist_usage_payload(
    session: AsyncSession,
    payload: Mapping[str, Any],
) -> tuple[UsageRecord, ...]:
    records = parse_usage_payload(payload)
    raw = dict(payload)
    for record in records:
        await upsert_usage(
            session,
            usage_date=record.usage_date,
            project_id=record.project_id,
            app_id=record.app_id,
            posts_consumed=record.posts_consumed,
            project_cap=record.project_cap,
            raw=raw,
        )
    return records


async def refresh_usage(
    settings: Settings,
    *,
    database: Database | None = None,
    client: UsageApi | None = None,
) -> UsageAssessment:
    """Fetch usage once, persist its rate state and daily records, then assess budget."""

    own_database = database is None
    own_client = client is None
    if database is None:
        database = create_database(settings)
    if client is None:
        from x_virality.providers.x_api import XApiClient

        client = XApiClient(settings)
    try:
        try:
            payload, rate_limit = await client.usage()
        except XApiError as error:
            await persist_x_api_error_rate_limit(database, error)
            raise
        try:
            records = parse_usage_payload(payload)
        except ValueError:
            await persist_rate_limit_observation(database, rate_limit)
            raise
        async with database.transaction() as session:
            await persist_rate_limit(session, rate_limit)
            for record in records:
                await upsert_usage(
                    session,
                    usage_date=record.usage_date,
                    project_id=record.project_id,
                    app_id=record.app_id,
                    posts_consumed=record.posts_consumed,
                    project_cap=record.project_cap,
                    raw=payload,
                )
        return assess_usage(records, soft_limit_percent=settings.usage_soft_limit_percent)
    finally:
        if own_client:
            await client.aclose()
        if own_database:
            await database.dispose()


async def process_usage_job(
    settings: Settings,
    database: Database,
    client: UsageApi,
    payload: Mapping[str, Any],
) -> UsageAssessment:
    if payload.get("schema_version") != 1:
        raise ValueError("usage refresh job requires schema_version=1")
    scheduled_hour = payload.get("scheduled_hour")
    if not isinstance(scheduled_hour, str):
        raise ValueError("usage refresh job requires scheduled_hour")
    parsed_hour = datetime.fromisoformat(scheduled_hour.replace("Z", "+00:00"))
    if parsed_hour.tzinfo is None or parsed_hour != usage_refresh_hour(parsed_hour):
        raise ValueError("scheduled_hour must be an exact UTC hour")
    return await refresh_usage(settings, database=database, client=client)


parse_usage = parse_usage_payload
is_soft_limited = soft_limit_reached
