from __future__ import annotations

import asyncio
import hashlib
import importlib
import math
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar, cast

from x_virality.config import RulesConfig, Settings, load_rules
from x_virality.db.jobs import (
    ClaimedJob,
    claim_jobs,
    complete_job,
    defer_job,
    fail_job,
    renew_job,
    upsert_heartbeat,
)
from x_virality.db.repositories import (
    ClaimedOutbox,
    active_rate_limit_delay,
    claim_outbox,
    complete_outbox,
    defer_outbox,
    fail_outbox,
    renew_outbox,
    upsert_rate_limit,
)
from x_virality.db.session import Database, create_database, utcnow
from x_virality.domain.models import RateLimitObservation
from x_virality.logging import get_logger
from x_virality.providers.discord import DiscordApiClient, DiscordApiError
from x_virality.providers.x_api import XApiClient, XApiError
from x_virality.runtime import shutdown_signal_handlers
from x_virality.services.alerts import create_alert
from x_virality.services.charts import render_chart_artifact

JobResult = dict[str, Any]
JobHandler = Callable[[ClaimedJob], Awaitable[JobResult]]
UsageRefresh = Callable[[], Awaitable[bool]]
AsyncServiceCallable = Callable[..., Awaitable[Any]]
T = TypeVar("T")

_USAGE_REFRESH_SECONDS = 60 * 60
_QUOTA_SENSITIVE_KINDS = frozenset({"bootstrap", "recovery"})
_X_ENDPOINT_BY_KIND = {
    "snapshot": "/2/tweets",
    "recovery": "/2/tweets/search/recent",
    "bootstrap": "/2/tweets/search/recent",
}


class DiscordSender(Protocol):
    async def send_message(
        self,
        channel_id: str,
        payload: dict[str, Any],
        *,
        nonce: str,
        chart_png: bytes | None = None,
        filename: str = "virality.png",
        attachment_description: str | None = None,
    ) -> Any: ...


class LeaseLostError(RuntimeError):
    """The worker no longer owns a durable work item."""


class LeaseRenewalError(RuntimeError):
    """The worker could not prove that it still owns a durable work item."""


@dataclass(frozen=True, slots=True)
class FailureDirective:
    error_class: str
    error_detail: str
    retry_delay_seconds: float
    permanent: bool
    rate_limited: bool


@dataclass(frozen=True, slots=True)
class DefaultWorkerComponents:
    handlers: dict[str, JobHandler]
    usage_refresh: UsageRefresh | None
    disabled_services: tuple[str, ...]


def bounded_retry_delay(
    attempt_number: int,
    *,
    base_seconds: float,
    maximum_seconds: float,
    jitter_key: str,
) -> float:
    """Deterministic exponential backoff with per-item jitter."""

    exponent = min(max(0, attempt_number - 1), 30)
    ceiling: float = min(maximum_seconds, base_seconds * (2.0**exponent))
    digest = hashlib.sha256(f"{jitter_key}:{attempt_number}".encode()).digest()
    jitter: float = 0.5 + int.from_bytes(digest[:2], "big") / 131_070
    return float(max(0.1, ceiling * jitter))


def _rate_limit_delay(error: Exception, fallback_seconds: float) -> float:
    candidates: list[float] = []
    retry_after = getattr(error, "retry_after_seconds", None)
    if isinstance(retry_after, int | float) and math.isfinite(retry_after):
        candidates.append(max(0.0, float(retry_after)))
    observation = getattr(error, "rate_limit", None)
    reset_at = getattr(observation, "reset_at", None)
    if isinstance(reset_at, datetime):
        aware_reset = reset_at if reset_at.tzinfo is not None else reset_at.replace(tzinfo=UTC)
        candidates.append(max(0.0, (aware_reset - utcnow()).total_seconds()))
    delay = max(candidates, default=fallback_seconds)
    return max(1.0, delay + 0.25)


def classify_failure(
    error: Exception,
    *,
    attempt_number: int,
    settings: Settings,
    jitter_key: str,
) -> FailureDirective:
    rate_limited = bool(getattr(error, "rate_limited", False))
    permanent = bool(getattr(error, "permanent", False))
    if isinstance(error, ValueError | KeyError | TypeError):
        permanent = True
    if rate_limited:
        delay = _rate_limit_delay(error, settings.worker_retry_base_seconds)
        permanent = False
    elif permanent:
        delay = 0.0
    else:
        delay = bounded_retry_delay(
            attempt_number,
            base_seconds=settings.worker_retry_base_seconds,
            maximum_seconds=settings.worker_retry_max_seconds,
            jitter_key=jitter_key,
        )
    return FailureDirective(
        error_class=type(error).__name__[:128],
        error_detail=(str(error) or type(error).__name__)[:4000],
        retry_delay_seconds=delay,
        permanent=permanent,
        rate_limited=rate_limited,
    )


async def run_with_lease_renewal(
    operation: Coroutine[Any, Any, T],
    *,
    renew: Callable[[], Awaitable[bool]],
    interval_seconds: float,
) -> T:
    """Run an operation while renewing its fence; cancel it if ownership is uncertain."""

    async def renewal_loop() -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                renewed = await renew()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise LeaseRenewalError("durable lease renewal failed") from error
            if not renewed:
                raise LeaseLostError("durable lease fencing token is no longer current")

    operation_task: asyncio.Task[T] = asyncio.create_task(operation)
    renewal_task: asyncio.Task[None] = asyncio.create_task(renewal_loop())
    try:
        done, _ = await asyncio.wait(
            {operation_task, renewal_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if operation_task in done:
            return await operation_task
        await renewal_task
        raise AssertionError("lease renewal loop returned unexpectedly")
    finally:
        for task in (operation_task, renewal_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, renewal_task, return_exceptions=True)


def _load_service_callable(
    module_name: str, attribute: str
) -> tuple[AsyncServiceCallable | None, str | None]:
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        return None, f"{module_name}: {type(error).__name__}"
    value = getattr(module, attribute, None)
    if not callable(value):
        return None, f"{module_name}.{attribute}: unavailable"
    return cast(AsyncServiceCallable, value), None


def _stringify(value: object) -> str | None:
    return None if value is None else str(value)


def build_default_worker_components(
    settings: Settings,
    database: Database,
    x_client: Any,
    rules: RulesConfig,
    *,
    worker_id: str,
) -> DefaultWorkerComponents:
    """Bind staged service contracts without making absent services an import-time failure."""

    handlers: dict[str, JobHandler] = {}
    disabled: list[str] = []

    snapshot_processor, snapshot_error = _load_service_callable(
        "x_virality.services.snapshots", "process_snapshot_job"
    )
    if snapshot_processor is None:
        disabled.append(snapshot_error or "snapshots")
    else:

        async def handle_snapshot(claimed: ClaimedJob) -> JobResult:
            async with database.transaction() as session:
                batch = await snapshot_processor(session, x_client, rules, claimed.payload)
                candidates = tuple(getattr(batch, "candidates", ()))
                alerts_created = 0
                for candidate in candidates:
                    result = await create_alert(
                        session,
                        candidate,
                        settings,
                        cooldown_minutes=rules.alert_policy.cooldown_minutes,
                    )
                    alerts_created += int(result.created)
            return {
                "outcomes": len(tuple(getattr(batch, "outcomes", ()))),
                "observed": bool(getattr(batch, "observed", False)),
                "alert_candidates": len(candidates),
                "alerts_created": alerts_created,
            }

        handlers["snapshot"] = handle_snapshot

    recovery_processor, recovery_error = _load_service_callable(
        "x_virality.services.recovery", "process_recovery_job"
    )
    if recovery_processor is None:
        disabled.append(recovery_error or "recovery")
    else:

        async def handle_recovery(claimed: ClaimedJob) -> JobResult:
            result = await recovery_processor(
                settings,
                database,
                x_client,
                claimed,
                worker_id=worker_id,
            )
            return {
                "recovery_run_id": _stringify(getattr(result, "recovery_run_id", None)),
                "state": _stringify(getattr(result, "state", None)),
                "pages_processed": int(getattr(result, "pages_processed", 0)),
                "posts_recovered": int(getattr(result, "posts_recovered", 0)),
                "next_token": _stringify(getattr(result, "next_token", None)),
            }

        handlers["recovery"] = handle_recovery

    topic_processor, topic_error = _load_service_callable(
        "x_virality.services.topics", "process_post_topic"
    )
    if topic_processor is None:
        disabled.append(topic_error or "topics")
    else:

        async def handle_topic(claimed: ClaimedJob) -> JobResult:
            payload = claimed.payload
            if payload.get("schema_version") != 1:
                raise ValueError("topic payload schema_version must be 1")
            post_id = payload.get("post_id")
            observed_value = payload.get("observed_at")
            if not isinstance(post_id, str) or not post_id:
                raise ValueError("topic payload requires post_id")
            if not isinstance(observed_value, str):
                raise ValueError("topic payload requires observed_at")
            try:
                observed_at = datetime.fromisoformat(observed_value.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("topic payload observed_at must be ISO-8601") from error
            if observed_at.tzinfo is None:
                raise ValueError("topic payload observed_at must be timezone-aware")
            async with database.transaction() as session:
                topic_result = await topic_processor(
                    session,
                    post_id,
                    observed_at=observed_at,
                )
            return {"cluster_id": _stringify(topic_result.assignment.cluster_id)}

        handlers["topic"] = handle_topic

    usage_processor, usage_error = _load_service_callable(
        "x_virality.services.usage", "process_usage_job"
    )
    usage_refresh: UsageRefresh | None = None
    if usage_processor is None:
        disabled.append(usage_error or "usage")
    else:

        async def run_usage(payload: dict[str, Any]) -> Any:
            if payload.get("schema_version") != 1:
                raise ValueError("usage_refresh payload schema_version must be 1")
            if not isinstance(payload.get("scheduled_hour"), str):
                raise ValueError("usage_refresh payload requires scheduled_hour")
            return await usage_processor(
                settings,
                database,
                x_client,
                payload,
            )

        async def handle_usage(claimed: ClaimedJob) -> JobResult:
            assessment = await run_usage(claimed.payload)
            return {
                "over_soft_limit": bool(getattr(assessment, "over_soft_limit", False)),
                "usage_date": _stringify(getattr(assessment, "usage_date", None)),
                "posts_consumed": int(getattr(assessment, "posts_consumed", 0)),
                "project_cap": getattr(assessment, "project_cap", None),
            }

        async def refresh_usage_state() -> bool:
            scheduled_hour = utcnow().replace(minute=0, second=0, microsecond=0)
            assessment = await run_usage(
                {
                    "schema_version": 1,
                    "scheduled_hour": scheduled_hour.isoformat().replace("+00:00", "Z"),
                }
            )
            return bool(getattr(assessment, "over_soft_limit", False))

        handlers["usage_refresh"] = handle_usage
        usage_refresh = refresh_usage_state

    return DefaultWorkerComponents(
        handlers=handlers,
        usage_refresh=usage_refresh,
        disabled_services=tuple(disabled),
    )


class DurableWorker:
    """Concurrent PostgreSQL-leased job and Discord-outbox dispatcher."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        discord_client: DiscordSender,
        handlers: Mapping[str, JobHandler],
        *,
        worker_id: str | None = None,
        usage_refresh: UsageRefresh | None = None,
        disabled_services: tuple[str, ...] = (),
    ) -> None:
        self.settings = settings
        self.database = database
        self.discord_client = discord_client
        self.handlers = dict(handlers)
        self.worker_id = worker_id or settings.worker_id
        self.usage_refresh = usage_refresh
        self.disabled_services = disabled_services
        self._active_jobs: set[asyncio.Task[None]] = set()
        self._active_outbox: set[asyncio.Task[None]] = set()
        self._usage_soft_limited = False
        self._usage_refresh_failed = False
        self._usage_refreshed_at: datetime | None = None
        self._lifecycle_state = "starting"
        self._last_error_class: str | None = None
        self._stats = {
            "jobs_completed": 0,
            "jobs_deferred": 0,
            "jobs_failed": 0,
            "outbox_sent": 0,
            "outbox_deferred": 0,
            "outbox_failed": 0,
        }
        self._logger = get_logger("worker")

    @property
    def usage_soft_limited(self) -> bool:
        return self._usage_soft_limited

    def _operational_state(self) -> str:
        if self.disabled_services or self._usage_refresh_failed:
            return "degraded"
        return "running"

    def _heartbeat_details(self) -> dict[str, Any]:
        return {
            "active_jobs": len(self._active_jobs),
            "active_outbox": len(self._active_outbox),
            "job_handlers": sorted(self.handlers),
            "disabled_services": list(self.disabled_services),
            "usage_soft_limited": self._usage_soft_limited,
            "usage_refreshed_at": (
                self._usage_refreshed_at.astimezone(UTC).isoformat()
                if self._usage_refreshed_at is not None
                else None
            ),
            "last_error_class": self._last_error_class,
            **self._stats,
        }

    async def _heartbeat(self, state: str | None = None) -> None:
        async with self.database.transaction() as session:
            await upsert_heartbeat(
                session,
                role="worker",
                instance_id=self.worker_id,
                build_sha=self.settings.build_sha,
                state=state or self._lifecycle_state,
                details=self._heartbeat_details(),
            )

    async def _safe_heartbeat(self, state: str | None = None) -> None:
        try:
            await self._heartbeat(state)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._last_error_class = type(error).__name__
            self._logger.warning("worker_heartbeat_failed", error_class=type(error).__name__)

    async def _heartbeat_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=self.settings.heartbeat_interval_seconds
                )
            if not stop.is_set():
                await self._safe_heartbeat()

    async def refresh_usage(self) -> None:
        if self.usage_refresh is None:
            return
        try:
            soft_limited = await self.usage_refresh()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._usage_refresh_failed = True
            self._last_error_class = type(error).__name__
            self._logger.warning("worker_usage_refresh_failed", error_class=type(error).__name__)
            return
        previous = self._usage_soft_limited
        self._usage_soft_limited = soft_limited
        self._usage_refresh_failed = False
        self._usage_refreshed_at = utcnow()
        if soft_limited != previous:
            self._logger.info("worker_usage_gate_changed", soft_limited=soft_limited)
        if self._lifecycle_state in {"running", "degraded"}:
            self._lifecycle_state = self._operational_state()
            await self._safe_heartbeat()

    async def _usage_loop(self, stop: asyncio.Event) -> None:
        await self.refresh_usage()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=_USAGE_REFRESH_SECONDS)
            except TimeoutError:
                await self.refresh_usage()

    async def _wait_for_poll(self, stop: asyncio.Event) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=self.settings.poll_interval_seconds)

    def _claimable_job_kinds(self) -> set[str]:
        kinds = set(self.handlers)
        if self._usage_soft_limited:
            kinds.difference_update(_QUOTA_SENSITIVE_KINDS)
        return kinds

    async def _dispatch_jobs(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            capacity = self.settings.worker_concurrency - len(self._active_jobs)
            kinds = self._claimable_job_kinds()
            if capacity <= 0 or not kinds:
                await self._wait_for_poll(stop)
                continue
            try:
                claimed = await claim_jobs(
                    self.database,
                    worker_id=self.worker_id,
                    lease_seconds=self.settings.job_lease_seconds,
                    limit=capacity,
                    kinds=kinds,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._last_error_class = type(error).__name__
                self._logger.warning("job_claim_failed", error_class=type(error).__name__)
                await self._wait_for_poll(stop)
                continue
            for job in claimed:
                task = asyncio.create_task(self.process_job(job), name=f"job:{job.kind}:{job.id}")
                self._active_jobs.add(task)
                task.add_done_callback(self._active_jobs.discard)
            if len(claimed) < capacity:
                await self._wait_for_poll(stop)

    async def _dispatch_outbox(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            capacity = self.settings.outbox_concurrency - len(self._active_outbox)
            if capacity <= 0:
                await self._wait_for_poll(stop)
                continue
            try:
                claimed = await claim_outbox(
                    self.database,
                    worker_id=self.worker_id,
                    lease_seconds=self.settings.job_lease_seconds,
                    limit=capacity,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._last_error_class = type(error).__name__
                self._logger.warning("outbox_claim_failed", error_class=type(error).__name__)
                await self._wait_for_poll(stop)
                continue
            for item in claimed:
                task = asyncio.create_task(self.process_outbox(item), name=f"outbox:{item.id}")
                self._active_outbox.add(task)
                task.add_done_callback(self._active_outbox.discard)
            if len(claimed) < capacity:
                await self._wait_for_poll(stop)

    async def _active_provider_delay(self, provider: str, endpoint: str) -> float | None:
        async with self.database.session() as session:
            return await active_rate_limit_delay(
                session,
                provider=provider,
                endpoint=endpoint,
            )

    async def _renew_job(self, claimed: ClaimedJob) -> bool:
        async with self.database.transaction() as session:
            return bool(
                await renew_job(
                    session,
                    claimed,
                    worker_id=self.worker_id,
                    lease_seconds=self.settings.job_lease_seconds,
                )
            )

    async def _best_effort_defer_job(
        self,
        claimed: ClaimedJob,
        *,
        error_class: str,
        error_detail: str,
        retry_delay_seconds: float = 0,
    ) -> None:
        try:
            deferred = await asyncio.shield(
                defer_job(
                    self.database,
                    claimed,
                    worker_id=self.worker_id,
                    retry_delay_seconds=retry_delay_seconds,
                    error_class=error_class,
                    error_detail=error_detail,
                )
            )
        except Exception as error:
            self._logger.warning(
                "job_defer_failed",
                job_id=str(claimed.id),
                error_class=type(error).__name__,
            )
        else:
            self._stats["jobs_deferred"] += int(deferred)

    async def _record_x_error(self, error: Exception) -> None:
        if not isinstance(error, XApiError) or error.rate_limit is None:
            return
        try:
            async with self.database.transaction() as session:
                await upsert_rate_limit(session, error.rate_limit)
        except Exception as persistence_error:
            self._logger.warning(
                "x_rate_limit_persist_failed",
                error_class=type(persistence_error).__name__,
            )

    async def process_job(self, claimed: ClaimedJob) -> None:
        if self._usage_soft_limited and claimed.kind in _QUOTA_SENSITIVE_KINDS:
            await self._best_effort_defer_job(
                claimed,
                error_class="UsageSoftLimit",
                error_detail="X usage soft limit pauses bootstrap and recovery work",
                retry_delay_seconds=_USAGE_REFRESH_SECONDS,
            )
            return

        endpoint = _X_ENDPOINT_BY_KIND.get(claimed.kind)
        if endpoint is not None:
            try:
                provider_delay = await self._active_provider_delay("x", endpoint)
            except Exception as error:
                provider_delay = None
                self._logger.warning(
                    "x_rate_limit_gate_read_failed",
                    job_id=str(claimed.id),
                    error_class=type(error).__name__,
                )
            if provider_delay is not None:
                await fail_job(
                    self.database,
                    claimed,
                    worker_id=self.worker_id,
                    error_class="RateLimitGate",
                    error_detail=f"persisted X rate limit is active for {endpoint}",
                    retry_delay_seconds=provider_delay + 0.25,
                    rate_limited=True,
                )
                self._stats["jobs_deferred"] += 1
                return

        handler = self.handlers.get(claimed.kind)
        if handler is None:
            await fail_job(
                self.database,
                claimed,
                worker_id=self.worker_id,
                error_class="UnknownJobKind",
                error_detail=f"no handler is registered for {claimed.kind}",
                permanent=True,
            )
            self._stats["jobs_failed"] += 1
            return

        async def operation() -> JobResult:
            result = await handler(claimed)
            if claimed.kind == "usage_refresh" and "over_soft_limit" in result:
                self._usage_soft_limited = bool(result["over_soft_limit"])
                self._usage_refreshed_at = utcnow()
            completed = await complete_job(
                self.database,
                claimed,
                worker_id=self.worker_id,
                result=result,
            )
            if not completed:
                raise LeaseLostError("job completion was rejected by its fencing token")
            return result

        renewal_interval = max(1.0, self.settings.job_lease_seconds / 3)
        try:
            await run_with_lease_renewal(
                operation(),
                renew=lambda: self._renew_job(claimed),
                interval_seconds=renewal_interval,
            )
        except asyncio.CancelledError:
            await self._best_effort_defer_job(
                claimed,
                error_class="WorkerShutdown",
                error_detail="worker stopped before job completion",
            )
            raise
        except LeaseLostError:
            self._logger.warning("job_lease_lost", job_id=str(claimed.id), kind=claimed.kind)
        except LeaseRenewalError as error:
            await self._best_effort_defer_job(
                claimed,
                error_class=type(error).__name__,
                error_detail=str(error),
            )
        except Exception as error:
            await self._record_x_error(error)
            directive = classify_failure(
                error,
                attempt_number=claimed.attempt_number,
                settings=self.settings,
                jitter_key=str(claimed.id),
            )
            try:
                outcome = await fail_job(
                    self.database,
                    claimed,
                    worker_id=self.worker_id,
                    error_class=directive.error_class,
                    error_detail=directive.error_detail,
                    retry_delay_seconds=directive.retry_delay_seconds,
                    permanent=directive.permanent,
                    rate_limited=directive.rate_limited,
                )
            except Exception as persistence_error:
                self._logger.exception(
                    "job_failure_persist_failed",
                    job_id=str(claimed.id),
                    error_class=type(persistence_error).__name__,
                )
            else:
                self._stats["jobs_failed"] += int(outcome is not None)
                self._logger.warning(
                    "job_failed",
                    job_id=str(claimed.id),
                    kind=claimed.kind,
                    outcome=outcome,
                    error_class=directive.error_class,
                    retry_delay_seconds=directive.retry_delay_seconds,
                )
        else:
            self._stats["jobs_completed"] += 1
            self._logger.info("job_completed", job_id=str(claimed.id), kind=claimed.kind)

    @staticmethod
    def _discord_endpoint(item: ClaimedOutbox) -> str:
        return f"/channels/{item.channel_id}/messages"

    async def _record_discord_observation(
        self,
        *,
        endpoint: str,
        status_code: int,
        limit: int | None,
        remaining: int | None,
        reset_after_seconds: float | None,
    ) -> None:
        reset_at = (
            utcnow() + timedelta(seconds=max(0.0, reset_after_seconds))
            if reset_after_seconds is not None
            else None
        )
        observation = RateLimitObservation(
            endpoint=endpoint,
            status_code=status_code,
            limit=limit,
            remaining=remaining,
            reset_at=reset_at,
            retry_after_seconds=reset_after_seconds,
        )
        async with self.database.transaction() as session:
            await upsert_rate_limit(session, observation, provider="discord")

    async def _record_discord_error(self, endpoint: str, error: DiscordApiError) -> None:
        try:
            await self._record_discord_observation(
                endpoint=endpoint,
                status_code=error.status_code or 0,
                limit=None,
                remaining=0 if error.rate_limited else None,
                reset_after_seconds=error.retry_after_seconds,
            )
        except Exception as persistence_error:
            self._logger.warning(
                "discord_rate_limit_persist_failed",
                error_class=type(persistence_error).__name__,
            )

    async def _best_effort_defer_outbox(
        self,
        item: ClaimedOutbox,
        *,
        error: str,
        retry_delay_seconds: float = 0,
    ) -> None:
        try:
            deferred = await asyncio.shield(
                defer_outbox(
                    self.database,
                    item,
                    worker_id=self.worker_id,
                    retry_delay_seconds=retry_delay_seconds,
                    error=error,
                )
            )
        except Exception as persistence_error:
            self._logger.warning(
                "outbox_defer_failed",
                outbox_id=str(item.id),
                error_class=type(persistence_error).__name__,
            )
        else:
            self._stats["outbox_deferred"] += int(deferred)

    async def process_outbox(self, item: ClaimedOutbox) -> None:
        endpoint = self._discord_endpoint(item)
        try:
            provider_delay = await self._active_provider_delay("discord", endpoint)
        except Exception as error:
            provider_delay = None
            self._logger.warning(
                "discord_rate_limit_gate_read_failed",
                outbox_id=str(item.id),
                error_class=type(error).__name__,
            )
        if provider_delay is not None:
            await fail_outbox(
                self.database,
                item,
                worker_id=self.worker_id,
                error="persisted Discord rate limit is active",
                retry_delay_seconds=provider_delay + 0.25,
                rate_limited=True,
            )
            self._stats["outbox_deferred"] += 1
            return

        delivery_started = False

        async def operation() -> None:
            nonlocal delivery_started
            artifact = (
                await asyncio.to_thread(render_chart_artifact, item.chart_spec)
                if item.chart_spec is not None
                else None
            )
            delivery_started = True
            message = await self.discord_client.send_message(
                item.channel_id,
                item.payload,
                nonce=item.nonce,
                chart_png=artifact.png if artifact is not None else None,
                filename=artifact.filename if artifact is not None else "virality.png",
                attachment_description=(artifact.description if artifact is not None else None),
            )
            rate_limit = message.rate_limit
            await self._record_discord_observation(
                endpoint=endpoint,
                status_code=200,
                limit=rate_limit.limit,
                remaining=rate_limit.remaining,
                reset_after_seconds=rate_limit.reset_after_seconds,
            )
            completed = await complete_outbox(
                self.database,
                item,
                worker_id=self.worker_id,
                discord_message_id=str(message.id),
                provider_response=cast(dict[str, Any], message.raw),
            )
            if not completed:
                raise LeaseLostError("outbox completion was rejected by its fencing token")
            delivery_started = False

        renewal_interval = max(1.0, self.settings.job_lease_seconds / 3)
        try:
            await run_with_lease_renewal(
                operation(),
                renew=lambda: renew_outbox(
                    self.database,
                    item,
                    worker_id=self.worker_id,
                    lease_seconds=self.settings.job_lease_seconds,
                ),
                interval_seconds=renewal_interval,
            )
        except asyncio.CancelledError:
            if delivery_started:
                self._logger.warning("outbox_shutdown_delivery_ambiguous", outbox_id=str(item.id))
            else:
                await self._best_effort_defer_outbox(
                    item, error="worker stopped before Discord delivery started"
                )
            raise
        except LeaseLostError:
            self._logger.warning("outbox_lease_lost", outbox_id=str(item.id))
        except LeaseRenewalError as error:
            if not delivery_started:
                await self._best_effort_defer_outbox(item, error=str(error))
            else:
                self._logger.warning("outbox_lease_renewal_ambiguous", outbox_id=str(item.id))
        except Exception as error:
            if isinstance(error, DiscordApiError):
                await self._record_discord_error(endpoint, error)
            directive = classify_failure(
                error,
                attempt_number=item.attempt_number,
                settings=self.settings,
                jitter_key=str(item.id),
            )
            try:
                outcome = await fail_outbox(
                    self.database,
                    item,
                    worker_id=self.worker_id,
                    error=f"{directive.error_class}: {directive.error_detail}",
                    retry_delay_seconds=directive.retry_delay_seconds,
                    permanent=directive.permanent,
                    rate_limited=directive.rate_limited,
                )
            except Exception as persistence_error:
                self._logger.exception(
                    "outbox_failure_persist_failed",
                    outbox_id=str(item.id),
                    error_class=type(persistence_error).__name__,
                )
            else:
                self._stats["outbox_failed"] += int(outcome is not None)
                self._logger.warning(
                    "outbox_failed",
                    outbox_id=str(item.id),
                    outcome=outcome,
                    error_class=directive.error_class,
                    retry_delay_seconds=directive.retry_delay_seconds,
                )
        else:
            self._stats["outbox_sent"] += 1
            self._logger.info("outbox_sent", outbox_id=str(item.id), alert_id=str(item.alert_id))

    async def _drain_active_work(self) -> None:
        active = set(self._active_jobs) | set(self._active_outbox)
        if not active:
            return
        _, pending = await asyncio.wait(active, timeout=self.settings.worker_shutdown_grace_seconds)
        if pending:
            self._logger.warning("worker_shutdown_grace_expired", active_work=len(pending))
            for task in pending:
                task.cancel()
        await asyncio.gather(*active, return_exceptions=True)

    async def run(self, stop: asyncio.Event) -> None:
        heartbeat_stop = asyncio.Event()
        self._lifecycle_state = self._operational_state()
        await self._safe_heartbeat("starting")
        await self._safe_heartbeat()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(heartbeat_stop), name="worker-heartbeat"
        )
        usage_task = (
            asyncio.create_task(self._usage_loop(stop), name="worker-usage")
            if self.usage_refresh is not None
            else None
        )
        dispatchers = (
            asyncio.create_task(self._dispatch_jobs(stop), name="worker-jobs"),
            asyncio.create_task(self._dispatch_outbox(stop), name="worker-outbox"),
        )
        cancelled = False
        try:
            await stop.wait()
        except asyncio.CancelledError:
            cancelled = True
            stop.set()
        finally:
            self._lifecycle_state = "draining"
            await self._safe_heartbeat()
            await asyncio.gather(*dispatchers, return_exceptions=True)
            if usage_task is not None:
                usage_task.cancel()
                await asyncio.gather(usage_task, return_exceptions=True)
            await self._drain_active_work()
            heartbeat_stop.set()
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            self._lifecycle_state = "stopped"
            await self._safe_heartbeat()
        if cancelled:
            raise asyncio.CancelledError


async def run_job_worker(settings: Settings) -> None:
    """Run the production worker until SIGINT/SIGTERM, then drain leased work."""

    settings.validate_role("worker")
    stop = asyncio.Event()
    database = create_database(settings)
    x_client: XApiClient | None = None
    discord_client: DiscordApiClient | None = None
    logger = get_logger("worker")
    try:
        x_client = XApiClient(settings)
        discord_client = DiscordApiClient(settings)
        rules = load_rules(settings.rules_path)
        components = build_default_worker_components(
            settings,
            database,
            x_client,
            rules,
            worker_id=settings.worker_id,
        )
        if components.disabled_services:
            logger.warning(
                "worker_services_disabled",
                services=list(components.disabled_services),
            )
        worker = DurableWorker(
            settings,
            database,
            discord_client,
            components.handlers,
            usage_refresh=components.usage_refresh,
            disabled_services=components.disabled_services,
        )
        with shutdown_signal_handlers(stop):
            await worker.run(stop)
    finally:
        if discord_client is not None:
            await discord_client.aclose()
        if x_client is not None:
            await x_client.aclose()
        await database.dispose()
