from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import update

from x_virality.config import RulesConfig, Settings, load_rules
from x_virality.db.models import RuleSet, XRule
from x_virality.db.repositories import record_rule_set, upsert_rule
from x_virality.db.session import Database, create_database, utcnow
from x_virality.domain.models import RateLimitObservation, RemoteRule
from x_virality.logging import get_logger
from x_virality.providers.x_api import XApiClient, XApiError
from x_virality.services.usage import (
    persist_rate_limit_observation,
    persist_x_api_error_rate_limit,
)

_ENVIRONMENT_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
logger = get_logger("services.rules")


class RuleApi(Protocol):
    async def list_rules(self) -> tuple[tuple[RemoteRule, ...], RateLimitObservation]: ...

    async def add_rules(
        self, rules: Iterable[tuple[str, str]]
    ) -> tuple[tuple[RemoteRule, ...], RateLimitObservation]: ...

    async def delete_rules(self, ids: Iterable[str]) -> RateLimitObservation: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DesiredRule:
    local_key: str
    value: str
    tag: str


@dataclass(frozen=True, slots=True)
class UnchangedRule:
    desired: DesiredRule
    remote: RemoteRule


@dataclass(frozen=True, slots=True)
class RuleSyncPlan:
    config_hash: str
    owner_tag_prefix: str
    additions: tuple[DesiredRule, ...]
    deletions: tuple[RemoteRule, ...]
    unchanged: tuple[UnchangedRule, ...]

    @property
    def changed(self) -> bool:
        return bool(self.additions or self.deletions)


@dataclass(frozen=True, slots=True)
class RuleSyncResult:
    plan: RuleSyncPlan
    applied: bool
    rule_set_id: UUID | None
    completed_at: datetime

    @property
    def config_hash(self) -> str:
        return self.plan.config_hash


class RuleSyncVerificationError(RuntimeError):
    pass


async def _check_fence(fence: Callable[[], Awaitable[None]] | None) -> None:
    if fence is not None:
        await fence()


def _validate_rule_tags(rules: RulesConfig, prefix: str) -> None:
    if not _ENVIRONMENT_RE.fullmatch(rules.environment):
        raise ValueError(
            "rule environment must contain only letters, digits, underscores, or hyphens"
        )
    if not _ENVIRONMENT_RE.fullmatch(prefix):
        raise ValueError(
            "managed rule prefix must contain only letters, digits, underscores, or hyphens"
        )
    for rule in rules.rules:
        tag = rules.managed_tag(prefix, rule)
        if len(tag) > 255:
            raise ValueError(f"managed tag for rule {rule.key} exceeds 255 characters")


def build_rule_sync_plan(
    rules: RulesConfig,
    remote_rules: Iterable[RemoteRule],
    *,
    prefix: str,
) -> RuleSyncPlan:
    """Produce a stable reconciliation plan while leaving every unowned rule untouched."""

    _validate_rule_tags(rules, prefix)
    owner_tag_prefix = f"{prefix}:{rules.environment}:"
    desired = tuple(
        sorted(
            (
                DesiredRule(
                    local_key=rule.key,
                    value=rule.query,
                    tag=rules.managed_tag(prefix, rule),
                )
                for rule in rules.rules
                if rule.enabled
            ),
            key=lambda item: (item.local_key, item.tag, item.value),
        )
    )
    owned = tuple(
        sorted(
            (rule for rule in remote_rules if rule.tag.startswith(owner_tag_prefix)),
            key=lambda item: (item.tag, item.value, item.id),
        )
    )
    retained_ids: set[str] = set()
    additions: list[DesiredRule] = []
    unchanged: list[UnchangedRule] = []
    for wanted in desired:
        exact = tuple(
            rule for rule in owned if rule.tag == wanted.tag and rule.value == wanted.value
        )
        if not exact:
            additions.append(wanted)
            continue
        retained = exact[0]
        retained_ids.add(retained.id)
        unchanged.append(UnchangedRule(desired=wanted, remote=retained))
    deletions = tuple(rule for rule in owned if rule.id not in retained_ids)
    return RuleSyncPlan(
        config_hash=rules.canonical_hash(),
        owner_tag_prefix=owner_tag_prefix,
        additions=tuple(additions),
        deletions=deletions,
        unchanged=tuple(unchanged),
    )


def _log_plan(plan: RuleSyncPlan, *, dry_run: bool) -> None:
    logger.info(
        "x_rules_sync_plan",
        dry_run=dry_run,
        config_hash=plan.config_hash,
        owner_tag_prefix=plan.owner_tag_prefix,
        additions=[
            {"local_key": item.local_key, "tag": item.tag, "query": item.value}
            for item in plan.additions
        ],
        deletions=[
            {"remote_rule_id": item.id, "tag": item.tag, "query": item.value}
            for item in plan.deletions
        ],
        unchanged=[
            {
                "local_key": item.desired.local_key,
                "remote_rule_id": item.remote.id,
                "tag": item.remote.tag,
            }
            for item in plan.unchanged
        ],
    )


async def _record_applying(database: Database, rules: RulesConfig) -> UUID:
    async with database.transaction() as session:
        return await record_rule_set(session, rules, state="applying")


async def _mark_rule_set_failed(
    database: Database,
    rule_set_id: UUID,
    error: BaseException,
) -> None:
    async with database.transaction() as session:
        await session.execute(
            update(RuleSet)
            .where(RuleSet.id == rule_set_id)
            .values(
                state="failed",
                validation_error=f"{type(error).__name__}: {error}"[:4000],
            )
        )


async def _persist_applied_rules(
    database: Database,
    *,
    rules: RulesConfig,
    prefix: str,
    final_remote: tuple[RemoteRule, ...],
    rule_set_id: UUID,
) -> None:
    config_hash = rules.canonical_hash()
    remote_by_identity = {(rule.tag, rule.value): rule for rule in final_remote}
    configured_keys = tuple(rule.key for rule in rules.rules)
    now = utcnow()
    async with database.transaction() as session:
        stale = update(XRule)
        if configured_keys:
            stale = stale.where(XRule.local_key.not_in(configured_keys))
        stale_values = {
            "enabled": False,
            "remote_rule_id": None,
            "last_synced_at": now,
            "last_error": "not present in active rule configuration",
            "updated_at": now,
        }
        await session.execute(stale.values(**stale_values))

        for rule in sorted(rules.rules, key=lambda item: item.key):
            tag = rules.managed_tag(prefix, rule)
            remote = remote_by_identity.get((tag, rule.query)) if rule.enabled else None
            await upsert_rule(
                session,
                rule=rule,
                managed_tag=tag,
                config_hash=config_hash,
                remote_rule_id=remote.id if remote else None,
                error=None if remote is not None or not rule.enabled else "remote rule missing",
            )
        await session.execute(
            update(RuleSet)
            .where(RuleSet.id == rule_set_id)
            .values(
                state="applied",
                validation_error=None,
                applied_at=now,
            )
        )


def _verify_final_plan(plan: RuleSyncPlan, expected_count: int) -> None:
    if plan.additions or plan.deletions or len(plan.unchanged) != expected_count:
        raise RuleSyncVerificationError(
            "managed X rules did not converge after reconciliation "
            f"(add={len(plan.additions)}, delete={len(plan.deletions)}, "
            f"unchanged={len(plan.unchanged)}, expected={expected_count})"
        )


async def sync_rules(
    settings: Settings,
    *,
    dry_run: bool = False,
    database: Database | None = None,
    client: RuleApi | None = None,
    fence: Callable[[], Awaitable[None]] | None = None,
) -> RuleSyncResult:
    """Reconcile only this prefix/rule-environment namespace and verify convergence."""

    settings.validate_role("sync-rules")
    rules = load_rules(settings.rules_path)
    _validate_rule_tags(rules, settings.managed_rule_prefix)
    own_database = database is None and not dry_run
    own_client = client is None
    if not dry_run and database is None:
        database = create_database(settings)
    if client is None:
        client = XApiClient(settings)

    rule_set_id: UUID | None = None
    try:
        if not dry_run:
            if database is None:
                raise RuntimeError("database is required when applying X rules")
            await _check_fence(fence)
            rule_set_id = await _record_applying(database, rules)
        try:
            await _check_fence(fence)
            remote, rate_limit = await client.list_rules()
        except XApiError as error:
            if database is not None and not dry_run:
                await persist_x_api_error_rate_limit(database, error)
            raise
        if database is not None and not dry_run:
            await persist_rate_limit_observation(database, rate_limit)
        plan = build_rule_sync_plan(
            rules,
            remote,
            prefix=settings.managed_rule_prefix,
        )
        _log_plan(plan, dry_run=dry_run)
        if dry_run:
            return RuleSyncResult(
                plan=plan,
                applied=False,
                rule_set_id=None,
                completed_at=utcnow(),
            )

        if database is None or rule_set_id is None:
            raise RuntimeError("rule reconciliation lost its database state")
        if plan.deletions:
            try:
                await _check_fence(fence)
                delete_rate = await client.delete_rules(rule.id for rule in plan.deletions)
            except XApiError as error:
                await persist_x_api_error_rate_limit(database, error)
                raise
            await persist_rate_limit_observation(database, delete_rate)
        if plan.additions:
            try:
                await _check_fence(fence)
                _added, add_rate = await client.add_rules(
                    (rule.value, rule.tag) for rule in plan.additions
                )
            except XApiError as error:
                await persist_x_api_error_rate_limit(database, error)
                raise
            await persist_rate_limit_observation(database, add_rate)

        try:
            await _check_fence(fence)
            final_remote, final_rate = await client.list_rules()
        except XApiError as error:
            await persist_x_api_error_rate_limit(database, error)
            raise
        await persist_rate_limit_observation(database, final_rate)
        final_plan = build_rule_sync_plan(
            rules,
            final_remote,
            prefix=settings.managed_rule_prefix,
        )
        expected_count = sum(rule.enabled for rule in rules.rules)
        _verify_final_plan(final_plan, expected_count)
        await _check_fence(fence)
        await _persist_applied_rules(
            database,
            rules=rules,
            prefix=settings.managed_rule_prefix,
            final_remote=final_remote,
            rule_set_id=rule_set_id,
        )
        logger.info(
            "x_rules_sync_applied",
            config_hash=rules.canonical_hash(),
            additions=len(plan.additions),
            deletions=len(plan.deletions),
            unchanged=len(plan.unchanged),
        )
        return RuleSyncResult(
            plan=plan,
            applied=True,
            rule_set_id=rule_set_id,
            completed_at=utcnow(),
        )
    except BaseException as error:
        if database is not None and rule_set_id is not None and not dry_run:
            try:
                await _mark_rule_set_failed(database, rule_set_id, error)
            except Exception as persistence_error:
                logger.error(
                    "x_rules_sync_failure_persistence_failed",
                    error_class=type(persistence_error).__name__,
                    error=str(persistence_error),
                )
        raise
    finally:
        if own_client:
            await client.aclose()
        if own_database and database is not None:
            await database.dispose()


plan_rule_sync = build_rule_sync_plan
