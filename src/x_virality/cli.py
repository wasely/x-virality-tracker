from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import uvicorn
from alembic import command
from alembic.config import Config

from x_virality.config import Settings, load_rules
from x_virality.logging import configure_logging, get_logger
from x_virality.runtime import configure_event_loop_policy

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="x-virality")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("migrate", help="upgrade PostgreSQL to the latest schema")

    sync = subcommands.add_parser("sync-rules", help="reconcile managed Filtered Stream rules")
    sync.add_argument("--dry-run", action="store_true")

    subcommands.add_parser("stream", help="run the singleton Filtered Stream collector")
    subcommands.add_parser("worker", help="run durable background jobs")
    subcommands.add_parser("api", help="run health and Discord interactions API")

    backfill = subcommands.add_parser("backfill", help="enqueue a bounded Recent Search recovery")
    backfill.add_argument("--since", help="inclusive ISO-8601 timestamp")
    backfill.add_argument("--until", help="exclusive ISO-8601 timestamp")

    health = subcommands.add_parser("healthcheck", help="check a service heartbeat")
    health.add_argument("--role", required=True, choices=["api", "stream", "worker"])

    subcommands.add_parser("validate-config", help="validate environment and rule configuration")
    return parser


def _migrate(settings: Settings) -> None:
    config = Config(str(_PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(_PROJECT_ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    command.upgrade(config, "head")


async def _run_async(args: argparse.Namespace, settings: Settings) -> None:
    if args.command == "sync-rules":
        from x_virality.services.rules import sync_rules

        await sync_rules(settings, dry_run=args.dry_run)
    elif args.command == "stream":
        from x_virality.workers.stream import run_stream_worker

        await run_stream_worker(settings)
    elif args.command == "worker":
        from x_virality.workers.jobs import run_job_worker

        await run_job_worker(settings)
    elif args.command == "backfill":
        from x_virality.services.recovery import enqueue_manual_backfill

        await enqueue_manual_backfill(settings, since=args.since, until=args.until)
    elif args.command == "healthcheck":
        from x_virality.services.health import heartbeat_is_fresh

        if not await heartbeat_is_fresh(settings, args.role):
            raise RuntimeError(f"{args.role} heartbeat is stale")
    else:
        raise RuntimeError(f"unsupported async command: {args.command}")


def main() -> None:
    configure_event_loop_policy()
    args = _parser().parse_args()
    settings = Settings()
    configure_logging(settings.log_level)
    logger = get_logger("cli")

    try:
        if args.command == "validate-config":
            rules = load_rules(settings.rules_path)
            logger.info(
                "configuration_valid",
                environment=settings.environment,
                enabled_rules=sum(rule.enabled for rule in rules.rules),
                config_hash=rules.canonical_hash(),
            )
            return
        if args.command == "migrate":
            settings.validate_role("migrate")
            _migrate(settings)
            return
        if args.command == "api":
            settings.validate_role("api")
            uvicorn.run(
                "x_virality.api.app:create_app",
                factory=True,
                host=settings.api_host,
                port=settings.api_port,
                log_config=None,
            )
            return

        settings.validate_role(args.command)
        asyncio.run(_run_async(args, settings))
    except (RuntimeError, ValueError) as error:
        logger.error("command_failed", command=args.command, error=str(error))
        raise SystemExit(2) from error
    except KeyboardInterrupt:
        logger.info("command_interrupted", command=args.command)
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
