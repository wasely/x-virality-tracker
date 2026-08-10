# X Virality Tracker

Standalone Python 3.11/PostgreSQL codebase for official X API ingestion, deterministic virality scoring, and Discord alerts.

The schema, provider clients, deterministic scoring/topic pipeline, alert rendering, signed Discord interactions, health API, migrations, stream ingestion, recovery, rule reconciliation, and durable workers are implemented as one standalone service.

## Current architecture

| Area | Current state |
|---|---|
| PostgreSQL/Alembic | Schema, sessions, repositories, job leases, and migration CLI implemented |
| X and Discord | HTTP provider clients implemented |
| Detection | Snapshot, topic, scoring, and alert/outbox pipeline implemented |
| API | Liveness, readiness/details, and signed Discord interaction handling implemented |
| Runtime orchestration | Stream ingestion, job dispatch, rule reconciliation, bounded recovery, and alert delivery implemented |
| Containers | Image builds as a non-root user; Compose runs the production roles with PostgreSQL-backed coordination |

Production flow:

```text
X Filtered Stream / Recent Search / Post lookup
  -> PostgreSQL raw + canonical records
  -> 2/5/10/20/40/60m snapshots
  -> deterministic post/topic scoring
  -> durable Discord outbox
  -> alerts and signed feedback
```

The production roles are `migrate` (one shot), one active `stream`, one or more `worker` replicas, one or more `api` replicas, and one PostgreSQL primary.

## Quick start

Requirements: Docker Compose and Docker Engine.

```bash
cp .env.example .env
# Replace every placeholder. Keep XV_ENVIRONMENT equal to the rules YAML environment.
docker compose --env-file .env config --quiet
docker compose --env-file .env build
docker compose --env-file .env run --rm --no-deps api validate-config
```

`validate-config` validates settings construction and YAML parsing. Provider/database connectivity is verified by role startup and health checks.

## Production deployment

The canonical standalone deployment sequence is:

```bash
docker compose --env-file .env build --pull
docker compose --env-file .env run --rm --no-deps api validate-config
docker compose --env-file .env up -d postgres
docker compose --env-file .env run --rm migrate
docker compose --env-file .env run --rm stream sync-rules --dry-run
# Review the remote rule plan.
docker compose --env-file .env run --rm stream sync-rules
docker compose --env-file .env up -d api stream worker
```

The API binds to `127.0.0.1:${XV_HOST_API_PORT}` by default. Put it behind an HTTPS reverse proxy and configure the dedicated Discord application's Interactions Endpoint URL as:

```text
https://your-host.example/discord/interactions
```

Do not reuse another service's Discord application.

## CLI

```text
migrate, validate-config, sync-rules, stream, worker, api, backfill, healthcheck
```

Run `x-virality --help` for syntax.

## Configuration

- Runtime settings use `XV_` environment variables; detection rules, topics, score weights, and alert thresholds use YAML.
- `.env.example` is the Compose template: its database hostname is `postgres` and rules path is under `/app`.
- For direct host execution, use `localhost` in `XV_DATABASE_URL` and a host path such as `config/rules.example.yml` in `XV_RULES_PATH`.
- Compose requires `.env`; known database-password fallbacks are intentionally disabled.
- `config/rules.example.yml` is mounted read-only into each application container, so a reviewed host edit is visible without rebuilding.
- Never commit `.env`, dumps, tokens, or credential-bearing URLs.

## Development

```bash
uv sync --frozen --extra dev
uv run ruff format --check src tests
uv run ruff check src tests
uv run mypy src
```

Integration tests drop and recreate their configured database. Use a separate disposable database—never the application database:

```bash
createdb x_virality_test
XV_TEST_DATABASE_URL=postgresql+psycopg://x_virality:local_test_password@localhost:5432/x_virality_test \
  uv run pytest
```

PostgreSQL is required; SQLite cannot validate `SKIP LOCKED`, leases, JSONB, or PostgreSQL upserts.

## Detection constraints

The service uses official X endpoints, deterministic calculations, null for unavailable metrics, actual observation times, durable leases/outbox records, and version/hash evidence. Database rows are protected operationally rather than by immutable-table triggers; persisted evidence should not be described as tamper-proof.

See the [operations runbook](docs/RUNBOOK.md) and [scoring specification](docs/SCORING.md).
