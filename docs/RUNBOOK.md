# Operations Runbook

## Prerequisites

- Always-on host with Docker Engine, Docker Compose, and persistent storage.
- PostgreSQL 16 backup destination.
- Official X API project with Filtered Stream, Recent Search, Post lookup, and sufficient usage access.
- Dedicated Discord application/bot and HTTPS reverse proxy.

## First deployment

```bash
cp .env.example .env
# Replace every placeholder. Keep XV_ENVIRONMENT equal to the rules YAML environment.
docker compose --env-file .env config --quiet
docker compose --env-file .env build --pull
docker compose --env-file .env run --rm --no-deps api validate-config
docker compose --env-file .env up -d postgres
docker compose --env-file .env run --rm migrate
docker compose --env-file .env run --rm stream sync-rules --dry-run
# Review adds/deletes/unchanged, then apply.
docker compose --env-file .env run --rm stream sync-rules
docker compose --env-file .env up -d api stream worker
```

Compose requires `.env`; known database-password fallbacks are disabled. Use the same strong password in `XV_POSTGRES_PASSWORD` and `XV_DATABASE_URL`.

Verify:

```bash
curl -fsS http://127.0.0.1:8080/health/live
curl -fsS http://127.0.0.1:8080/health/ready
read -rsp "Admin token: " XV_ADMIN_TOKEN && printf '\n'
curl -fsS -H "Authorization: Bearer $XV_ADMIN_TOKEN" \
  http://127.0.0.1:8080/health/details
unset XV_ADMIN_TOKEN
```

Compose `--env-file` does not export values into the operator shell. Keep `/health/details` private and inspect its JSON status.

## Discord setup

1. Create a dedicated application; do not reuse another service's application.
2. Put its bot token, application ID/public key, channel ID, and generated component/admin secrets in `.env`.
3. Grant only View Channel, Send Messages, Embed Links, and Attach Files.
4. Expose `api` through HTTPS and set the Interactions Endpoint URL to `https://host/discord/interactions`.

No gateway connection or slash-command registration is required.

## Rule changes

`config/rules.example.yml` is mounted read-only at `/app/config/rules.example.yml`; reviewed host edits are visible without rebuilding. Keep its `environment` equal to `XV_ENVIRONMENT`.

```bash
docker compose --env-file .env run --rm --no-deps api validate-config
docker compose --env-file .env run --rm stream sync-rules --dry-run
# Review, then apply.
docker compose --env-file .env run --rm stream sync-rules
```

The service only modifies remote rules under its configured prefix/environment. Never delete unrelated rules to make room.

## Bounded recovery

The stream role schedules overlapping Recent Search recovery after disconnects. Operators can enqueue a bounded window:

```bash
docker compose --env-file .env run --rm stream backfill \
  --since 2026-08-09T10:00:00Z \
  --until 2026-08-09T11:00:00Z
```

X limits Recent Search availability. Enterprise stream backfill is used only when explicitly enabled and supported by the project tier.

## Deploy and migrate

Back up first, then:

```bash
docker compose --env-file .env build --pull
docker compose --env-file .env run --rm migrate
docker compose --env-file .env up -d api stream worker
```

Migrations are forward-applied by the one-shot role. Test downgrades on a restored copy.

## Backup and restore

```bash
docker compose --env-file .env exec -T postgres \
  sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > x-virality.dump
```

Restore during a maintenance window:

```bash
docker compose --env-file .env stop api stream worker
docker compose --env-file .env exec -T postgres \
  sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' \
  < x-virality.dump
docker compose --env-file .env up -d api stream worker
```

Protect dumps and test restoration regularly.

## Incident checks

- Stream stale: inspect logs/details, X auth/rates/usage, and confirm one lease owner.
- Job backlog: inspect oldest due/dead jobs and rate resets; replicas do not add X quota.
- Discord failure: check membership, permissions, outbox retry/dead counts, and retry-after state.
- Interaction rejection: preserve raw body and signature/timestamp headers; verify clock skew.

## Secret rotation

- X bearer token: recreate stream/workers.
- Discord bot token: recreate workers.
- Discord public key/admin token: recreate API.
- Component secret: recreate API/workers; existing component signatures become invalid.
- Database credentials: rotate PostgreSQL and every role atomically.

Never print tokens or credential-bearing database URLs in logs or incident reports.
