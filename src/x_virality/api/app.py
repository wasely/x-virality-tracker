from __future__ import annotations

import asyncio
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from x_virality.api.interactions import build_interactions_router
from x_virality.config import Settings
from x_virality.db.jobs import upsert_heartbeat
from x_virality.db.session import Database, create_database
from x_virality.logging import get_logger
from x_virality.services.health import collect_health


def _bearer_authorized(request: Request, settings: Settings) -> bool:
    configured = settings.admin_token
    if configured is None:
        return False
    authorization = request.headers.get("authorization", "")
    scheme, separator, credentials = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not credentials:
        return False
    return hmac.compare_digest(credentials, configured.get_secret_value())


def _unauthorized() -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"detail": "valid bearer token required"},
        headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
    )


def create_app(
    settings: Settings | None = None,
    database: Database | None = None,
) -> FastAPI:
    """Create the dedicated X virality operations and interactions app."""

    resolved_settings = settings or Settings()
    resolved_settings.validate_role("api")
    owns_database = database is None
    resolved_database = database or create_database(resolved_settings)
    logger = get_logger("api")

    async def heartbeat_loop(stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                async with resolved_database.transaction() as session:
                    await upsert_heartbeat(
                        session,
                        role="api",
                        instance_id=resolved_settings.worker_id,
                        build_sha=resolved_settings.build_sha,
                        state="running",
                        details={"http": "ready"},
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("api_heartbeat_failed", error_class=type(error).__name__)
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=resolved_settings.heartbeat_interval_seconds
                )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(heartbeat_loop(stop), name="api-heartbeat")
        try:
            yield
        finally:
            stop.set()
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            if owns_database:
                await resolved_database.dispose()

    app = FastAPI(
        title="X Virality API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.database = resolved_database

    @app.get("/health/live", include_in_schema=False)
    async def health_live() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "service": "x-virality-api",
                "build_sha": resolved_settings.build_sha,
            },
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/health/ready", include_in_schema=False)
    async def health_ready() -> JSONResponse:
        try:
            report = await collect_health(resolved_database, resolved_settings)
        except Exception as error:
            logger.exception("readiness_check_failed", error_class=type(error).__name__)
            return JSONResponse(
                status_code=503,
                content={"status": "fail", "ready": False},
                headers={"Cache-Control": "no-store"},
            )
        return JSONResponse(
            status_code=200 if report.ready else 503,
            content=report.as_dict(include_details=False),
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/health/details", include_in_schema=False)
    async def health_details(request: Request) -> JSONResponse:
        if not _bearer_authorized(request, resolved_settings):
            return _unauthorized()
        try:
            report = await collect_health(resolved_database, resolved_settings)
        except Exception as error:
            logger.exception("health_details_failed", error_class=type(error).__name__)
            return JSONResponse(
                status_code=503,
                content={"status": "fail", "ready": False},
                headers={"Cache-Control": "no-store"},
            )
        return JSONResponse(
            status_code=200,
            content=report.as_dict(include_details=True),
            headers={"Cache-Control": "no-store"},
        )

    app.include_router(build_interactions_router(resolved_settings, resolved_database))
    return app
