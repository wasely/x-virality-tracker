from __future__ import annotations

import asyncio
import hmac
import json
import re
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import uuid4

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from x_virality.config import Settings
from x_virality.db.models import Alert, AlertFeedback, DiscordOutbox
from x_virality.db.session import Database, utcnow
from x_virality.logging import get_logger
from x_virality.services.alerts import FeedbackComponent, verify_feedback_custom_id

_INTERACTION_PING = 1
_INTERACTION_MESSAGE_COMPONENT = 3
_CALLBACK_PONG = 1
_CALLBACK_CHANNEL_MESSAGE = 4
_EPHEMERAL_FLAG = 1 << 6
_BUTTON_COMPONENT = 2
_MAX_BODY_BYTES = 64 * 1024
_COMPONENT_DEADLINE_SECONDS = 2.5
_SNOWFLAKE_RE = re.compile(r"[0-9]{5,32}\Z")
_HEX_SIGNATURE_RE = re.compile(r"[0-9a-fA-F]{128}\Z")


@dataclass(frozen=True, slots=True)
class FeedbackWriteResult:
    outcome: Literal["recorded", "updated", "unchanged", "duplicate"]
    vote: Literal["good", "bad"]


def verify_discord_signature(
    *,
    public_key_hex: str,
    signature_hex: str,
    timestamp: str,
    body: bytes,
) -> bool:
    """Verify Discord's Ed25519 signature over timestamp plus untouched body."""

    if not _HEX_SIGNATURE_RE.fullmatch(signature_hex) or not timestamp or len(timestamp) > 64:
        return False
    try:
        signed = timestamp.encode("ascii") + body
        VerifyKey(bytes.fromhex(public_key_hex)).verify(signed, bytes.fromhex(signature_hex))
    except (BadSignatureError, UnicodeEncodeError, ValueError):
        return False
    return True


def _json_error(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


def _ephemeral(content: str) -> JSONResponse:
    return JSONResponse(
        status_code=200,
        content={
            "type": _CALLBACK_CHANNEL_MESSAGE,
            "data": {
                "content": content,
                "flags": _EPHEMERAL_FLAG,
                "allowed_mentions": {"parse": []},
            },
        },
    )


def _object(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _snowflake(value: object) -> str | None:
    parsed = str(value) if isinstance(value, str | int) else ""
    return parsed if _SNOWFLAKE_RE.fullmatch(parsed) else None


def _user_id(payload: dict[str, Any]) -> str | None:
    member = _object(payload.get("member"))
    member_user = _object(member.get("user"))
    direct_user = _object(payload.get("user"))
    return _snowflake(member_user.get("id") or direct_user.get("id"))


def _channel_id(payload: dict[str, Any]) -> str | None:
    channel = _object(payload.get("channel"))
    return _snowflake(payload.get("channel_id") or channel.get("id"))


def _message_id(payload: dict[str, Any]) -> str | None:
    return _snowflake(_object(payload.get("message")).get("id"))


def _validate_application(payload: dict[str, Any], application_id: str) -> bool:
    received = _snowflake(payload.get("application_id"))
    if received is None or not hmac.compare_digest(received, application_id):
        return False
    message_application = _object(payload.get("message")).get("application_id")
    if message_application is None:
        return True
    parsed_message_application = _snowflake(message_application)
    return parsed_message_application is not None and hmac.compare_digest(
        parsed_message_application, application_id
    )


async def _record_feedback(
    database: Database,
    *,
    component: FeedbackComponent,
    interaction_id: str,
    channel_id: str,
    message_id: str,
    user_id: str,
) -> FeedbackWriteResult | None:
    now = utcnow()
    async with database.transaction() as session:
        outbox = await session.get(DiscordOutbox, component.outbox_id)
        if (
            outbox is None
            or outbox.alert_id != component.alert_id
            or outbox.channel_id != channel_id
            or outbox.discord_message_id != message_id
            or outbox.state != "sent"
        ):
            return None
        alert = await session.get(Alert, component.alert_id)
        if alert is None or alert.id != outbox.alert_id:
            return None

        duplicate = await session.scalar(
            select(AlertFeedback).where(AlertFeedback.interaction_id == interaction_id)
        )
        if duplicate is not None:
            if (
                duplicate.alert_id != component.alert_id
                or duplicate.outbox_id != component.outbox_id
                or duplicate.discord_user_id != user_id
                or duplicate.vote != component.vote
            ):
                return None
            return FeedbackWriteResult(outcome="duplicate", vote=component.vote)

        current = await session.scalar(
            select(AlertFeedback)
            .where(
                AlertFeedback.alert_id == component.alert_id,
                AlertFeedback.discord_user_id == user_id,
            )
            .with_for_update()
        )
        if current is None:
            outcome: Literal["recorded", "updated", "unchanged", "duplicate"] = "recorded"
        elif int(interaction_id) <= int(current.interaction_id):
            return FeedbackWriteResult(outcome="duplicate", vote=component.vote)
        elif current.vote == component.vote:
            outcome = "unchanged"
        else:
            outcome = "updated"

        statement = (
            pg_insert(AlertFeedback)
            .values(
                id=uuid4(),
                alert_id=component.alert_id,
                outbox_id=component.outbox_id,
                interaction_id=interaction_id,
                discord_user_id=user_id,
                vote=component.vote,
                first_voted_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=[AlertFeedback.alert_id, AlertFeedback.discord_user_id],
                set_={
                    "outbox_id": component.outbox_id,
                    "interaction_id": interaction_id,
                    "vote": component.vote,
                    "updated_at": now,
                },
            )
        )
        try:
            async with session.begin_nested():
                await session.execute(statement)
        except IntegrityError:
            concurrent_duplicate = await session.scalar(
                select(AlertFeedback).where(AlertFeedback.interaction_id == interaction_id)
            )
            if concurrent_duplicate is None:
                raise
            if (
                concurrent_duplicate.alert_id != component.alert_id
                or concurrent_duplicate.discord_user_id != user_id
            ):
                return None
            outcome = "duplicate"
        return FeedbackWriteResult(outcome=outcome, vote=component.vote)


def _feedback_message(result: FeedbackWriteResult) -> str:
    label = "Good" if result.vote == "good" else "Bad"
    if result.outcome == "recorded":
        return f"Feedback saved: {label}."
    if result.outcome == "updated":
        return f"Feedback updated: {label}."
    if result.outcome == "unchanged":
        return f"Feedback already set to {label}."
    return "This interaction was already processed."


def build_interactions_router(settings: Settings, database: Database) -> APIRouter:
    application_id = settings.discord_application_id
    public_key = settings.discord_public_key
    component_secret = settings.discord_component_secret
    if application_id is None or public_key is None or component_secret is None:
        raise ValueError("Discord application ID, public key, and component secret are required")
    public_key_hex = public_key.get_secret_value()
    logger = get_logger("discord_interactions")
    router = APIRouter()

    @router.post("/discord/interactions", include_in_schema=False)
    async def discord_interactions(request: Request) -> JSONResponse:
        content_length = request.headers.get("content-length")
        try:
            if content_length is not None and int(content_length) > _MAX_BODY_BYTES:
                return _json_error(413, "interaction body too large")
        except ValueError:
            return _json_error(400, "invalid content length")

        body = await request.body()
        if len(body) > _MAX_BODY_BYTES:
            return _json_error(413, "interaction body too large")
        signature = request.headers.get("x-signature-ed25519", "")
        timestamp = request.headers.get("x-signature-timestamp", "")
        if not verify_discord_signature(
            public_key_hex=public_key_hex,
            signature_hex=signature,
            timestamp=timestamp,
            body=body,
        ):
            return _json_error(401, "invalid request signature")

        try:
            raw_payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _json_error(400, "invalid interaction JSON")
        if not isinstance(raw_payload, dict):
            return _json_error(400, "interaction payload must be an object")
        payload = cast(dict[str, Any], raw_payload)
        if not _validate_application(payload, application_id):
            return _json_error(401, "invalid interaction application")

        interaction_type = payload.get("type")
        if interaction_type == _INTERACTION_PING:
            return JSONResponse(status_code=200, content={"type": _CALLBACK_PONG})
        if interaction_type != _INTERACTION_MESSAGE_COMPONENT:
            return _json_error(400, "unsupported interaction type")

        interaction_id = _snowflake(payload.get("id"))
        channel_id = _channel_id(payload)
        message_id = _message_id(payload)
        user_id = _user_id(payload)
        data = _object(payload.get("data"))
        custom_id = data.get("custom_id")
        if (
            interaction_id is None
            or channel_id is None
            or message_id is None
            or user_id is None
            or data.get("component_type") != _BUTTON_COMPONENT
            or not isinstance(custom_id, str)
        ):
            return _json_error(400, "invalid component interaction")
        configured_channel = settings.discord_alert_channel_id
        if configured_channel is not None and not hmac.compare_digest(
            channel_id, configured_channel
        ):
            return _json_error(400, "invalid component interaction")

        component = verify_feedback_custom_id(
            custom_id,
            channel_id=channel_id,
            secret=component_secret,
        )
        if component is None:
            return _json_error(400, "invalid component interaction")

        try:
            async with asyncio.timeout(_COMPONENT_DEADLINE_SECONDS):
                result = await _record_feedback(
                    database,
                    component=component,
                    interaction_id=interaction_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    user_id=user_id,
                )
        except TimeoutError:
            logger.warning("feedback_deadline_exceeded")
            return _ephemeral("Feedback was not saved in time. Please try again.")
        except Exception as error:
            logger.exception("feedback_persistence_failed", error_class=type(error).__name__)
            return _ephemeral("Feedback is temporarily unavailable. Please try again.")
        if result is None:
            return _json_error(400, "invalid component interaction")
        return _ephemeral(_feedback_message(result))

    return router
