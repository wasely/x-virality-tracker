from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
from nacl.signing import SigningKey
from sqlalchemy import select

from x_virality.api.app import create_app
from x_virality.config import Settings
from x_virality.db.models import Alert, AlertFeedback, DiscordOutbox
from x_virality.db.session import Database
from x_virality.services.alerts import build_feedback_custom_id

pytestmark = pytest.mark.integration


def signed_headers(signing_key: SigningKey, body: bytes) -> dict[str, str]:
    timestamp = "1786276800"
    signature = signing_key.sign(timestamp.encode() + body).signature.hex()
    return {
        "content-type": "application/json",
        "x-signature-ed25519": signature,
        "x-signature-timestamp": timestamp,
    }


def interaction_settings(signing_key: SigningKey) -> Settings:
    return Settings(
        _env_file=None,
        discord_application_id="1234567890",
        discord_public_key=signing_key.verify_key.encode().hex(),
        discord_alert_channel_id="9876543210",
        discord_component_secret="component-secret-0123456789abcdef",
        admin_token="admin-secret-0123456789abcdef-00",
    )


async def post_interaction(
    app: object,
    signing_key: SigningKey,
    payload: dict[str, object],
) -> httpx.Response:
    body = json.dumps(payload, separators=(",", ":")).encode()
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="https://api.test") as client:
        return await client.post(
            "/discord/interactions",
            content=body,
            headers=signed_headers(signing_key, body),
        )


@pytest.mark.asyncio
async def test_signed_ping_and_invalid_signature(database: Database) -> None:
    signing_key = SigningKey.generate()
    settings = interaction_settings(signing_key)
    app = create_app(settings, database)
    payload = {"id": "1000000001", "application_id": "1234567890", "type": 1}

    response = await post_interaction(app, signing_key, payload)
    assert response.status_code == 200
    assert response.json() == {"type": 1}

    body = json.dumps(payload).encode()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.test") as client:
        rejected = await client.post(
            "/discord/interactions",
            content=body,
            headers={
                "content-type": "application/json",
                "x-signature-ed25519": "00" * 64,
                "x-signature-timestamp": "1786276800",
            },
        )
    assert rejected.status_code == 401


@pytest.mark.asyncio
async def test_feedback_is_bound_deduplicated_and_updatable(database: Database) -> None:
    signing_key = SigningKey.generate()
    settings = interaction_settings(signing_key)
    alert_id = uuid4()
    outbox_id = uuid4()
    now = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
    async with database.transaction() as session:
        session.add(
            Alert(
                id=alert_id,
                kind="breakout_post",
                entity_type="post",
                entity_id="123",
                score_id=uuid4(),
                score=Decimal("91"),
                evidence={},
                policy_hash="a" * 64,
                dedupe_key="feedback-alert",
                state="sent",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            DiscordOutbox(
                id=outbox_id,
                alert_id=alert_id,
                channel_id="9876543210",
                nonce="feedback-nonce",
                payload={},
                state="sent",
                attempts=0,
                max_attempts=6,
                run_at=now,
                discord_message_id="7777777777",
                sent_at=now,
                created_at=now,
                updated_at=now,
            )
        )

    def payload(interaction_id: str, vote: str) -> dict[str, object]:
        custom_id = build_feedback_custom_id(
            vote,  # type: ignore[arg-type]
            alert_id=alert_id,
            outbox_id=outbox_id,
            channel_id="9876543210",
            secret=settings.discord_component_secret or "",
        )
        return {
            "id": interaction_id,
            "application_id": "1234567890",
            "type": 3,
            "channel_id": "9876543210",
            "member": {"user": {"id": "2222222222"}},
            "message": {"id": "7777777777", "application_id": "1234567890"},
            "data": {"component_type": 2, "custom_id": custom_id},
        }

    app = create_app(settings, database)
    first = await post_interaction(app, signing_key, payload("8000000001", "good"))
    replay = await post_interaction(app, signing_key, payload("8000000001", "good"))
    update = await post_interaction(app, signing_key, payload("8000000002", "bad"))
    stale = await post_interaction(app, signing_key, payload("8000000001", "good"))

    assert first.status_code == replay.status_code == update.status_code == stale.status_code == 200
    assert "saved" in first.json()["data"]["content"]
    assert "already processed" in replay.json()["data"]["content"]
    assert "updated" in update.json()["data"]["content"]
    assert "already processed" in stale.json()["data"]["content"]

    async with database.session() as session:
        rows = list((await session.scalars(select(AlertFeedback))).all())
    assert len(rows) == 1
    assert rows[0].alert_id == alert_id
    assert rows[0].outbox_id == outbox_id
    assert rows[0].discord_user_id == "2222222222"
    assert rows[0].interaction_id == "8000000002"
    assert rows[0].vote == "bad"


@pytest.mark.asyncio
async def test_component_rejects_wrong_message(database: Database) -> None:
    signing_key = SigningKey.generate()
    settings = interaction_settings(signing_key)
    app = create_app(settings, database)
    unknown_id = UUID("00000000-0000-0000-0000-000000000001")
    custom_id = build_feedback_custom_id(
        "good",
        alert_id=unknown_id,
        outbox_id=unknown_id,
        channel_id="9876543210",
        secret=settings.discord_component_secret or "",
    )
    response = await post_interaction(
        app,
        signing_key,
        {
            "id": "8000000001",
            "application_id": "1234567890",
            "type": 3,
            "channel_id": "9876543210",
            "member": {"user": {"id": "2222222222"}},
            "message": {"id": "7777777777", "application_id": "1234567890"},
            "data": {"component_type": 2, "custom_id": custom_id},
        },
    )
    assert response.status_code == 400
