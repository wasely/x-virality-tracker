from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from x_virality.config import Settings
from x_virality.domain.models import AlertCandidate
from x_virality.providers.discord import DiscordApiClient, DiscordApiError
from x_virality.services.alerts import prepare_alert, verify_feedback_custom_id
from x_virality.services.charts import render_chart_artifact, render_preview_states


@pytest.fixture
def discord_settings() -> Settings:
    return Settings(
        _env_file=None,
        discord_bot_token="test.bot.token",
        discord_application_id="1234567890",
        discord_public_key="ab" * 32,
        discord_alert_channel_id="9876543210",
        discord_component_secret="component-secret-0123456789abcdef",
        admin_token="admin-secret-0123456789abcdef-00",
    )


def candidate() -> AlertCandidate:
    return AlertCandidate(
        kind="breakout_post",
        entity_id="1234567890123456789",
        score_id=str(uuid4()),
        score="91.25",
        evidence={
            "username": "builder",
            "post_text": "A useful launch @everyone",
            "velocity_percentile": "97",
            "author_lift": "3.2",
            "components": {
                "velocity_percentile": "0.97",
                "topic_spike_percentile": "0.91",
            },
            "snapshots": [
                {"target_age_seconds": 120, "engagement": "10"},
                {"target_age_seconds": 300, "engagement": "30"},
            ],
        },
        policy_hash="a" * 64,
        dedupe_key="breakout:123:rules-v1",
    )


def test_prepare_alert_binds_feedback_and_disables_mentions(
    discord_settings: Settings,
) -> None:
    alert_id = uuid4()
    outbox_id = uuid4()
    prepared = prepare_alert(
        candidate(),
        alert_id=alert_id,
        outbox_id=outbox_id,
        channel_id="9876543210",
        component_secret=discord_settings.discord_component_secret or "",
    )

    assert prepared.payload["allowed_mentions"] == {"parse": []}
    assert prepared.payload["nonce"] == prepared.nonce
    assert len(prepared.nonce) == 25
    embed = prepared.payload["embeds"][0]
    assert embed["title"].startswith("BREAKOUT POST")
    assert "@everyone" in embed["description"]
    buttons = prepared.payload["components"][0]["components"]
    parsed = verify_feedback_custom_id(
        buttons[0]["custom_id"],
        channel_id="9876543210",
        secret=discord_settings.discord_component_secret or "",
    )
    assert parsed is not None
    assert parsed.alert_id == alert_id
    assert parsed.outbox_id == outbox_id
    assert (
        verify_feedback_custom_id(
            buttons[0]["custom_id"],
            channel_id="9876543211",
            secret=discord_settings.discord_component_secret or "",
        )
        is None
    )


def test_chart_renderer_returns_real_png_for_all_states() -> None:
    rendered = render_chart_artifact(
        {
            "title": "Observed engagement",
            "series": [{"name": "Engagement", "points": [["2m", 10], ["5m", 30]]}],
        }
    )
    states = render_preview_states()

    assert rendered.png.startswith(b"\x89PNG\r\n\x1a\n")
    assert rendered.width == 960
    assert rendered.height == 540
    assert {"success", "empty", "error"}.issubset(states)
    assert all(payload.startswith(b"\x89PNG\r\n\x1a\n") for payload in states.values())


@pytest.mark.asyncio
async def test_discord_send_uses_stable_nonce_and_multipart(
    discord_settings: Settings,
) -> None:
    request_body = b""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_body
        request_body = request.content
        assert request.headers["authorization"] == "Bot test.bot.token"
        assert request.url.path == "/api/v10/channels/9876543210/messages"
        return httpx.Response(
            200,
            json={"id": "1111111111", "channel_id": "9876543210"},
            headers={"x-ratelimit-remaining": "4"},
        )

    transport = httpx.MockTransport(handler)
    async with DiscordApiClient(discord_settings, transport=transport) as client:
        message = await client.send_message(
            "9876543210",
            {"embeds": [{"title": "Alert", "image": {"url": "attachment://chart.png"}}]},
            nonce="stable-nonce",
            chart_png=render_preview_states()["success"],
            filename="chart.png",
        )

    assert message.id == "1111111111"
    assert message.rate_limit.remaining == 4
    assert b'"nonce":"stable-nonce"' in request_body
    assert b'"allowed_mentions":{"parse":[]}' in request_body
    assert b"\x89PNG\r\n\x1a\n" in request_body


@pytest.mark.asyncio
async def test_discord_429_is_retryable(discord_settings: Settings) -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(429, json={"message": "limited", "retry_after": 2.5})
    )
    async with DiscordApiClient(discord_settings, transport=transport) as client:
        with pytest.raises(DiscordApiError) as raised:
            await client.send_message(
                "9876543210",
                {"content": "alert"},
                nonce="stable-nonce",
            )

    assert raised.value.rate_limited
    assert not raised.value.permanent
    assert raised.value.retry_after_seconds == 2.5


def test_candidate_score_is_decimal_compatible() -> None:
    assert Decimal(candidate().score) > 85
