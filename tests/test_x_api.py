from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from x_virality.config import Settings
from x_virality.providers.x_api import XApiClient, XApiError, parse_posts


def post_payload(*, post_id: str = "123", matching: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "data": {
            "id": post_id,
            "author_id": "42",
            "text": "A launch https://example.com/news",
            "created_at": "2026-08-09T12:00:00.000Z",
            "conversation_id": post_id,
            "lang": "en",
            "entities": {"hashtags": [{"tag": "AI"}]},
            "referenced_tweets": [{"type": "quoted", "id": "100"}],
            "public_metrics": {
                "like_count": 10,
                "retweet_count": 2,
                "reply_count": 3,
                "quote_count": 1,
                "bookmark_count": 4,
                "impression_count": 500,
            },
        },
        "includes": {
            "users": [
                {
                    "id": "42",
                    "username": "builder",
                    "name": "Builder",
                    "created_at": "2020-01-01T00:00:00.000Z",
                    "verified": False,
                    "public_metrics": {
                        "followers_count": 1000,
                        "following_count": 100,
                        "tweet_count": 500,
                        "listed_count": 10,
                    },
                }
            ]
        },
    }
    if matching:
        payload["matching_rules"] = [{"id": "900", "tag": "xv:test:ai:abc12345"}]
    return payload


def settings() -> Settings:
    return Settings(
        _env_file=None,
        x_bearer_token="secret-test-token",
        x_api_base_url="https://x.test",
    )


def test_parse_posts_preserves_metrics_author_and_references() -> None:
    (post,) = parse_posts(post_payload())

    assert post.id == "123"
    assert post.reference_id("quoted") == "100"
    assert post.matching_rule_ids == ("900",)
    assert post.metrics.bookmarks == 4
    assert post.metrics.impressions == 500
    assert post.author is not None
    assert post.author.followers == 1000


def test_parse_posts_rejects_incomplete_identity() -> None:
    payload = post_payload()
    assert isinstance(payload["data"], dict)
    del payload["data"]["author_id"]

    with pytest.raises(ValueError, match="missing"):
        parse_posts(payload)


@pytest.mark.asyncio
async def test_recent_search_contract_and_rate_limit() -> None:
    seen_request: httpx.Request | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_request
        seen_request = request
        payload = post_payload(matching=False)
        payload["meta"] = {"result_count": 1, "next_token": "next", "newest_id": "123"}
        return httpx.Response(
            200,
            json=payload,
            headers={
                "x-rate-limit-limit": "450",
                "x-rate-limit-remaining": "449",
                "x-rate-limit-reset": "2000000000",
            },
        )

    async with XApiClient(settings(), transport=httpx.MockTransport(handler)) as client:
        page = await client.recent_search(
            query="AI lang:en",
            start_time=datetime(2026, 8, 9, 11, 0, tzinfo=UTC),
            end_time=datetime(2026, 8, 9, 12, 0, tzinfo=UTC),
        )

    assert seen_request is not None
    assert seen_request.headers["authorization"] == "Bearer secret-test-token"
    assert seen_request.url.params["max_results"] == "100"
    assert "public_metrics" in seen_request.url.params["tweet.fields"]
    assert page.result_count == 1
    assert page.next_token == "next"
    assert page.rate_limit.remaining == 449


@pytest.mark.asyncio
async def test_stream_emits_keepalive_and_post() -> None:
    payload = json.dumps(post_payload())

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/2/tweets/search/stream"
        return httpx.Response(200, content=("\n" + payload + "\n").encode())

    messages = []
    async with XApiClient(settings(), transport=httpx.MockTransport(handler)) as client:
        async for message in client.stream():
            messages.append(message)

    assert [message.kind for message in messages] == ["keepalive", "post"]
    assert messages[1].post is not None
    assert messages[1].post.id == "123"


@pytest.mark.asyncio
async def test_rate_limit_error_carries_retry_delay() -> None:
    reset = int((datetime.now(UTC) + timedelta(seconds=30)).timestamp())

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"title": "Too Many Requests"},
            headers={"retry-after": "12.5", "x-rate-limit-reset": str(reset)},
        )

    async with XApiClient(settings(), transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(XApiError) as raised:
            await client.lookup_posts(["123"])

    assert raised.value.rate_limited
    assert not raised.value.permanent
    assert raised.value.retry_after_seconds == 12.5
    assert raised.value.rate_limit is not None
    assert raised.value.rate_limit.status_code == 429


@pytest.mark.asyncio
async def test_stream_classifies_error_body_before_iteration() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b'{"title":"temporarily unavailable"}')

    async with XApiClient(settings(), transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(XApiError, match="temporarily unavailable") as raised:
            async for _message in client.stream():
                pass

    assert raised.value.status_code == 503
    assert not raised.value.permanent


@pytest.mark.asyncio
async def test_lookup_validates_batch_size_before_request() -> None:
    async with XApiClient(
        settings(),
        transport=httpx.MockTransport(lambda _request: httpx.Response(500)),
    ) as client:
        with pytest.raises(ValueError, match="1-100"):
            await client.lookup_posts([])
        with pytest.raises(ValueError, match="1-100"):
            await client.lookup_posts(str(index) for index in range(101))
