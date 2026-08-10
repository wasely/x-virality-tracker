from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

import httpx

from x_virality.config import Settings
from x_virality.domain.models import (
    AuthorData,
    PostData,
    PostReference,
    PublicMetrics,
    RateLimitObservation,
    RemoteRule,
)

_POST_FIELDS = ",".join(
    (
        "id",
        "text",
        "author_id",
        "created_at",
        "conversation_id",
        "lang",
        "entities",
        "context_annotations",
        "referenced_tweets",
        "public_metrics",
    )
)
_USER_FIELDS = "id,name,username,created_at,verified,public_metrics"
_EXPANSIONS = "author_id"


class XApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        permanent: bool = False,
        rate_limited: bool = False,
        rate_limit: RateLimitObservation | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.permanent = permanent
        self.rate_limited = rate_limited
        self.rate_limit = rate_limit


@dataclass(frozen=True, slots=True)
class XPage:
    posts: tuple[PostData, ...]
    next_token: str | None
    newest_id: str | None
    oldest_id: str | None
    result_count: int
    raw: dict[str, Any]
    rate_limit: RateLimitObservation


@dataclass(frozen=True, slots=True)
class XLookup:
    posts: tuple[PostData, ...]
    errors: tuple[dict[str, Any], ...]
    raw: dict[str, Any]
    rate_limit: RateLimitObservation


@dataclass(frozen=True, slots=True)
class StreamMessage:
    kind: Literal["keepalive", "post"]
    post: PostData | None = None
    raw: dict[str, Any] | None = None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _metric(metrics: dict[str, Any], name: str) -> int | None:
    value = metrics.get(name)
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def _public_metrics(raw: Any) -> PublicMetrics:
    metrics = raw if isinstance(raw, dict) else {}
    return PublicMetrics(
        likes=_metric(metrics, "like_count") or 0,
        reposts=_metric(metrics, "retweet_count") or 0,
        replies=_metric(metrics, "reply_count") or 0,
        quotes=_metric(metrics, "quote_count") or 0,
        bookmarks=_metric(metrics, "bookmark_count"),
        impressions=_metric(metrics, "impression_count"),
    )


def _author(raw: dict[str, Any]) -> AuthorData:
    metrics = raw.get("public_metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    return AuthorData(
        id=str(raw["id"]),
        username=str(raw["username"]) if raw.get("username") else None,
        name=str(raw["name"]) if raw.get("name") else None,
        created_at=_parse_datetime(str(raw.get("created_at") or "")),
        verified=bool(raw["verified"]) if "verified" in raw else None,
        followers=_metric(metrics, "followers_count"),
        following=_metric(metrics, "following_count"),
        posts=_metric(metrics, "tweet_count"),
        listed=_metric(metrics, "listed_count"),
        raw=raw,
    )


def parse_posts(payload: dict[str, Any]) -> tuple[PostData, ...]:
    data = payload.get("data")
    records = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
    includes = payload.get("includes")
    includes = includes if isinstance(includes, dict) else {}
    users = includes.get("users")
    users = users if isinstance(users, list) else []
    authors = {
        str(user["id"]): _author(user)
        for user in users
        if isinstance(user, dict) and user.get("id") is not None
    }

    matching_rules = payload.get("matching_rules")
    matching_rules = matching_rules if isinstance(matching_rules, list) else []
    matched_ids = tuple(
        str(rule["id"])
        for rule in matching_rules
        if isinstance(rule, dict) and rule.get("id") is not None
    )

    parsed: list[PostData] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        post_id = record.get("id")
        author_id = record.get("author_id")
        created_at = _parse_datetime(str(record.get("created_at") or ""))
        if post_id is None or author_id is None or created_at is None:
            raise ValueError("X Post is missing id, author_id, or created_at")
        references_raw = record.get("referenced_tweets")
        references_raw = references_raw if isinstance(references_raw, list) else []
        references = tuple(
            PostReference(type=reference["type"], id=str(reference["id"]))
            for reference in references_raw
            if isinstance(reference, dict)
            and reference.get("type") in {"replied_to", "quoted", "retweeted"}
            and reference.get("id") is not None
        )
        entities = record.get("entities")
        annotations = record.get("context_annotations")
        parsed.append(
            PostData(
                id=str(post_id),
                author_id=str(author_id),
                text=str(record.get("text") or ""),
                created_at=created_at,
                conversation_id=(
                    str(record["conversation_id"]) if record.get("conversation_id") else None
                ),
                language=str(record["lang"]) if record.get("lang") else None,
                references=references,
                entities=entities if isinstance(entities, dict) else {},
                context_annotations=tuple(
                    annotation for annotation in annotations if isinstance(annotation, dict)
                )
                if isinstance(annotations, list)
                else (),
                metrics=_public_metrics(record.get("public_metrics")),
                author=authors.get(str(author_id)),
                matching_rule_ids=matched_ids,
                raw={"data": record, "includes": includes, "matching_rules": matching_rules},
            )
        )
    return tuple(parsed)


class XApiClient:
    def __init__(self, settings: Settings, *, transport: httpx.AsyncBaseTransport | None = None):
        token = settings.x_bearer_token
        if token is None:
            raise ValueError("XV_X_BEARER_TOKEN is required")
        self.base_url = settings.x_api_base_url
        self.timeout_seconds = settings.http_timeout_seconds
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "authorization": f"Bearer {token.get_secret_value()}",
                "user-agent": "x-virality/0.1",
                "accept": "application/json",
            },
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            transport=transport,
        )

    async def __aenter__(self) -> XApiClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def rate_limit(response: httpx.Response, endpoint: str) -> RateLimitObservation:
        headers = response.headers

        def integer(name: str) -> int | None:
            try:
                return int(headers[name])
            except (KeyError, TypeError, ValueError):
                return None

        reset_value = integer("x-rate-limit-reset")
        reset_at = datetime.fromtimestamp(reset_value, UTC) if reset_value else None
        retry_after: float | None = None
        try:
            retry_after = float(headers["retry-after"])
        except (KeyError, TypeError, ValueError):
            if reset_at:
                retry_after = max(0.0, (reset_at - datetime.now(UTC)).total_seconds())
        return RateLimitObservation(
            endpoint=endpoint,
            status_code=response.status_code,
            limit=integer("x-rate-limit-limit"),
            remaining=integer("x-rate-limit-remaining"),
            reset_at=reset_at,
            retry_after_seconds=retry_after,
        )

    @classmethod
    async def ensure_success(cls, response: httpx.Response, endpoint: str) -> None:
        if response.is_success:
            return
        observation = cls.rate_limit(response, endpoint)
        body = await response.aread()
        try:
            detail = json.dumps(json.loads(body), ensure_ascii=False)[:1000]
        except (UnicodeDecodeError, json.JSONDecodeError):
            detail = body.decode(errors="replace")[:1000]
        status = response.status_code
        raise XApiError(
            f"X API {endpoint} returned {status}: {detail}",
            status_code=status,
            retry_after_seconds=observation.retry_after_seconds,
            permanent=status in {400, 401, 403, 404, 422},
            rate_limited=status == 429,
            rate_limit=observation,
        )

    async def list_rules(self) -> tuple[tuple[RemoteRule, ...], RateLimitObservation]:
        endpoint = "/2/tweets/search/stream/rules"
        response = await self._client.get(endpoint)
        await self.ensure_success(response, endpoint)
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        records = data if isinstance(data, list) else []
        rules = tuple(
            RemoteRule(id=str(item["id"]), value=str(item["value"]), tag=str(item.get("tag") or ""))
            for item in records
            if isinstance(item, dict) and item.get("id") and item.get("value")
        )
        return rules, self.rate_limit(response, endpoint)

    async def add_rules(
        self, rules: Iterable[tuple[str, str]]
    ) -> tuple[tuple[RemoteRule, ...], RateLimitObservation]:
        endpoint = "/2/tweets/search/stream/rules"
        response = await self._client.post(
            endpoint, json={"add": [{"value": value, "tag": tag} for value, tag in rules]}
        )
        await self.ensure_success(response, endpoint)
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        records = data if isinstance(data, list) else []
        added = tuple(
            RemoteRule(id=str(item["id"]), value=str(item["value"]), tag=str(item.get("tag") or ""))
            for item in records
            if isinstance(item, dict) and item.get("id") and item.get("value")
        )
        return added, self.rate_limit(response, endpoint)

    async def delete_rules(self, ids: Iterable[str]) -> RateLimitObservation:
        endpoint = "/2/tweets/search/stream/rules"
        response = await self._client.post(endpoint, json={"delete": {"ids": list(ids)}})
        await self.ensure_success(response, endpoint)
        return self.rate_limit(response, endpoint)

    async def recent_search(
        self,
        *,
        query: str,
        start_time: datetime,
        end_time: datetime,
        next_token: str | None = None,
    ) -> XPage:
        endpoint = "/2/tweets/search/recent"
        params = {
            "query": query,
            "start_time": start_time.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "end_time": end_time.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "max_results": "100",
            "tweet.fields": _POST_FIELDS,
            "expansions": _EXPANSIONS,
            "user.fields": _USER_FIELDS,
        }
        if next_token:
            params["next_token"] = next_token
        response = await self._client.get(endpoint, params=params)
        await self.ensure_success(response, endpoint)
        raw = response.json()
        if not isinstance(raw, dict):
            raise XApiError("X Recent Search returned a non-object payload", permanent=True)
        meta_value = raw.get("meta")
        meta: Mapping[str, Any] = (
            cast(Mapping[str, Any], meta_value) if isinstance(meta_value, dict) else {}
        )
        return XPage(
            posts=parse_posts(raw),
            next_token=str(meta["next_token"]) if meta.get("next_token") else None,
            newest_id=str(meta["newest_id"]) if meta.get("newest_id") else None,
            oldest_id=str(meta["oldest_id"]) if meta.get("oldest_id") else None,
            result_count=int(meta.get("result_count") or 0),
            raw=raw,
            rate_limit=self.rate_limit(response, endpoint),
        )

    async def lookup_posts(self, ids: Iterable[str]) -> XLookup:
        post_ids = tuple(dict.fromkeys(str(post_id) for post_id in ids))
        if not post_ids or len(post_ids) > 100:
            raise ValueError("lookup_posts requires 1-100 unique Post IDs")
        endpoint = "/2/tweets"
        response = await self._client.get(
            endpoint,
            params={
                "ids": ",".join(post_ids),
                "tweet.fields": _POST_FIELDS,
                "expansions": _EXPANSIONS,
                "user.fields": _USER_FIELDS,
            },
        )
        await self.ensure_success(response, endpoint)
        raw = response.json()
        if not isinstance(raw, dict):
            raise XApiError("X Post lookup returned a non-object payload", permanent=True)
        errors = raw.get("errors")
        return XLookup(
            posts=parse_posts(raw),
            errors=tuple(item for item in errors if isinstance(item, dict))
            if isinstance(errors, list)
            else (),
            raw=raw,
            rate_limit=self.rate_limit(response, endpoint),
        )

    async def usage(self) -> tuple[dict[str, Any], RateLimitObservation]:
        endpoint = "/2/usage/tweets"
        response = await self._client.get(endpoint)
        await self.ensure_success(response, endpoint)
        payload = response.json()
        if not isinstance(payload, dict):
            raise XApiError("X usage endpoint returned a non-object payload", permanent=True)
        return payload, self.rate_limit(response, endpoint)

    async def stream(self, *, backfill_minutes: int | None = None) -> AsyncIterator[StreamMessage]:
        endpoint = "/2/tweets/search/stream"
        params = {
            "tweet.fields": _POST_FIELDS,
            "expansions": _EXPANSIONS,
            "user.fields": _USER_FIELDS,
        }
        if backfill_minutes is not None:
            params["backfill_minutes"] = str(backfill_minutes)
        timeout = httpx.Timeout(
            connect=self.timeout_seconds,
            read=None,
            write=self.timeout_seconds,
            pool=self.timeout_seconds,
        )
        async with self._client.stream("GET", endpoint, params=params, timeout=timeout) as response:
            await self.ensure_success(response, endpoint)
            async for line in response.aiter_lines():
                if not line.strip():
                    yield StreamMessage(kind="keepalive")
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as error:
                    raise XApiError(f"invalid Filtered Stream JSON: {error}") from error
                if not isinstance(payload, dict):
                    raise XApiError("Filtered Stream emitted a non-object payload")
                posts = parse_posts(payload)
                if not posts:
                    if payload.get("errors"):
                        raise XApiError(f"Filtered Stream emitted errors: {payload['errors']}")
                    continue
                for post in posts:
                    yield StreamMessage(kind="post", post=post, raw=payload)
