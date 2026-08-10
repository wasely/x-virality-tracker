from __future__ import annotations

import copy
import json
import math
import re
from dataclasses import dataclass
from typing import Any

import httpx

from x_virality.config import Settings

_DISCORD_API_BASE_URL = "https://discord.com/api/v10"
_MAX_ATTACHMENT_BYTES = 8 * 1024 * 1024
_SNOWFLAKE_RE = re.compile(r"[0-9]{1,32}\Z")
_FILENAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class DiscordApiError(RuntimeError):
    """A classified Discord REST failure safe for durable retry handling."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        permanent: bool = False,
        rate_limited: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.permanent = permanent
        self.rate_limited = rate_limited


@dataclass(frozen=True, slots=True)
class DiscordRateLimit:
    limit: int | None
    remaining: int | None
    reset_after_seconds: float | None


@dataclass(frozen=True, slots=True)
class DiscordMessage:
    id: str
    channel_id: str
    raw: dict[str, Any]
    rate_limit: DiscordRateLimit


def _bounded_nonnegative_float(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return max(0.0, parsed)


def _integer_header(headers: httpx.Headers, name: str) -> int | None:
    try:
        return int(headers[name])
    except (KeyError, TypeError, ValueError):
        return None


def _retry_after(response: httpx.Response, payload: object) -> float | None:
    if isinstance(payload, dict):
        parsed = _bounded_nonnegative_float(payload.get("retry_after"))
        if parsed is not None:
            return parsed
    for header in ("retry-after", "x-ratelimit-reset-after"):
        parsed = _bounded_nonnegative_float(response.headers.get(header))
        if parsed is not None:
            return parsed
    return None


def _safe_error_detail(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    code = payload.get("code")
    message = payload.get("message")
    parts: list[str] = []
    if isinstance(code, int | str):
        parts.append(f"code {str(code)[:32]}")
    if isinstance(message, str) and message.strip():
        normalized = " ".join(message.split())
        parts.append(normalized[:300])
    return f" ({'; '.join(parts)})" if parts else ""


class DiscordApiClient:
    """Minimal Bot REST client; it never opens a gateway connection."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        base_url: str = _DISCORD_API_BASE_URL,
    ) -> None:
        token = settings.discord_bot_token
        if token is None:
            raise ValueError("XV_DISCORD_BOT_TOKEN is required")
        normalized_base_url = base_url.rstrip("/")
        if not normalized_base_url.startswith("https://"):
            raise ValueError("Discord API base URL must use HTTPS")
        self._client = httpx.AsyncClient(
            base_url=normalized_base_url,
            headers={
                "authorization": f"Bot {token.get_secret_value()}",
                "accept": "application/json",
                "user-agent": "x-virality/0.1",
            },
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            transport=transport,
        )

    async def __aenter__(self) -> DiscordApiClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def rate_limit(response: httpx.Response) -> DiscordRateLimit:
        return DiscordRateLimit(
            limit=_integer_header(response.headers, "x-ratelimit-limit"),
            remaining=_integer_header(response.headers, "x-ratelimit-remaining"),
            reset_after_seconds=_bounded_nonnegative_float(
                response.headers.get("x-ratelimit-reset-after")
            ),
        )

    @staticmethod
    async def ensure_success(response: httpx.Response) -> dict[str, Any]:
        payload: object
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError):
            payload = None

        if response.is_success:
            if not isinstance(payload, dict):
                raise DiscordApiError(
                    "Discord API returned a non-object success payload",
                    status_code=response.status_code,
                )
            return payload

        status = response.status_code
        retry_after_seconds = _retry_after(response, payload)
        category = (
            "rate limited"
            if status == 429
            else ("client error" if status < 500 else "server error")
        )
        raise DiscordApiError(
            f"Discord API {category}: HTTP {status}{_safe_error_detail(payload)}",
            status_code=status,
            retry_after_seconds=retry_after_seconds,
            permanent=400 <= status < 500 and status != 429,
            rate_limited=status == 429,
        )

    async def send_message(
        self,
        channel_id: str,
        payload: dict[str, Any],
        *,
        nonce: str,
        chart_png: bytes | None = None,
        filename: str = "virality.png",
        attachment_description: str | None = None,
    ) -> DiscordMessage:
        """Create one channel message using multipart ``payload_json``.

        The caller persists ``nonce`` before delivery. Reusing it with
        ``enforce_nonce`` makes an ambiguous retry return Discord's existing message
        instead of creating a duplicate.
        """

        if not _SNOWFLAKE_RE.fullmatch(channel_id):
            raise ValueError("channel_id must be a Discord snowflake")
        if not 1 <= len(nonce) <= 25:
            raise ValueError("nonce must contain 1-25 characters")
        if not _FILENAME_RE.fullmatch(filename):
            raise ValueError("filename contains unsupported characters")
        if chart_png is not None:
            if len(chart_png) > _MAX_ATTACHMENT_BYTES:
                raise ValueError("chart PNG exceeds the 8 MiB attachment limit")
            if not chart_png.startswith(_PNG_SIGNATURE):
                raise ValueError("chart_png is not a PNG image")

        body = copy.deepcopy(payload)
        body["allowed_mentions"] = {"parse": []}
        body["nonce"] = nonce
        body["enforce_nonce"] = True

        if chart_png is not None:
            description = attachment_description
            existing_attachments = body.get("attachments")
            if description is None and isinstance(existing_attachments, list):
                first_attachment = existing_attachments[0] if existing_attachments else None
                if isinstance(first_attachment, dict) and isinstance(
                    first_attachment.get("description"), str
                ):
                    description = first_attachment["description"]
            attachment: dict[str, Any] = {"id": "0", "filename": filename}
            if description:
                attachment["description"] = description[:1024]
            body["attachments"] = [attachment]
        else:
            body.pop("attachments", None)
            embeds = body.get("embeds")
            if isinstance(embeds, list):
                for embed in embeds:
                    if not isinstance(embed, dict):
                        continue
                    image = embed.get("image")
                    if isinstance(image, dict) and str(image.get("url", "")).startswith(
                        "attachment://"
                    ):
                        embed.pop("image", None)

        payload_json = json.dumps(
            body,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        files: dict[str, tuple[str | None, bytes, str]] = {
            "payload_json": (None, payload_json, "application/json")
        }
        if chart_png is not None:
            files["files[0]"] = (filename, chart_png, "image/png")

        try:
            response = await self._client.post(f"/channels/{channel_id}/messages", files=files)
        except httpx.RequestError as error:
            raise DiscordApiError("Discord API request failed") from error

        response_payload = await self.ensure_success(response)
        message_id = response_payload.get("id")
        response_channel_id = response_payload.get("channel_id")
        if not isinstance(message_id, str) or not _SNOWFLAKE_RE.fullmatch(message_id):
            raise DiscordApiError(
                "Discord API response omitted a valid message ID",
                status_code=response.status_code,
            )
        if response_channel_id is not None and str(response_channel_id) != channel_id:
            raise DiscordApiError(
                "Discord API response channel did not match the request",
                status_code=response.status_code,
                permanent=True,
            )
        return DiscordMessage(
            id=message_id,
            channel_id=channel_id,
            raw=response_payload,
            rate_limit=self.rate_limit(response),
        )


DiscordClient = DiscordApiClient
