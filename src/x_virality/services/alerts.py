from __future__ import annotations

import base64
import hashlib
import hmac
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from urllib.parse import quote
from uuid import UUID, uuid4

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from x_virality.config import Settings
from x_virality.db.models import Alert, DiscordOutbox
from x_virality.db.session import utcnow
from x_virality.domain.models import AlertCandidate
from x_virality.services.charts import chart_description

FeedbackVote = Literal["good", "bad"]

_COMPONENT_VERSION = "xv1"
_COMPONENT_ID_RE = re.compile(
    r"xv1:(?P<vote>[gb]):(?P<alert>[0-9a-f]{32}):(?P<outbox>[0-9a-f]{32}):"
    r"(?P<mac>[A-Za-z0-9_-]{16})\Z"
)
_SNOWFLAKE_RE = re.compile(r"[0-9]{5,32}\Z")
_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{1,15}\Z")
_DISCORD_EMBED_TOTAL = 6000
_CHART_FILENAME = "virality.png"


@dataclass(frozen=True, slots=True)
class FeedbackComponent:
    vote: FeedbackVote
    alert_id: UUID
    outbox_id: UUID


@dataclass(frozen=True, slots=True)
class PreparedAlert:
    alert_id: UUID
    outbox_id: UUID
    nonce: str
    payload: dict[str, Any]
    chart_spec: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AlertOutboxResult:
    alert_id: UUID
    outbox_id: UUID
    created: bool


def _secret_bytes(secret: SecretStr | str | bytes) -> bytes:
    if isinstance(secret, SecretStr):
        value = secret.get_secret_value().encode("utf-8")
    elif isinstance(secret, str):
        value = secret.encode("utf-8")
    else:
        value = secret
    if len(value) < 32:
        raise ValueError("component secret must contain at least 32 bytes")
    return value


def _component_mac(
    secret: SecretStr | str | bytes,
    *,
    vote: FeedbackVote,
    alert_id: UUID,
    outbox_id: UUID,
    channel_id: str,
) -> str:
    message = (f"{_COMPONENT_VERSION}|{vote}|{alert_id.hex}|{outbox_id.hex}|{channel_id}").encode(
        "ascii"
    )
    digest = hmac.new(_secret_bytes(secret), message, hashlib.sha256).digest()[:12]
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def build_feedback_custom_id(
    vote: FeedbackVote,
    *,
    alert_id: UUID,
    outbox_id: UUID,
    channel_id: str,
    secret: SecretStr | str | bytes,
) -> str:
    if vote not in {"good", "bad"}:
        raise ValueError("vote must be good or bad")
    if not _SNOWFLAKE_RE.fullmatch(channel_id):
        raise ValueError("channel_id must be a Discord snowflake")
    code = "g" if vote == "good" else "b"
    signature = _component_mac(
        secret,
        vote=vote,
        alert_id=alert_id,
        outbox_id=outbox_id,
        channel_id=channel_id,
    )
    custom_id = f"{_COMPONENT_VERSION}:{code}:{alert_id.hex}:{outbox_id.hex}:{signature}"
    if len(custom_id) > 100:
        raise RuntimeError("feedback component ID exceeds Discord's limit")
    return custom_id


def verify_feedback_custom_id(
    custom_id: str,
    *,
    channel_id: str,
    secret: SecretStr | str | bytes,
) -> FeedbackComponent | None:
    if len(custom_id) > 100 or not _SNOWFLAKE_RE.fullmatch(channel_id):
        return None
    match = _COMPONENT_ID_RE.fullmatch(custom_id)
    if match is None:
        return None
    vote: FeedbackVote = "good" if match["vote"] == "g" else "bad"
    alert_id = UUID(hex=match["alert"])
    outbox_id = UUID(hex=match["outbox"])
    expected = _component_mac(
        secret,
        vote=vote,
        alert_id=alert_id,
        outbox_id=outbox_id,
        channel_id=channel_id,
    )
    if not hmac.compare_digest(expected, match["mac"]):
        return None
    return FeedbackComponent(vote=vote, alert_id=alert_id, outbox_id=outbox_id)


def stable_discord_nonce(alert_id: UUID, outbox_id: UUID, channel_id: str) -> str:
    if not _SNOWFLAKE_RE.fullmatch(channel_id):
        raise ValueError("channel_id must be a Discord snowflake")
    source = f"xv|{alert_id.hex}|{outbox_id.hex}|{channel_id}".encode("ascii")
    return hashlib.sha256(source).hexdigest()[:25]


def x_post_url(post_id: object, username: object | None = None) -> str:
    safe_post_id = quote(str(post_id), safe="")
    candidate_username = str(username or "").lstrip("@")
    if _USERNAME_RE.fullmatch(candidate_username):
        return f"https://x.com/{candidate_username}/status/{safe_post_id}"
    return f"https://x.com/i/web/status/{safe_post_id}"


def _safe_text(value: object, *, limit: int, fallback: str = "") -> str:
    if value is None:
        return fallback
    normalized = unicodedata.normalize("NFKC", str(value))
    cleaned = "\n".join(
        " ".join(
            "".join(character if character.isprintable() else " " for character in line).split()
        )
        for line in normalized.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ).strip()
    if not cleaned:
        return fallback
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: max(0, limit - 1)].rstrip()}…"


def _bounded_identifier(value: object, *, name: str, limit: int) -> str:
    text = str(value)
    if not text or len(text) > limit or any(not character.isprintable() for character in text):
        raise ValueError(f"{name} must contain 1-{limit} printable characters")
    return text


def _escape_markdown(value: object, *, limit: int) -> str:
    text = _safe_text(value, limit=limit * 2)
    for character in ("\\", "`", "*", "_", "~", "|", "[", "]", "(", ")"):
        text = text.replace(character, f"\\{character}")
    return _safe_text(text, limit=limit)


def _mapping(value: object) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[object]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return cast(Sequence[object], value)
    return ()


def _first(evidence: Mapping[str, Any], *keys: str) -> object | None:
    for key in keys:
        value = evidence.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _format_score(score: Decimal) -> str:
    return f"{score.quantize(Decimal('0.1'))}/100"


def _ordinal(value: Decimal) -> str:
    rounded = int(value.quantize(Decimal("1")))
    suffix = "th"
    if rounded % 100 not in {11, 12, 13}:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rounded % 10, "th")
    return f"{rounded}{suffix} percentile"


def _format_driver(name: str, value: object) -> str:
    parsed = _decimal(value)
    if parsed is None:
        return _safe_text(value, limit=80, fallback="Unavailable")
    normalized = name.lower()
    if "percentile" in normalized or normalized.endswith("_pct"):
        if Decimal("0") <= parsed <= Decimal("1"):
            parsed *= Decimal("100")
        return _ordinal(parsed)
    if any(token in normalized for token in ("lift", "spike", "acceleration", "growth")):
        return f"{parsed.quantize(Decimal('0.1'))}x"
    if any(token in normalized for token in ("ratio", "diversity", "concentration", "share")):
        if Decimal("0") <= parsed <= Decimal("1"):
            parsed *= Decimal("100")
        return f"{parsed.quantize(Decimal('0.1'))}%"
    return f"{parsed.quantize(Decimal('0.01')).normalize()}"


def _label(key: object) -> str:
    return _safe_text(str(key).replace("_", " ").strip().title(), limit=64, fallback="Signal")


def _driver_lines(evidence: Mapping[str, Any]) -> list[str]:
    explicit = _first(evidence, "score_drivers", "drivers", "top_drivers")
    lines: list[str] = []
    if isinstance(explicit, Mapping):
        for name, value in list(explicit.items())[:5]:
            lines.append(f"- {_label(name)}: {_format_driver(str(name), value)}")
    else:
        for item in _sequence(explicit)[:5]:
            if isinstance(item, Mapping):
                name = item.get("name", item.get("key", "Signal"))
                value = item.get("value", item.get("contribution", item.get("score")))
                lines.append(f"- {_label(name)}: {_format_driver(str(name), value)}")
            elif item is not None:
                lines.append(f"- {_safe_text(item, limit=100)}")
    if lines:
        return lines

    components = _mapping(evidence.get("components"))
    preferred = (
        "velocity_percentile",
        "acceleration_percentile",
        "author_lift",
        "topic_spike",
        "conversation_spread_percentile",
        "unique_authors",
        "original_content_ratio",
        "text_diversity",
    )
    ordered_keys = [key for key in preferred if key in components]
    ordered_keys.extend(sorted(str(key) for key in components if str(key) not in ordered_keys))
    for key in ordered_keys[:5]:
        lines.append(f"- {_label(key)}: {_format_driver(key, components[key])}")
    return lines


def _field(name: str, value: object, *, inline: bool = False) -> dict[str, Any]:
    return {
        "name": _safe_text(name, limit=80, fallback="Signal"),
        "value": _safe_text(value, limit=550, fallback="Unavailable"),
        "inline": inline,
    }


def _timestamp(evidence: Mapping[str, Any]) -> str | None:
    value = _first(evidence, "as_of", "observed_at", "created_at", "posted_at")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _top_post_lines(evidence: Mapping[str, Any]) -> list[str]:
    raw_posts = _first(evidence, "top_driving_posts", "top_posts", "driving_posts")
    lines: list[str] = []
    for index, item in enumerate(_sequence(raw_posts)[:3], start=1):
        if isinstance(item, Mapping):
            post_id = item.get("id", item.get("post_id"))
            if post_id is None:
                continue
            username = item.get("username", item.get("author_username"))
            label = _escape_markdown(
                item.get("label", item.get("text", f"Driving post {index}")), limit=70
            )
            lines.append(f"[{label}]({x_post_url(post_id, username)})")
        elif item is not None:
            lines.append(f"[Driving post {index}]({x_post_url(item)})")
    return lines


def _embed_character_count(embed: Mapping[str, Any]) -> int:
    total = len(str(embed.get("title", ""))) + len(str(embed.get("description", "")))
    author = _mapping(embed.get("author"))
    footer = _mapping(embed.get("footer"))
    total += len(str(author.get("name", ""))) + len(str(footer.get("text", "")))
    for field in _sequence(embed.get("fields")):
        if isinstance(field, Mapping):
            total += len(str(field.get("name", ""))) + len(str(field.get("value", "")))
    return total


def _fit_embed(embed: dict[str, Any]) -> dict[str, Any]:
    if _embed_character_count(embed) <= _DISCORD_EMBED_TOTAL:
        return embed
    embed["description"] = _safe_text(embed.get("description"), limit=500)
    fields = embed.get("fields")
    if isinstance(fields, list):
        for field in fields:
            if isinstance(field, dict):
                field["value"] = _safe_text(field.get("value"), limit=350)
    if _embed_character_count(embed) > _DISCORD_EMBED_TOTAL:
        embed["fields"] = fields[:5] if isinstance(fields, list) else []
    if _embed_character_count(embed) > _DISCORD_EMBED_TOTAL:
        raise ValueError("Discord embed exceeds the 6000-character limit")
    return embed


def _breakout_embed(
    candidate: AlertCandidate, score: Decimal, evidence: Mapping[str, Any]
) -> dict[str, Any]:
    username = _first(evidence, "username", "author_username")
    post_id = _first(evidence, "post_id") or candidate.entity_id
    post_url = x_post_url(post_id, username)
    post_text = _escape_markdown(_first(evidence, "post_text", "text", "content") or "", limit=900)
    explanation = _safe_text(
        _first(evidence, "explanation", "reason")
        or "The post crossed the configured breakout gates.",
        limit=550,
    )
    fields = [
        _field("Score", _format_score(score), inline=True),
        _field(
            "Velocity",
            _format_driver(
                "velocity_percentile",
                _first(evidence, "velocity_percentile", "velocity_pct")
                or _mapping(evidence.get("components")).get("velocity_percentile"),
            ),
            inline=True,
        ),
        _field(
            "Author lift",
            _format_driver(
                "author_lift",
                _first(evidence, "author_lift")
                or _mapping(evidence.get("components")).get("author_lift"),
            ),
            inline=True,
        ),
        _field("Why it triggered", explanation),
    ]
    drivers = _driver_lines(evidence)
    if drivers:
        fields.append(_field("Top score drivers", "\n".join(drivers)))
    embed: dict[str, Any] = {
        "title": _safe_text(f"BREAKOUT POST · {_format_score(score)}", limit=256),
        "url": post_url,
        "description": post_text or "Open the post on X for the source content.",
        "color": 0xE34948,
        "fields": fields,
        "image": {"url": f"attachment://{_CHART_FILENAME}"},
        "footer": {"text": "X Virality · Deterministic alert"},
    }
    if username and _USERNAME_RE.fullmatch(str(username).lstrip("@")):
        handle = str(username).lstrip("@")
        embed["author"] = {"name": f"@{handle}", "url": f"https://x.com/{handle}"}
    timestamp = _timestamp(evidence)
    if timestamp:
        embed["timestamp"] = timestamp
    return _fit_embed(embed)


def _emerging_embed(
    candidate: AlertCandidate, score: Decimal, evidence: Mapping[str, Any]
) -> dict[str, Any]:
    label = _safe_text(
        _first(evidence, "topic_label", "label", "topic") or "Emerging conversation",
        limit=180,
    )
    explanation = _safe_text(
        _first(evidence, "explanation", "reason")
        or "Independent-author activity crossed the configured conversation gates.",
        limit=550,
    )
    components = _mapping(evidence.get("components"))
    unique_authors = _first(evidence, "unique_authors") or components.get("unique_authors")
    topic_spike = _first(evidence, "topic_spike", "growth") or components.get("topic_spike")
    original_ratio = _first(
        evidence, "original_content_ratio", "original_post_ratio"
    ) or components.get("original_content_ratio")
    fields = [
        _field("Score", _format_score(score), inline=True),
        _field("Independent authors", unique_authors, inline=True),
        _field("Topic spike", _format_driver("topic_spike", topic_spike), inline=True),
        _field(
            "Original-post ratio",
            _format_driver("original_content_ratio", original_ratio),
            inline=True,
        ),
        _field("Why it triggered", explanation),
    ]
    top_posts = _top_post_lines(evidence)
    if top_posts:
        fields.append(_field("Top driving posts", "\n".join(top_posts)))
    else:
        drivers = _driver_lines(evidence)
        if drivers:
            fields.append(_field("Top score drivers", "\n".join(drivers)))
    embed: dict[str, Any] = {
        "title": _safe_text(f"EMERGING CONVERSATION · {_format_score(score)}", limit=256),
        "description": label,
        "color": 0x2A78D6,
        "fields": fields,
        "image": {"url": f"attachment://{_CHART_FILENAME}"},
        "footer": {"text": "X Virality · Independent-author signal"},
    }
    first_post = next(iter(_sequence(_first(evidence, "top_driving_posts", "top_posts"))), None)
    if isinstance(first_post, Mapping):
        post_id = first_post.get("id", first_post.get("post_id"))
        if post_id is not None:
            embed["url"] = x_post_url(
                post_id, first_post.get("username", first_post.get("author_username"))
            )
    timestamp = _timestamp(evidence)
    if timestamp:
        embed["timestamp"] = timestamp
    return _fit_embed(embed)


def _numeric_points(rows: object, *, value_keys: tuple[str, ...]) -> list[list[object]]:
    points: list[list[object]] = []
    for index, item in enumerate(_sequence(rows)[:24]):
        if not isinstance(item, Mapping):
            continue
        value: object | None = None
        for key in value_keys:
            if item.get(key) is not None:
                value = item[key]
                break
        parsed = _decimal(value)
        if parsed is None:
            continue
        label: object = item.get("label", item.get("x", index + 1))
        if item.get("age_minutes") is not None:
            label = f"{item['age_minutes']}m"
        elif item.get("window_minutes") is not None:
            label = f"{item['window_minutes']}m"
        elif item.get("target_age_seconds") is not None:
            seconds = _decimal(item["target_age_seconds"])
            if seconds is not None:
                label = f"{seconds / Decimal(60):g}m"
        points.append([_safe_text(label, limit=24), float(parsed)])
    return points


def _chart_spec(
    candidate: AlertCandidate, score: Decimal, evidence: Mapping[str, Any]
) -> dict[str, Any]:
    provided = evidence.get("chart_spec")
    if isinstance(provided, Mapping):
        chart = dict(provided)
        chart.setdefault(
            "title",
            "Breakout post — early engagement"
            if candidate.kind == "breakout_post"
            else "Emerging conversation — independent authors",
        )
        chart.setdefault("subtitle", "Observed values only")
        return cast(dict[str, Any], _json_safe(chart))

    if candidate.kind == "breakout_post":
        snapshots = _first(evidence, "snapshots", "metric_snapshots", "observations")
        engagement = _numeric_points(
            snapshots, value_keys=("engagement", "weighted_engagement", "value")
        )
        if engagement:
            return {
                "title": "Breakout post — early engagement",
                "subtitle": "Observed snapshots only; no interpolated history",
                "x_label": "Post age",
                "y_label": "Weighted engagement",
                "series": [{"name": "Engagement", "points": engagement}],
            }
        title = "Breakout post — score drivers"
    else:
        snapshots = _first(evidence, "topic_snapshots", "windows", "observations")
        authors = _numeric_points(
            snapshots, value_keys=("unique_authors", "independent_authors", "value")
        )
        posts = _numeric_points(snapshots, value_keys=("post_count", "posts"))
        series: list[dict[str, Any]] = []
        if authors:
            series.append({"name": "Independent authors", "points": authors})
        if posts:
            series.append({"name": "Posts", "points": posts})
        if series:
            return {
                "title": "Emerging conversation — spread",
                "subtitle": "Observed topic windows only",
                "x_label": "Window",
                "y_label": "Count",
                "series": series,
            }
        title = "Emerging conversation — score drivers"

    driver_points: list[list[object]] = []
    components = _mapping(evidence.get("components"))
    for key in (
        "velocity_percentile",
        "acceleration_percentile",
        "author_lift_percentile",
        "topic_spike_percentile",
        "conversation_spread_percentile",
        "unique_author_growth",
        "original_content_ratio",
        "text_diversity",
    ):
        parsed = _decimal(components.get(key))
        if parsed is None:
            continue
        if Decimal("0") <= parsed <= Decimal("1"):
            parsed *= Decimal("100")
        driver_points.append([_label(key), float(parsed)])
    if not driver_points:
        driver_points = [["Virality score", float(score)]]
    return {
        "kind": "bar",
        "title": title,
        "subtitle": "Deterministic model components",
        "y_label": "Component value",
        "series_name": "Drivers",
        "points": driver_points[:6],
    }


def _json_safe(value: object, *, depth: int = 0) -> Any:
    if depth > 12:
        return _safe_text(value, limit=256)
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {
            _safe_text(key, limit=128): _json_safe(item, depth=depth + 1)
            for key, item in list(value.items())[:200]
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(item, depth=depth + 1) for item in list(value)[:200]]
    return _safe_text(value, limit=4000)


def prepare_alert(
    candidate: AlertCandidate,
    *,
    alert_id: UUID,
    outbox_id: UUID,
    channel_id: str,
    component_secret: SecretStr | str | bytes,
) -> PreparedAlert:
    score = _decimal(candidate.score)
    if score is None or not Decimal("0") <= score <= Decimal("100"):
        raise ValueError("alert score must be between 0 and 100")
    evidence = _mapping(_json_safe(candidate.evidence))
    chart_spec = _chart_spec(candidate, score, evidence)
    embed = (
        _breakout_embed(candidate, score, evidence)
        if candidate.kind == "breakout_post"
        else _emerging_embed(candidate, score, evidence)
    )
    good_id = build_feedback_custom_id(
        "good",
        alert_id=alert_id,
        outbox_id=outbox_id,
        channel_id=channel_id,
        secret=component_secret,
    )
    bad_id = build_feedback_custom_id(
        "bad",
        alert_id=alert_id,
        outbox_id=outbox_id,
        channel_id=channel_id,
        secret=component_secret,
    )
    nonce = stable_discord_nonce(alert_id, outbox_id, channel_id)
    payload: dict[str, Any] = {
        "embeds": [embed],
        "components": [
            {
                "type": 1,
                "components": [
                    {"type": 2, "style": 3, "label": "Good alert", "custom_id": good_id},
                    {"type": 2, "style": 4, "label": "Bad alert", "custom_id": bad_id},
                ],
            }
        ],
        "allowed_mentions": {"parse": []},
        "nonce": nonce,
        "enforce_nonce": True,
        "attachments": [
            {
                "id": "0",
                "filename": _CHART_FILENAME,
                "description": chart_description(chart_spec),
            }
        ],
    }
    return PreparedAlert(
        alert_id=alert_id,
        outbox_id=outbox_id,
        nonce=nonce,
        payload=payload,
        chart_spec=chart_spec,
    )


async def create_alert_outbox(
    session: AsyncSession,
    candidate: AlertCandidate,
    *,
    channel_id: str,
    component_secret: SecretStr | str | bytes,
    cooldown_minutes: int = 180,
    max_attempts: int = 6,
    now: datetime | None = None,
) -> AlertOutboxResult:
    """Idempotently persist an alert and its permanent Discord outbox row."""

    if candidate.kind not in {"breakout_post", "emerging_conversation"}:
        raise ValueError("unsupported alert kind")
    if not _SNOWFLAKE_RE.fullmatch(channel_id):
        raise ValueError("channel_id must be a Discord snowflake")
    if not 0 <= cooldown_minutes <= 10_080:
        raise ValueError("cooldown_minutes must be between 0 and 10080")
    if not 1 <= max_attempts <= 100:
        raise ValueError("max_attempts must be between 1 and 100")
    created_at = now or utcnow()
    if created_at.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    score = _decimal(candidate.score)
    if score is None or not Decimal("0") <= score <= Decimal("100"):
        raise ValueError("alert score must be between 0 and 100")
    try:
        score_id = UUID(candidate.score_id)
    except ValueError as error:
        raise ValueError("candidate score_id must be a UUID") from error
    entity_id = _bounded_identifier(candidate.entity_id, name="entity_id", limit=64)
    policy_hash = _bounded_identifier(candidate.policy_hash, name="policy_hash", limit=64)
    dedupe_key = _bounded_identifier(candidate.dedupe_key, name="dedupe_key", limit=255)

    proposed_alert_id = uuid4()
    alert_id = await session.scalar(
        pg_insert(Alert)
        .values(
            id=proposed_alert_id,
            kind=candidate.kind,
            entity_type="post" if candidate.kind == "breakout_post" else "topic",
            entity_id=entity_id,
            score_id=score_id,
            score=score,
            evidence=_json_safe(candidate.evidence),
            policy_hash=policy_hash,
            dedupe_key=dedupe_key,
            cooldown_until=created_at + timedelta(minutes=cooldown_minutes),
            state="queued",
            created_at=created_at,
            updated_at=created_at,
        )
        .on_conflict_do_nothing(index_elements=[Alert.dedupe_key])
        .returning(Alert.id)
    )
    if alert_id is None:
        existing_alert = await session.scalar(select(Alert).where(Alert.dedupe_key == dedupe_key))
        if existing_alert is None:
            raise RuntimeError("alert conflict occurred but the existing alert was not found")
        alert_id = existing_alert.id

    existing_outbox = await session.scalar(
        select(DiscordOutbox).where(
            DiscordOutbox.alert_id == alert_id,
            DiscordOutbox.channel_id == channel_id,
        )
    )
    if existing_outbox is not None:
        return AlertOutboxResult(
            alert_id=alert_id,
            outbox_id=existing_outbox.id,
            created=False,
        )

    proposed_outbox_id = uuid4()
    prepared = prepare_alert(
        candidate,
        alert_id=alert_id,
        outbox_id=proposed_outbox_id,
        channel_id=channel_id,
        component_secret=component_secret,
    )
    outbox_id = await session.scalar(
        pg_insert(DiscordOutbox)
        .values(
            id=proposed_outbox_id,
            alert_id=alert_id,
            channel_id=channel_id,
            nonce=prepared.nonce,
            payload=prepared.payload,
            chart_spec=prepared.chart_spec,
            state="queued",
            attempts=0,
            max_attempts=max_attempts,
            run_at=created_at,
            created_at=created_at,
            updated_at=created_at,
        )
        .on_conflict_do_nothing(index_elements=[DiscordOutbox.alert_id, DiscordOutbox.channel_id])
        .returning(DiscordOutbox.id)
    )
    if outbox_id is None:
        concurrent_outbox = await session.scalar(
            select(DiscordOutbox).where(
                DiscordOutbox.alert_id == alert_id,
                DiscordOutbox.channel_id == channel_id,
            )
        )
        if concurrent_outbox is None:
            raise RuntimeError("outbox conflict occurred but the existing row was not found")
        outbox_id = concurrent_outbox.id
        return AlertOutboxResult(alert_id=alert_id, outbox_id=outbox_id, created=False)
    return AlertOutboxResult(
        alert_id=alert_id,
        outbox_id=outbox_id,
        created=True,
    )


async def create_alert(
    session: AsyncSession,
    candidate: AlertCandidate,
    settings: Settings,
    *,
    cooldown_minutes: int = 180,
    max_attempts: int = 6,
    now: datetime | None = None,
) -> AlertOutboxResult:
    channel_id = settings.discord_alert_channel_id
    component_secret = settings.discord_component_secret
    if channel_id is None or component_secret is None:
        raise ValueError("Discord alert channel and component secret are required")
    return await create_alert_outbox(
        session,
        candidate,
        channel_id=channel_id,
        component_secret=component_secret,
        cooldown_minutes=cooldown_minutes,
        max_attempts=max_attempts,
        now=now,
    )


queue_alert = create_alert
build_component_custom_id = build_feedback_custom_id
parse_component_custom_id = verify_feedback_custom_id
