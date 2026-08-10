from __future__ import annotations

import math
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any, Literal, cast

from PIL import Image, ImageDraw, ImageFont

ChartState = Literal["success", "empty", "error"]

_SURFACE = "#fcfcfb"
_PAGE = "#f9f9f7"
_INK = "#0b0b0b"
_SECONDARY_INK = "#52514e"
_MUTED_INK = "#898781"
_GRID = "#e1e0d9"
_AXIS = "#c3c2b7"
_SERIES_COLORS = ("#2a78d6", "#eb6834", "#1baf7a")
_BLUE_RAMP = ("#86b6ef", "#5598e7", "#2a78d6", "#256abf", "#1c5cab", "#184f95")
_MAX_SERIES = 3
_MAX_POINTS = 24
_MAX_BARS = 6


@dataclass(frozen=True, slots=True)
class ChartPoint:
    label: str
    value: float


@dataclass(frozen=True, slots=True)
class ChartSeries:
    name: str
    points: tuple[ChartPoint, ...]


@dataclass(frozen=True, slots=True)
class ParsedChart:
    title: str
    subtitle: str
    x_label: str
    y_label: str
    kind: Literal["line", "bar"]
    state: ChartState
    message: str
    series: tuple[ChartSeries, ...]


@dataclass(frozen=True, slots=True)
class RenderedChart:
    png: bytes
    filename: str
    description: str
    width: int
    height: int


def _safe_text(value: object, *, limit: int) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(value))
    cleaned = " ".join(
        "".join(character if character.isprintable() else " " for character in normalized).split()
    )
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: max(0, limit - 1)].rstrip()}…"


def _finite_float(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _is_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray)


def _point(value: object, index: int) -> ChartPoint | None:
    if isinstance(value, Mapping):
        numeric = value.get("y", value.get("value", value.get("engagement")))
        parsed = _finite_float(numeric)
        if parsed is None:
            return None
        label_value = value.get(
            "label",
            value.get(
                "x",
                value.get(
                    "age_minutes",
                    value.get("window_minutes", value.get("target_age_seconds", index + 1)),
                ),
            ),
        )
        label = _safe_text(label_value, limit=24)
        if "target_age_seconds" in value and "label" not in value and "x" not in value:
            seconds = _finite_float(value.get("target_age_seconds"))
            if seconds is not None:
                label = f"{seconds / 60:g}m"
        elif "age_minutes" in value and "label" not in value and "x" not in value:
            minutes = _finite_float(value.get("age_minutes"))
            if minutes is not None:
                label = f"{minutes:g}m"
        return ChartPoint(label=label or str(index + 1), value=parsed)
    if _is_sequence(value):
        items = list(cast(Sequence[object], value))
        if len(items) < 2:
            return None
        parsed = _finite_float(items[1])
        if parsed is None:
            return None
        return ChartPoint(label=_safe_text(items[0], limit=24) or str(index + 1), value=parsed)
    parsed = _finite_float(value)
    if parsed is None:
        return None
    return ChartPoint(label=str(index + 1), value=parsed)


def _points(raw: object) -> tuple[ChartPoint, ...]:
    if isinstance(raw, Mapping):
        values: list[object] = [[key, value] for key, value in raw.items()]
    elif _is_sequence(raw):
        values = list(cast(Sequence[object], raw))
    else:
        values = []
    parsed = tuple(
        point
        for index, item in enumerate(values[:_MAX_POINTS])
        if (point := _point(item, index)) is not None
    )
    return parsed


def _series(spec: Mapping[str, Any]) -> tuple[ChartSeries, ...]:
    raw_series = spec.get("series")
    parsed: list[ChartSeries] = []
    if isinstance(raw_series, Mapping):
        for name, values in list(raw_series.items())[:_MAX_SERIES]:
            points = _points(values)
            if points:
                parsed.append(ChartSeries(_safe_text(name, limit=36) or "Series", points))
    elif _is_sequence(raw_series):
        for index, item in enumerate(list(cast(Sequence[object], raw_series))[:_MAX_SERIES]):
            if not isinstance(item, Mapping):
                continue
            points = _points(item.get("points", item.get("values")))
            if points:
                parsed.append(
                    ChartSeries(
                        _safe_text(
                            item.get("name", item.get("label", f"Series {index + 1}")), limit=36
                        ),
                        points,
                    )
                )
    if parsed:
        return tuple(parsed)

    points = _points(spec.get("points", spec.get("values")))
    if points:
        return (ChartSeries(_safe_text(spec.get("series_name", "Trend"), limit=36), points),)
    return ()


def parse_chart_spec(spec: Mapping[str, Any]) -> ParsedChart:
    requested_state = str(spec.get("state", "success")).lower()
    state = cast(
        ChartState,
        requested_state if requested_state in {"success", "empty", "error"} else "error",
    )
    kind: Literal["line", "bar"] = "bar" if str(spec.get("kind", "line")) == "bar" else "line"
    series = _series(spec)
    if state == "success" and not series:
        state = "empty"
    default_message = {
        "success": "",
        "empty": "The alert is valid; trend observations have not arrived yet.",
        "error": "Trend data could not be rendered. Alert evidence remains available above.",
    }[state]
    return ParsedChart(
        title=_safe_text(spec.get("title", "Virality signal"), limit=72) or "Virality signal",
        subtitle=_safe_text(spec.get("subtitle", "Early growth, observed values only"), limit=110),
        x_label=_safe_text(spec.get("x_label", "Observation"), limit=40),
        y_label=_safe_text(spec.get("y_label", "Value"), limit=40),
        kind=kind,
        state=state,
        message=_safe_text(spec.get("message", default_message), limit=180) or default_message,
        series=series,
    )


def _font_candidates(bold: bool) -> tuple[Path, ...]:
    pillow_root = Path(ImageFont.__file__).resolve().parent
    windows = Path("C:/Windows/Fonts")
    if bold:
        names = ("DejaVuSans-Bold.ttf", "segoeuib.ttf", "arialbd.ttf")
    else:
        names = ("DejaVuSans.ttf", "segoeui.ttf", "arial.ttf")
    roots = (
        pillow_root / "fonts",
        Path("/usr/share/fonts/truetype/dejavu"),
        Path("/usr/share/fonts/dejavu"),
        windows,
    )
    return tuple(root / name for root in roots for name in names)


@lru_cache(maxsize=32)
def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in _font_candidates(bold):
        if not candidate.is_file():
            continue
        try:
            return ImageFont.truetype(str(candidate), size=size)
        except OSError:
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _drawable_text(value: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> str:
    try:
        font.getbbox(value)
    except (UnicodeEncodeError, ValueError):
        return value.encode("latin-1", errors="replace").decode("latin-1")
    return value


def _text(
    draw: ImageDraw.ImageDraw,
    position: tuple[float, float],
    value: str,
    *,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    fill: str,
    anchor: str | None = None,
) -> None:
    draw.text(position, _drawable_text(value, font), font=font, fill=fill, anchor=anchor)


def _format_number(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.1f}M".replace(".0M", "M")
    if absolute >= 1_000:
        return f"{value / 1_000:.1f}K".replace(".0K", "K")
    if absolute >= 100 or value.is_integer():
        return f"{value:,.0f}"
    if absolute >= 10:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _header(
    draw: ImageDraw.ImageDraw,
    chart: ParsedChart,
    *,
    width: int,
    scale: float,
) -> int:
    title_font = _font(max(25, round(34 * scale)), bold=True)
    subtitle_font = _font(max(14, round(18 * scale)))
    left = round(42 * scale)
    top = round(34 * scale)
    _text(draw, (left, top), chart.title, font=title_font, fill=_INK)
    subtitle_y = top + round(46 * scale)
    if chart.subtitle:
        _text(draw, (left, subtitle_y), chart.subtitle, font=subtitle_font, fill=_SECONDARY_INK)
    draw.line(
        (left, subtitle_y + round(34 * scale), width - left, subtitle_y + round(34 * scale)),
        fill=_GRID,
        width=max(1, round(scale)),
    )
    return subtitle_y + round(54 * scale)


def _render_state(
    draw: ImageDraw.ImageDraw,
    chart: ParsedChart,
    *,
    width: int,
    height: int,
    top: int,
    scale: float,
) -> None:
    panel_left = round(42 * scale)
    panel_right = width - panel_left
    panel_bottom = height - round(42 * scale)
    draw.rounded_rectangle(
        (panel_left, top, panel_right, panel_bottom),
        radius=round(12 * scale),
        fill=_PAGE,
        outline=_GRID,
        width=max(1, round(scale)),
    )
    icon_center = (width // 2, top + round(94 * scale))
    icon_color = _AXIS if chart.state == "empty" else "#d03b3b"
    draw.ellipse(
        (
            icon_center[0] - round(18 * scale),
            icon_center[1] - round(18 * scale),
            icon_center[0] + round(18 * scale),
            icon_center[1] + round(18 * scale),
        ),
        outline=icon_color,
        width=max(2, round(3 * scale)),
    )
    if chart.state == "empty":
        draw.line(
            (
                icon_center[0] - round(8 * scale),
                icon_center[1],
                icon_center[0] + round(8 * scale),
                icon_center[1],
            ),
            fill=icon_color,
            width=max(2, round(3 * scale)),
        )
        heading = "Waiting for observations"
    else:
        draw.line(
            (
                icon_center[0],
                icon_center[1] - round(9 * scale),
                icon_center[0],
                icon_center[1] + round(4 * scale),
            ),
            fill=icon_color,
            width=max(2, round(3 * scale)),
        )
        draw.ellipse(
            (
                icon_center[0] - round(2 * scale),
                icon_center[1] + round(9 * scale),
                icon_center[0] + round(2 * scale),
                icon_center[1] + round(13 * scale),
            ),
            fill=icon_color,
        )
        heading = "Chart unavailable"
    heading_font = _font(max(19, round(24 * scale)), bold=True)
    body_font = _font(max(14, round(17 * scale)))
    _text(
        draw,
        (width / 2, icon_center[1] + round(42 * scale)),
        heading,
        font=heading_font,
        fill=_INK,
        anchor="ma",
    )
    _text(
        draw,
        (width / 2, icon_center[1] + round(82 * scale)),
        chart.message,
        font=body_font,
        fill=_SECONDARY_INK,
        anchor="ma",
    )


def _line_chart(
    draw: ImageDraw.ImageDraw,
    chart: ParsedChart,
    *,
    width: int,
    height: int,
    top: int,
    scale: float,
) -> None:
    axis_font = _font(max(12, round(14 * scale)))
    label_font = _font(max(13, round(15 * scale)), bold=True)
    legend_font = _font(max(12, round(14 * scale)))
    left = round(92 * scale)
    right = width - round(48 * scale)
    legend_height = round(36 * scale) if len(chart.series) > 1 else 0
    plot_top = top + legend_height + round(18 * scale)
    bottom = height - round(68 * scale)
    plot_width = max(1, right - left)
    plot_height = max(1, bottom - plot_top)

    values = [point.value for series in chart.series for point in series.points]
    minimum = min(values)
    maximum = max(values)
    y_min = min(0.0, minimum)
    y_max = max(0.0, maximum)
    if math.isclose(y_min, y_max):
        y_max = y_min + max(1.0, abs(y_min) * 0.1)
    padding = (y_max - y_min) * 0.08
    if y_min < 0:
        y_min -= padding
    y_max += padding

    if len(chart.series) > 1:
        cursor = left
        for index, series in enumerate(chart.series):
            color = _SERIES_COLORS[index]
            draw.line(
                (
                    cursor,
                    top + round(13 * scale),
                    cursor + round(24 * scale),
                    top + round(13 * scale),
                ),
                fill=color,
                width=max(2, round(3 * scale)),
            )
            draw.ellipse(
                (
                    cursor + round(9 * scale),
                    top + round(8 * scale),
                    cursor + round(19 * scale),
                    top + round(18 * scale),
                ),
                fill=color,
                outline=_SURFACE,
                width=max(1, round(2 * scale)),
            )
            _text(
                draw,
                (cursor + round(32 * scale), top + round(3 * scale)),
                series.name,
                font=legend_font,
                fill=_SECONDARY_INK,
            )
            cursor += round((48 + min(24, len(series.name)) * 8) * scale)

    for tick in range(5):
        ratio = tick / 4
        y = bottom - ratio * plot_height
        value = y_min + ratio * (y_max - y_min)
        draw.line((left, y, right, y), fill=_GRID, width=max(1, round(scale)))
        _text(
            draw,
            (left - round(12 * scale), y),
            _format_number(value),
            font=axis_font,
            fill=_MUTED_INK,
            anchor="rm",
        )
    draw.line((left, plot_top, left, bottom), fill=_AXIS, width=max(1, round(scale)))
    draw.line((left, bottom, right, bottom), fill=_AXIS, width=max(1, round(scale)))

    label_source = max(chart.series, key=lambda item: len(item.points)).points
    label_indexes = sorted({0, max(0, len(label_source) // 2), max(0, len(label_source) - 1)})
    for index in label_indexes:
        point = label_source[index]
        x = left if len(label_source) == 1 else left + index * plot_width / (len(label_source) - 1)
        _text(
            draw,
            (x, bottom + round(13 * scale)),
            point.label,
            font=axis_font,
            fill=_MUTED_INK,
            anchor="ma",
        )

    for series_index, series in enumerate(chart.series):
        color = _SERIES_COLORS[series_index]
        coordinates: list[tuple[float, float]] = []
        for index, point in enumerate(series.points):
            x = (
                left
                if len(series.points) == 1
                else left + index * plot_width / (len(series.points) - 1)
            )
            y = bottom - (point.value - y_min) / (y_max - y_min) * plot_height
            coordinates.append((x, y))
        if len(coordinates) >= 2:
            draw.line(
                coordinates,
                fill=color,
                width=max(2, round(3 * scale)),
                joint="curve",
            )
        for x, y in coordinates:
            radius = max(5, round(6 * scale))
            draw.ellipse(
                (x - radius, y - radius, x + radius, y + radius),
                fill=color,
                outline=_SURFACE,
                width=max(2, round(2 * scale)),
            )
        end_x, end_y = coordinates[-1]
        end_value = _format_number(series.points[-1].value)
        label_x = min(right - round(4 * scale), end_x + round(12 * scale))
        anchor = "rm" if label_x >= right - round(8 * scale) else "lm"
        _text(
            draw,
            (label_x, end_y - round(13 * scale)),
            end_value,
            font=label_font,
            fill=_INK,
            anchor=anchor,
        )

    if chart.y_label:
        _text(
            draw,
            (left, plot_top - round(12 * scale)),
            chart.y_label,
            font=axis_font,
            fill=_SECONDARY_INK,
            anchor="ls",
        )
    if chart.x_label:
        _text(
            draw,
            (right, height - round(24 * scale)),
            chart.x_label,
            font=axis_font,
            fill=_SECONDARY_INK,
            anchor="rs",
        )


def _bar_chart(
    draw: ImageDraw.ImageDraw,
    chart: ParsedChart,
    *,
    width: int,
    height: int,
    top: int,
    scale: float,
) -> None:
    series = chart.series[0]
    points = tuple(sorted(series.points, key=lambda point: point.value, reverse=True)[:_MAX_BARS])
    label_font = _font(max(12, round(15 * scale)))
    value_font = _font(max(12, round(15 * scale)), bold=True)
    left = round(180 * scale)
    right = width - round(78 * scale)
    plot_top = top + round(16 * scale)
    bottom = height - round(48 * scale)
    row_height = max(round(42 * scale), (bottom - plot_top) // max(1, len(points)))
    bar_height = min(round(24 * scale), max(round(12 * scale), row_height // 2))
    maximum = max(max(0.0, point.value) for point in points) or 1.0

    for index, point in enumerate(points):
        center_y = plot_top + index * row_height + row_height // 2
        _text(
            draw,
            (left - round(14 * scale), center_y),
            point.label,
            font=label_font,
            fill=_SECONDARY_INK,
            anchor="rm",
        )
        usable = right - left
        bar_width = max(2, round(max(0.0, point.value) / maximum * usable))
        color = _BLUE_RAMP[min(index, len(_BLUE_RAMP) - 1)]
        bar_top = center_y - bar_height // 2
        draw.rounded_rectangle(
            (left, bar_top, left + bar_width, bar_top + bar_height),
            radius=max(2, round(4 * scale)),
            fill=color,
        )
        draw.rectangle(
            (left, bar_top, min(left + round(5 * scale), left + bar_width), bar_top + bar_height),
            fill=color,
        )
        value_x = min(right, left + bar_width + round(10 * scale))
        anchor = "rm" if value_x >= right - round(4 * scale) else "lm"
        _text(
            draw,
            (value_x, center_y),
            _format_number(point.value),
            font=value_font,
            fill=_INK,
            anchor=anchor,
        )
    draw.line((left, plot_top, left, bottom), fill=_AXIS, width=max(1, round(scale)))
    if chart.y_label:
        _text(
            draw,
            (left, height - round(22 * scale)),
            chart.y_label,
            font=label_font,
            fill=_SECONDARY_INK,
            anchor="ls",
        )


def chart_description(spec: Mapping[str, Any]) -> str:
    chart = parse_chart_spec(spec)
    if chart.state != "success":
        return _safe_text(f"{chart.title}. {chart.message}", limit=1024)
    fragments = [chart.title]
    for series in chart.series:
        values = "; ".join(
            f"{point.label}: {_format_number(point.value)}" for point in series.points
        )
        fragments.append(f"{series.name} — {values}")
    if chart.x_label or chart.y_label:
        fragments.append(f"Axes: {chart.x_label or 'observation'}; {chart.y_label or 'value'}")
    return _safe_text(". ".join(fragments), limit=1024)


def render_chart(
    spec: Mapping[str, Any],
    *,
    width: int = 960,
    height: int = 540,
) -> bytes:
    """Render a deterministic PNG entirely in memory.

    Accessibility is supplied by :func:`chart_description`, which serializes the
    plotted values as text for Discord's attachment description.
    """

    if not 640 <= width <= 1600 or not 360 <= height <= 1000:
        raise ValueError("chart dimensions must be within 640x360 and 1600x1000")
    chart = parse_chart_spec(spec)
    image = Image.new("RGB", (width, height), _SURFACE)
    draw = ImageDraw.Draw(image)
    scale = min(width / 960, height / 540)
    top = _header(draw, chart, width=width, scale=scale)
    if chart.state != "success":
        _render_state(draw, chart, width=width, height=height, top=top, scale=scale)
    elif chart.kind == "bar":
        _bar_chart(draw, chart, width=width, height=height, top=top, scale=scale)
    else:
        _line_chart(draw, chart, width=width, height=height, top=top, scale=scale)

    output = BytesIO()
    image.save(output, format="PNG", optimize=True, compress_level=9)
    return output.getvalue()


def render_chart_artifact(
    spec: Mapping[str, Any],
    *,
    width: int = 960,
    height: int = 540,
    filename: str = "virality.png",
) -> RenderedChart:
    return RenderedChart(
        png=render_chart(spec, width=width, height=height),
        filename=filename,
        description=chart_description(spec),
        width=width,
        height=height,
    )


def render_preview_states(*, width: int = 720, height: int = 405) -> dict[str, bytes]:
    """Return deterministic success/empty/error PNGs for visual review.

    This helper has no filesystem side effects. A reviewer can write the returned
    bytes to a temporary directory and inspect them with the normal image reader.
    """

    specs: dict[str, dict[str, Any]] = {
        "success": {
            "title": "Breakout post — early engagement",
            "subtitle": "Observed snapshots only",
            "x_label": "Post age",
            "y_label": "Weighted engagement",
            "series": [
                {
                    "name": "Engagement",
                    "points": [["2m", 18], ["5m", 61], ["10m", 154], ["20m", 331]],
                },
                {
                    "name": "Quotes",
                    "points": [["2m", 2], ["5m", 9], ["10m", 31], ["20m", 72]],
                },
            ],
        },
        "empty": {
            "state": "empty",
            "title": "Breakout post — early engagement",
            "subtitle": "Observed snapshots only",
        },
        "error": {
            "state": "error",
            "title": "Emerging conversation — independent authors",
            "message": "Trend data could not be rendered. Alert evidence remains available above.",
        },
    }
    return {name: render_chart(spec, width=width, height=height) for name, spec in specs.items()}
