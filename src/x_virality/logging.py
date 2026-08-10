from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping
from typing import Any, cast

import structlog
from structlog.typing import EventDict, WrappedLogger

_SECRET_KEY = re.compile(r"(?:token|secret|authorization|password|database_url)", re.IGNORECASE)
_BEARER_VALUE = re.compile(r"(?i)\bBearer\s+[^\s,;]+")
_URL_CREDENTIAL = re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^:/@\s]+:)([^@\s]+)(@)")


def _redact_string(value: str) -> str:
    value = _BEARER_VALUE.sub("Bearer [redacted]", value)
    return _URL_CREDENTIAL.sub(r"\1[redacted]\3", value)


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: "[redacted]" if _SECRET_KEY.search(str(key)) else _redact_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, str):
        return _redact_string(value)
    return value


def redact_secrets(_logger: WrappedLogger, _method_name: str, event_dict: EventDict) -> EventDict:
    return cast(EventDict, _redact_value(event_dict))


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(stream=sys.stdout, level=numeric_level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            redact_secrets,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))
