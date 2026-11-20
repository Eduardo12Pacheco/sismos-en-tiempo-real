"""Structured logging shared by every service.

Structlog events and plain stdlib log records (Quix Streams, confluent-kafka,
uvicorn-style libraries) go through the same ``ProcessorFormatter`` so the whole
process emits one JSON line per record in production.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor

LOG_FORMATS = frozenset({"json", "console"})


def _service_processor(service: str) -> Processor:
    """Return a processor that stamps every record with the service name."""

    def add_service(
        logger: Any,
        method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict["service"] = service
        return event_dict

    return add_service


def _validate(level: str, fmt: str) -> tuple[str, int]:
    level_name = level.strip().upper()
    levels = logging.getLevelNamesMapping()
    if level_name not in levels:
        allowed = ", ".join(sorted(levels))
        raise ValueError(f"unknown log level {level!r}; expected one of: {allowed}")

    format_name = fmt.strip().lower()
    if format_name not in LOG_FORMATS:
        allowed = ", ".join(sorted(LOG_FORMATS))
        raise ValueError(f"unsupported log format {fmt!r}; expected one of: {allowed}")
    return format_name, levels[level_name]


def configure_logging(service: str, level: str = "INFO", fmt: str = "json") -> None:
    """Configure structlog and stdlib logging to emit one stream on stdout.

    ``fmt="json"`` is the production format; ``fmt="console"`` is the
    human-readable development renderer.
    """
    format_name, numeric_level = _validate(level, fmt)
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts")
    add_service = _service_processor(service)

    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        add_service,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )

    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if format_name == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            add_service,
            timestamper,
            structlog.processors.format_exc_info,
        ],
        processor=renderer,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(numeric_level)

    # HTTP client request lines are debug detail; keep bootstrap output readable.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(max(numeric_level, logging.WARNING))
