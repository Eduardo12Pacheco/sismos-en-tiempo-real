"""Typed application settings loaded from the environment and an optional ``.env``.

Every knob of the pipeline is declared here once. The CLI and Compose services
read the same model, so invalid values fail fast with a readable message naming
the offending ``SEISMIC_*`` variable instead of failing deep inside a service.
"""

from __future__ import annotations

import math
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Final, Literal

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ENV_PREFIX: Final = "SEISMIC_"

ALLOWED_LIVE_FEEDS: Final[frozenset[str]] = frozenset(
    {
        "all_hour",
        "all_day",
        "all_week",
        "2.5_hour",
        "2.5_day",
        "4.5_hour",
        "4.5_day",
        "significant_hour",
        "significant_day",
    }
)

LOG_LEVELS: Final[frozenset[str]] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

#: Default ``/metrics`` port injected by each service CLI command. ``METRICS_PORT``
#: can always override it through the environment.
SERVICE_METRICS_PORTS: Final = MappingProxyType(
    {
        "ingest": 9101,
        "processor": 9102,
        "sink": 9103,
        "monitor": 9104,
    }
)


class ConfigError(Exception):
    """Raised when the environment contains an invalid ``SEISMIC_*`` value."""


def _format_validation_error(error: ValidationError) -> str:
    """Render pydantic errors as one line per variable, never echoing values."""
    lines = ["invalid configuration:"]
    for detail in error.errors(include_url=False, include_input=False):
        field = ".".join(str(part) for part in detail["loc"])
        variable = f"{ENV_PREFIX}{field.upper()}"
        lines.append(f"  - {variable}: {detail['msg']}")
    return "\n".join(lines)


def _parse_csv(value: object) -> object:
    if not isinstance(value, str):
        return value
    items = tuple(part.strip() for part in value.split(",") if part.strip())
    if not items:
        raise ValueError("must be a non-empty comma-separated list")
    return items


class Settings(BaseSettings):
    """Validated runtime configuration for every service.

    Defaults target the local Compose stack. ``CLICKHOUSE_PASSWORD`` is a
    development-only credential and must be replaced for any non-local use.
    """

    model_config = SettingsConfigDict(
        env_prefix=ENV_PREFIX,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Broker and schema registry.
    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"

    # ClickHouse.
    clickhouse_host: str = "localhost"
    clickhouse_port: int = Field(default=8123, ge=1, le=65535)
    clickhouse_user: str = "seismic"
    clickhouse_password: str = "seismic-local"
    clickhouse_database: str = "seismic"

    # Ingest source and replay.
    ingest_source: Literal["replay", "usgs"] = "replay"
    replay_file: Path = Path("data/samples/usgs_2026-11-02_2026-11-09_m1.geojson")
    replay_speed: float | None = 1440.0
    replay_chaos: bool = False
    chaos_seed: int = 42
    chaos_malformed_rate: float = Field(default=0.02, ge=0.0, le=1.0, allow_inf_nan=False)
    chaos_duplicate_rate: float = Field(default=0.05, ge=0.0, le=1.0, allow_inf_nan=False)
    chaos_revision_rate: float = Field(default=0.02, ge=0.0, le=1.0, allow_inf_nan=False)

    # Optional live source.
    live_feed: str = "all_hour"
    live_poll_seconds: float = Field(default=60.0, ge=60.0, allow_inf_nan=False)

    # Processing.
    include_event_types: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("earthquake",), min_length=1
    )
    alert_warning_mag: float = Field(default=6.0, allow_inf_nan=False)
    alert_watch_mag: float = Field(default=5.0, allow_inf_nan=False)
    alert_ecuador_mag: float = Field(default=4.0, allow_inf_nan=False)
    window_minutes: int = Field(default=60, gt=0)
    window_grace_minutes: int = Field(default=10, ge=0)
    state_dir: Path = Path("./.state")

    # Sink, retries and producer backpressure.
    sink_batch_size: int = Field(default=500, gt=0)
    sink_batch_max_wait_seconds: float = Field(default=1.0, ge=0.0, allow_inf_nan=False)
    retry_base_seconds: float = Field(default=0.5, ge=0.0, allow_inf_nan=False)
    retry_max_seconds: float = Field(default=30.0, gt=0.0, allow_inf_nan=False)
    producer_queue_max_messages: int = Field(default=10_000, gt=0)

    # Observability and monitor.
    metrics_port: int = Field(default=9101, ge=1, le=65535)
    monitor_groups: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("seismic-processor", "seismic-sink"), min_length=1
    )
    monitor_interval_seconds: float = Field(default=5.0, gt=0.0, allow_inf_nan=False)
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    @field_validator("replay_speed", mode="before")
    @classmethod
    def _validate_replay_speed(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if text.lower() == "max":
                return None
            value = text
        try:
            number = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise ValueError("must be a finite number greater than 0 or 'max'") from None
        if not math.isfinite(number) or number <= 0:
            raise ValueError("must be a finite number greater than 0 or 'max'")
        return number

    @field_validator("live_feed")
    @classmethod
    def _validate_live_feed(cls, value: str) -> str:
        if value not in ALLOWED_LIVE_FEEDS:
            allowed = ", ".join(sorted(ALLOWED_LIVE_FEEDS))
            raise ValueError(f"must be one of: {allowed}")
        return value

    @field_validator("include_event_types", "monitor_groups", mode="before")
    @classmethod
    def _validate_csv(cls, value: object) -> object:
        return _parse_csv(value)

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        normalized = value.strip().upper()
        if normalized not in LOG_LEVELS:
            allowed = ", ".join(sorted(LOG_LEVELS))
            raise ValueError(f"must be one of: {allowed}")
        return normalized


def load_settings(env_file: str | Path | None = ".env") -> Settings:
    """Load settings, converting validation failures into :class:`ConfigError`.

    ``env_file=None`` disables the optional dotenv file, which tests use to keep
    the process environment authoritative.
    """
    try:
        # pydantic-settings accepts `_env_file` at runtime but its stubs omit it.
        return Settings(_env_file=env_file)  # type: ignore[call-arg]
    except ValidationError as error:
        raise ConfigError(_format_validation_error(error)) from error
