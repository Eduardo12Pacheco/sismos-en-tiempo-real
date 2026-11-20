"""Command-line entry point for the seismic stream pipeline."""

from __future__ import annotations

import seismic_stream  # noqa: F401  # isort: skip - must load warning filters first
import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryError

from seismic_stream import __version__
from seismic_stream.adapters.kafka.bootstrap import run as run_bootstrap
from seismic_stream.adapters.kafka.producer import DeliveryError
from seismic_stream.adapters.kafka.registry import SchemaConflictError
from seismic_stream.adapters.observability.logging import configure_logging
from seismic_stream.adapters.observability.server import (
    ObservabilityServer,
    Readiness,
    ShutdownSignal,
)
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.application.ingest import IngestSummary
from seismic_stream.application.ports import SourceError
from seismic_stream.composition import IngestSession, build_ingest_session
from seismic_stream.config import ConfigError, Settings, load_settings

app = typer.Typer(
    name="seismic-stream",
    help="Local real-time earthquake streaming pipeline with offline replay.",
    no_args_is_help=True,
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"seismic-stream {__version__}")
        raise typer.Exit


def _load_settings() -> Settings:
    """Load settings or exit with the documented usage/config error code."""
    try:
        return load_settings()
    except ConfigError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(2) from exc


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the installed version and exit.",
    ),
) -> None:
    """Local real-time earthquake streaming pipeline with offline replay."""


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(f"seismic-stream {__version__}")


@app.command()
def bootstrap() -> None:
    """Create topics, register Avro schemas and enforce BACKWARD compatibility."""
    settings = _load_settings()
    configure_logging("bootstrap", settings.log_level, settings.log_format)
    try:
        result = run_bootstrap(settings)
    except (KafkaException, SchemaRegistryError, SchemaConflictError) as exc:
        typer.echo(f"error: bootstrap failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result.as_dict(), sort_keys=True))


@app.command()
def ingest(  # noqa: PLR0917 - Typer exposes one parameter per CLI option
    source: Annotated[
        str | None,
        typer.Option(
            "--source",
            help="Event source to ingest: replay (bundled sample) or usgs (live feed).",
            metavar="[replay|usgs]",
        ),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            help="Recorded GeoJSON/NDJSON file for replay mode (overrides the setting).",
            metavar="PATH",
        ),
    ] = None,
    speed: Annotated[
        str | None,
        typer.Option(
            "--speed",
            help="Replay compression multiplier (> 0) or 'max' to disable pacing.",
            metavar="N|max",
        ),
    ] = None,
    chaos: Annotated[
        bool | None,
        typer.Option(
            "--chaos/--no-chaos",
            help="Inject malformed copies, duplicates and revisions deterministically.",
        ),
    ] = None,
    seed: Annotated[
        int | None,
        typer.Option("--seed", help="Seed for the deterministic chaos policy."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Stop after reading N source records.", metavar="N"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate and count records with in-memory publishers; no Kafka.",
        ),
    ] = False,
) -> None:
    """Stream recorded or live USGS events into Kafka, dead-lettering invalid ones."""
    settings = _load_settings()
    source_name = (source or settings.ingest_source).strip().lower()
    if source_name not in ("replay", "usgs"):
        raise typer.BadParameter("must be 'replay' or 'usgs'", param_hint="--source")
    if limit is not None and limit <= 0:
        raise typer.BadParameter("must be a positive integer", param_hint="--limit")
    resolved_speed = settings.replay_speed if speed is None else _parse_speed(speed)
    use_chaos = settings.replay_chaos if chaos is None else chaos
    chaos_seed = settings.chaos_seed if seed is None else seed
    replay_file = file if file is not None else settings.replay_file

    configure_logging("ingest", settings.log_level, settings.log_format)
    shutdown = ShutdownSignal()
    server: ObservabilityServer | None = None
    session: IngestSession | None = None
    summary = IngestSummary(published=0, filtered=0, invalid=0, duration_seconds=0.0)
    try:
        with shutdown:
            session = build_ingest_session(
                settings,
                source_name=source_name,
                replay_file=replay_file,
                speed=resolved_speed,
                chaos=use_chaos,
                seed=chaos_seed,
                limit=limit,
                dry_run=dry_run,
                stop=shutdown.stop_event,
            )
            if not dry_run:
                server = ObservabilityServer(
                    settings.metrics_port,
                    session.metrics.registry,
                    lambda: Readiness(ready=True),
                )
                server.start()
            summary = session.service.run(shutdown.stop_event)
    except DeliveryError as exc:
        typer.echo(f"error: ingest delivery failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    except (KafkaException, SchemaRegistryError, SourceError) as exc:
        typer.echo(f"error: ingest failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    finally:
        if server is not None:
            server.stop()
        if session is not None:
            session.shutdown()

    typer.echo(json.dumps(summary.as_dict(), sort_keys=True))


def _parse_speed(raw: str) -> float | None:
    """Parse ``--speed`` into a finite multiplier or ``None`` for ``max``."""
    text = raw.strip()
    if text.lower() == "max":
        return None
    try:
        value = float(text)
    except ValueError:
        raise typer.BadParameter(
            "must be a finite number greater than 0 or 'max'", param_hint="--speed"
        ) from None
    if not math.isfinite(value) or value <= 0:
        raise typer.BadParameter(
            "must be a finite number greater than 0 or 'max'", param_hint="--speed"
        )
    return value


@app.command()
def replay_info(
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            help="Recorded GeoJSON FeatureCollection or NDJSON file to inspect.",
            metavar="PATH",
        ),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print the summary as a single JSON object."),
    ] = False,
) -> None:
    """Show the profile and checksum status of a recorded replay sample."""
    if file is None:
        file = _load_settings().replay_file
    try:
        source = ReplaySource(file)
    except SourceError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(1) from exc

    summary = _summarize(file, source)
    if as_json:
        typer.echo(json.dumps(summary, sort_keys=True))
    else:
        typer.echo(_render_summary(summary))

    if summary["checksum"] == "mismatch":
        raise typer.Exit(1)


def _summarize(file: Path, source: ReplaySource) -> dict[str, Any]:
    types: Counter[str] = Counter()
    timestamps: list[int] = []
    for record in source.records():
        types[_event_type(record.feature)] += 1
        if record.event_time_ms is not None:
            timestamps.append(record.event_time_ms)

    digest = _sha256(file)
    return {
        "file": str(file),
        "features": len(source),
        "types": dict(sorted(types.items(), key=lambda item: (-item[1], item[0]))),
        "first_event_time": _iso_utc(min(timestamps)) if timestamps else None,
        "last_event_time": _iso_utc(max(timestamps)) if timestamps else None,
        "sha256": digest,
        "checksum": _checksum_status(file, digest),
    }


def _event_type(feature: Mapping[str, Any]) -> str:
    properties = feature.get("properties")
    if isinstance(properties, Mapping):
        value = properties.get("type")
        if isinstance(value, str):
            return value
    return "unknown"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checksum_status(path: Path, digest: str) -> str:
    manifest = path.parent / "SHA256SUMS"
    if not manifest.exists():
        return "not_available"
    try:
        lines = manifest.read_text(encoding="utf-8").splitlines()
    except OSError:  # pragma: no cover - defensive: unreadable manifest
        return "not_available"
    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            continue
        recorded = parts[0].lower()
        name = parts[1].lstrip("*")
        if Path(name).name != path.name:
            continue
        return "ok" if recorded == digest.lower() else "mismatch"
    return "not_available"


def _iso_utc(event_time_ms: int) -> str:
    moment = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC)
    return moment.isoformat(timespec="milliseconds")


def _render_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"file: {summary['file']}",
        f"features: {summary['features']}",
        "types:",
    ]
    lines.extend(f"  {name}: {count}" for name, count in summary["types"].items())
    lines.extend(
        [
            f"first_event_time: {summary['first_event_time']}",
            f"last_event_time: {summary['last_event_time']}",
            f"sha256: {summary['sha256']}",
            f"checksum: {_checksum_label(str(summary['checksum']))}",
        ]
    )
    return "\n".join(lines)


def _checksum_label(status: str) -> str:
    return {
        "ok": "OK",
        "mismatch": "MISMATCH",
        "not_available": "not available",
    }.get(status, status)
