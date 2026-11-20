"""Schema Registry administration for versioned Avro contracts.

Only ``bootstrap`` registers schemas; services configure their serializers with
``auto.register.schemas=false`` so an accidental schema change fails fast instead
of silently creating a new version (D4).
"""

from __future__ import annotations

import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError

COMPATIBILITY_LEVEL = "BACKWARD"
AVRO_TYPE = "AVRO"


class SchemaConflictError(RuntimeError):
    """Raised when a subject exists with a different schema than the bundled one."""


def _is_not_found(error: SchemaRegistryError) -> bool:
    """Return whether an error means a subject or its config does not exist yet."""
    return bool(error.http_status_code == 404 or error.error_code in {40401, 40408})


def _schemas_match(registered: str, expected: str) -> bool:
    """Compare schemas structurally so whitespace and key order do not matter."""
    try:
        return bool(json.loads(registered) == json.loads(expected))
    except json.JSONDecodeError:
        return registered.strip() == expected.strip()


class SchemaRegistryAdmin:
    """Idempotent registration, compatibility and compatibility checks."""

    def __init__(self, client: SchemaRegistryClient) -> None:
        self._client = client

    def ensure_subject(self, subject: str, schema: str) -> str:
        """Register ``schema`` when absent, otherwise verify it is identical.

        Returns ``"created"`` or ``"unchanged"``. A divergent existing schema is
        an error: contracts evolve only through a reviewed version bump.
        """
        try:
            registered = self._client.get_latest_version(subject)
        except SchemaRegistryError as error:
            if not _is_not_found(error):
                raise
            self._client.register_schema(subject, Schema(schema, AVRO_TYPE))
            return "created"

        if not _schemas_match(registered.schema.schema_str, schema):
            raise SchemaConflictError(
                f"subject {subject!r} already has a different schema; "
                "bump the schema version instead of changing it in place"
            )
        return "unchanged"

    def set_compatibility(self, subject: str, level: str = COMPATIBILITY_LEVEL) -> str:
        """Set the subject compatibility level, reporting ``updated``/``unchanged``."""
        try:
            current = self._client.get_compatibility(subject)
        except SchemaRegistryError as error:
            if not _is_not_found(error):
                raise
            current = None
        if current == level:
            return "unchanged"
        self._client.set_compatibility(subject_name=subject, level=level)
        return "updated"

    def check_compatibility(self, subject: str, schema: str) -> bool:
        """Return whether ``schema`` is compatible with the latest subject version."""
        return bool(
            self._client.test_compatibility(
                subject_name=subject,
                schema=Schema(schema, AVRO_TYPE),
            )
        )
