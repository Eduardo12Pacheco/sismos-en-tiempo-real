"""Kafka consumer configuration shared by the sink and monitor adapters.

The batch consumer itself is implemented together with the sink, where batching,
pause/resume and manual commits belong (D9). This skeleton fixes the consumer
defaults in one place so every consumer starts from the same delivery contract.
"""

from __future__ import annotations

from typing import Any, Final

AUTO_OFFSET_RESET: Final = "earliest"


def consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    """Return confluent-kafka consumer settings with manual commits enabled."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.partition.eof": False,
    }
