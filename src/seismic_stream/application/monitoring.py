"""Lag monitor use case (S14).

The monitor turns broker offset metadata into the two gauges the Pipeline Health
dashboard reads: committed consumer lag per group/topic and the current end
offset per topic. It depends only on the :class:`LagReader` port and the
:class:`Metrics` port, so the arithmetic and the polling loop are unit tested
without a broker.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from seismic_stream.application.ports import Clock, LagReader, Metrics


@dataclass(frozen=True, slots=True)
class LagSnapshot:
    """One poll's lag and end-offset readings, keyed for assertions and logs."""

    lags: Mapping[tuple[str, str], int]
    end_offsets: Mapping[str, int]


def topic_end_offsets(
    ends: Mapping[tuple[str, int], int],
    topics: Sequence[str],
) -> dict[str, int]:
    """Sum every partition's end offset per topic, defaulting absent topics to 0."""
    totals = dict.fromkeys(topics, 0)
    for (topic, _partition), offset in ends.items():
        if topic in totals:
            totals[topic] += max(0, offset)
    return totals


def compute_lags(
    end_offsets: Mapping[str, int],
    committed: Mapping[tuple[str, int], int],
    *,
    group: str,
    topics: Sequence[str],
) -> dict[tuple[str, str], int]:
    """Return non-negative lag per group/topic from summed end and committed offsets.

    Kafka reports ``-1001`` for a partition with no committed offset; treating it
    as zero makes the lag equal to the full backlog, which is what an operator
    expects for a group that has never consumed a topic.
    """
    lags: dict[tuple[str, str], int] = {}
    for topic in topics:
        end = end_offsets.get(topic, 0)
        consumed = sum(
            max(0, offset)
            for (committed_topic, _partition), offset in committed.items()
            if committed_topic == topic
        )
        lags[(group, topic)] = max(0, end - consumed)
    return lags


class LagMonitor:
    """Poll broker offsets on an interval and publish the lag gauge catalog."""

    def __init__(
        self,
        reader: LagReader,
        metrics: Metrics,
        *,
        groups: Sequence[str],
        topics: Sequence[str],
        interval_seconds: float = 5.0,
        clock: Clock | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        self._reader = reader
        self._metrics = metrics
        self._groups = tuple(groups)
        self._topics = tuple(topics)
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._on_error = on_error

    def poll_once(self) -> LagSnapshot:
        """Read every configured group and publish lag plus topic end offsets."""
        end_by_topic = topic_end_offsets(self._reader.end_offsets(), self._topics)

        lags: dict[tuple[str, str], int] = {}
        for group in self._groups:
            committed = self._reader.consumer_offsets(group)
            group_lags = compute_lags(
                end_by_topic,
                committed,
                group=group,
                topics=self._topics,
            )
            lags.update(group_lags)
            for (_group, topic), lag in group_lags.items():
                self._metrics.set_consumer_lag(group, topic, lag)

        for topic, offset in end_by_topic.items():
            self._metrics.set_topic_end_offset(topic, offset)

        return LagSnapshot(lags=lags, end_offsets=end_by_topic)

    def run(self, stop: threading.Event) -> int:
        """Poll until ``stop`` is set, sleeping one interval between attempts.

        A transient broker error is reported through ``on_error`` and does not
        stop the loop; without a callback it propagates so misconfiguration is
        not hidden.
        """
        if self._clock is None:
            raise RuntimeError("LagMonitor.run requires a clock")
        polls = 0
        while not stop.is_set():
            try:
                self.poll_once()
            except Exception as error:
                if self._on_error is None:
                    raise
                self._on_error(error)
            polls += 1
            if stop.is_set():
                break
            self._clock.sleep(self._interval_seconds)
        return polls
