"""System wall clock adapter for replay pacing."""

from __future__ import annotations

import time


class SystemClock:
    """Monotonic system clock; safe against wall-clock jumps."""

    def now(self) -> float:
        """Return monotonic seconds."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds``; negative requests are ignored."""
        if seconds > 0:
            time.sleep(seconds)
