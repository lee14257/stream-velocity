import time
from dataclasses import dataclass
from typing import Dict, Optional
from .ring_buffer import BucketedRingBuffer


@dataclass(frozen=True)
class VelocityResult:
    """A snapshot of one entity's rolling-window velocity at a point in time."""

    entity_id: str
    window_seconds: int
    count: int
    sum_amount: float


class VelocityTracker:
    """
    In-memory manager that tracks rolling velocity across many entities.

    Memory is reclaimed lazily: when `idle_timeout` is set, any entity that has
    not recorded an event for `idle_timeout` seconds is dropped on the next
    `cleanup()` call or the periodic automatic sweep. This keeps the registry
    bounded instead of growing forever with distinct entities.

    `idle_timeout` should be at least as large as `window_seconds`; by the time
    an entity has been idle for a full window, its counters are already empty,
    so dropping it loses no current-velocity information.
    """

    def __init__(
        self,
        window_seconds: int = 300,
        bucket_seconds: int = 5,
        idle_timeout: Optional[float] = None,
        cleanup_interval: float = 60.0,
    ):
        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle_timeout must be greater than 0")

        self.window_seconds = window_seconds
        self.bucket_seconds = bucket_seconds
        self.idle_timeout = idle_timeout
        self.cleanup_interval = cleanup_interval

        self.entities: Dict[str, BucketedRingBuffer] = {}
        self._last_seen: Dict[str, float] = {}
        self._last_cleanup: float = 0.0

    def __len__(self) -> int:
        return len(self.entities)

    def record_event(
        self,
        entity_id: str,
        amount: float = 0.0,
        timestamp: Optional[float] = None,
    ) -> None:
        if timestamp is None:
            timestamp = time.time()

        self._maybe_cleanup(timestamp)

        ring = self.entities.get(entity_id)
        if ring is None:
            ring = BucketedRingBuffer(
                window_seconds=self.window_seconds,
                bucket_seconds=self.bucket_seconds,
            )
            self.entities[entity_id] = ring

        ring.record(timestamp=timestamp, amount=amount)
        self._last_seen[entity_id] = timestamp

    def get_velocity(
        self,
        entity_id: str,
        timestamp: Optional[float] = None,
    ) -> VelocityResult:
        if timestamp is None:
            timestamp = time.time()

        self._maybe_cleanup(timestamp)

        ring = self.entities.get(entity_id)
        if ring is None:
            return VelocityResult(
                entity_id=entity_id,
                window_seconds=self.window_seconds,
                count=0,
                sum_amount=0.0,
            )

        metrics = ring.get_metrics(current_timestamp=timestamp)
        return VelocityResult(
            entity_id=entity_id,
            window_seconds=metrics["window_seconds"],
            count=metrics["count"],
            sum_amount=metrics["sum_amount"],
        )

    def cleanup(self, now: Optional[float] = None) -> None:
        """Drop entities that have been idle longer than `idle_timeout`."""
        if self.idle_timeout is None:
            return
        if now is None:
            now = time.time()

        expired = [
            entity_id
            for entity_id, seen in self._last_seen.items()
            if now - seen > self.idle_timeout
        ]
        for entity_id in expired:
            del self.entities[entity_id]
            del self._last_seen[entity_id]

        self._last_cleanup = now

    def _maybe_cleanup(self, now: float) -> None:
        if self.idle_timeout is None:
            return
        if now - self._last_cleanup < self.cleanup_interval:
            return
        self.cleanup(now)
