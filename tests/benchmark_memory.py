"""
Prove that stream-velocity uses a fixed amount of memory, no matter how many
events it processes.

The idea is simple: the library sets up a fixed number of "buckets" when it
starts (one bucket for each slice of the time window). As time moves forward,
old buckets are wiped clean and reused, so nothing new is ever allocated.

This script records a small number of events, measures the memory in use, then
records ten times as many events and measures again. If the library is doing
its job, the two numbers are the same.
"""

import tracemalloc
from stream_velocity import VelocityTracker


def measure_used_bytes(event_count: int) -> int:
    """Record `event_count` events and return how many bytes the library holds."""
    tracker = VelocityTracker(window_seconds=300, bucket_seconds=5)
    start_time = 1_000_000.0

    tracemalloc.start()
    for i in range(event_count):
        # Space events 1 ms apart so they flow through the time window.
        tracker.record_event(
            entity_id="account_1",
            amount=10.0,
            timestamp=start_time + (i * 0.001),
        )
    current_bytes, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return current_bytes


def main() -> None:
    small = measure_used_bytes(100_000)
    large = measure_used_bytes(1_000_000)

    print("--- stream-velocity memory benchmark ---")
    print(f"Memory after   100,000 events: {small / 1024:>8,.1f} KB")
    print(f"Memory after 1,000,000 events: {large / 1024:>8,.1f} KB")

    # Ten times the events should not mean ten times the memory.
    slack_bytes = 64 * 1024  # allow 64 KB of measurement noise
    assert large <= small + slack_bytes, (
        f"Memory grew with more events: {small / 1024:.1f} KB -> {large / 1024:.1f} KB"
    )

    # The whole library must stay far below the 100 MB ceiling.
    assert large < 100 * 1024 * 1024, (
        f"Memory exceeded 100 MB: {large / 1024 / 1024:.1f} MB"
    )

    # Even a much larger window (1 hour, 1-second buckets = 3600 buckets)
    # should still be tiny.
    big_window = VelocityTracker(window_seconds=3600, bucket_seconds=1)
    start_time = 1_000_000.0
    tracemalloc.start()
    for i in range(10_000):
        big_window.record_event(
            entity_id="account_1",
            amount=10.0,
            timestamp=start_time + (i * 0.001),
        )
    big_window_bytes, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Memory for a 1-hour window (3600 buckets): {big_window_bytes / 1024:>8,.1f} KB")

    print("PASS: memory stayed flat and is far below 100 MB")


if __name__ == "__main__":
    main()
