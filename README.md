# stream-velocity

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![CI](https://img.shields.io/github/actions/workflow/status/lee14257/stream-velocity/test.yml?branch=main)](https://github.com/lee14257/stream-velocity/actions/workflows/test.yml)

`stream-velocity` is a small, dependency-free Python library that answers one question in real time: **"how many events happened in the last few minutes, and how much did they total?"** — per account, card, or any other entity.

It exists because small fraud and risk teams often need this exact check but can't justify a Redis cluster or a $10k/month streaming platform. `stream-velocity` gives them a fast, in-process alternative that uses a fixed amount of memory no matter how much data flows through it.

## Installation

```bash
pip install -e .
```

> A public PyPI release is in progress. Until then, install from source as shown above.

## Quick Start

```python
from stream_velocity import VelocityTracker

# Track a 5-minute window, split into 5-second buckets
tracker = VelocityTracker(window_seconds=300, bucket_seconds=5)

# Record incoming events for an account
tracker.record_event(entity_id="acc_990142", amount=150.00)
tracker.record_event(entity_id="acc_990142", amount=45.50)
tracker.record_event(entity_id="acc_990142", amount=210.00)

# Read the current rolling totals
velocity = tracker.get_velocity("acc_990142")

print(velocity.count)        # 3
print(velocity.sum_amount)   # 405.5
```

## API Reference

### `VelocityTracker`

The main entry point. Tracks rolling velocity across many entities.

```python
VelocityTracker(
    window_seconds=300,
    bucket_seconds=5,
    idle_timeout=None,
    cleanup_interval=60.0,
)
```

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `window_seconds` | `int` | `300` | Length of the rolling window, in seconds. |
| `bucket_seconds` | `int` | `5` | Width of each bucket, in seconds. `window_seconds` must be evenly divisible by `bucket_seconds`. |
| `idle_timeout` | `float` or `None` | `None` | Drop an entity if it records no events for this many seconds. `None` disables eviction. Should be at least `window_seconds`. |
| `cleanup_interval` | `float` | `60.0` | How often (seconds) the idle sweep runs automatically. |

**Methods**

- `record_event(entity_id, amount=0.0, timestamp=None)` → `None` — record one event.
  - `timestamp` defaults to the current time; pass an explicit value for replay or testing.
- `get_velocity(entity_id, timestamp=None)` → `VelocityResult` — current rolling totals for an entity. Returns zeros if the entity is unknown or has been evicted.
- `cleanup(now=None)` → `None` — immediately drop entities idle longer than `idle_timeout`. The sweep also runs automatically every `cleanup_interval` seconds.
- `len(tracker)` → `int` — number of entities currently tracked.

### `VelocityResult`

A frozen (immutable) dataclass returned by `get_velocity`.

| Field | Type | Description |
| --- | --- | --- |
| `entity_id` | `str` | The entity the result is for. |
| `window_seconds` | `int` | The window length used. |
| `count` | `int` | Number of events in the window. |
| `sum_amount` | `float` | Sum of `amount` values in the window. |

### `BucketedRingBuffer` (lower-level)

The underlying single-entity counter. Prefer `VelocityTracker` unless you need to manage one entity manually.

```python
buffer = BucketedRingBuffer(window_seconds=300, bucket_seconds=5)
buffer.record(timestamp=1_000_000.0, amount=10.0)
metrics = buffer.get_metrics(current_timestamp=1_000_010.0)  # {"window_seconds": 300, "count": 1, "sum_amount": 10.0}
```

## Features

- **No dependencies** — pure Python 3.9+, no database, no C extensions.
- **Always fast** — updates and lookups take the same time whether you process 10 or 10 million events (this is what "O(1)" means).
- **Fixed memory** — counters are created once and reused forever, so memory use stays flat no matter how many events arrive.
- **Bounded tracking** — set `idle_timeout` to drop idle entities automatically, so millions of distinct accounts don't grow memory forever.
- **Low latency** — reads stay under 5 ms while processing 100,000+ events.

## How It Works

A rolling window is split into a fixed number of buckets. For a 5-minute window with 5-second buckets, that's 60 buckets.

- **Finding the bucket** — an event's timestamp maps to a bucket with one division: `slot = (timestamp // bucket_seconds) % num_buckets`.
- **Reusing buckets** — as time passes, expired buckets are wiped clean and reused, so the total number of buckets never changes.
- **Running totals** — two totals (count and amount) are kept up to date on every write, so reading the answer means just returning the totals — no scanning.

```
 events ──► slot = (timestamp // 5) % 60
              │
              ▼
   ┌───┬───┬───┬───┬───┬───┬───┐
   │ 0 │ 1 │ 2 │...│ 57│ 58│ 59│  ◄── fixed ring of buckets, reused forever
   └───┴───┴───┴───┴───┴───┴───┘
```

Only counts and sums are stored — no raw transaction details.

## Benchmarks

### Latency

```bash
python3 tests/benchmark_latency.py
```

Representative output (Apple M-series MacBook Air, Python 3.10). Absolute timings vary by machine; the `< 5.0 ms` SLA is enforced in CI on every push.

```
--- Running stream-velocity benchmark: 100,000 events ---
Total write time for 100,000 events: 61.88 ms
Average write latency per event: 0.62 µs
Read query latency: 0.0418 ms
Aggregation result: 100000 events, $1,000,000.00
SLA Target Passed: Query execution < 5.0 ms
```

### Memory

```bash
python3 tests/benchmark_memory.py
```

The library creates its counters once and reuses them, so memory stays flat as events pile up. A typical 5-minute window uses only a few kilobytes — far below the 100 MB ceiling a lightweight fraud service can run within.

## Docker

A minimal Alpine image installs the library and runs the speed benchmark:

```bash
docker build -t stream-velocity .
docker run --rm stream-velocity
```

## Development

Run the test suite and benchmarks:

```bash
python3 -m unittest discover -s tests
python3 tests/benchmark_latency.py
python3 tests/benchmark_memory.py
```

## License

MIT. See [LICENSE](LICENSE).
