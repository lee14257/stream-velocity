# stream-velocity

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)

`stream-velocity` is a small, fast Python library that counts how many transactions happen in a rolling time window (for example, "the last 5 minutes"), and how much they total. It has no dependencies and keeps its memory use fixed no matter how much data flows through it, which makes it a good fit for fraud-detection and rate-limiting checks in real-time payment and e-commerce systems.

---

## Real-World Problem & Tradeoffs

A fraud system constantly needs to answer *"has account X made more than 5 payments in the last 5 minutes?"* — instantly, over and over, as events stream in.

The usual ways to solve this, and why they hurt:

1. **Ask the database each time** (`SELECT COUNT(*)`): simple, but every query takes 50–100 ms and gets slower as the table grows.
2. **Keep every event in a growing list** (`collections.deque`): works until the list gets big, then memory use keeps growing and lookups slow down.
3. **Run a big streaming cluster** (Kafka/Flink/Redis): powerful, but adds significant monthly cost and needs a team to operate — too much for a small fintech.

What this library does instead: keep a fixed set of small counters plus two running totals. Recording an event is one quick update; reading the answer is just returning the totals. Fast (under 2 ms), tiny memory (a few KB), no server required.

---

## Key Features

* **No dependencies:** Pure Python 3.9+ only — no database, no C extensions.
* **Always fast:** Updates and lookups take the same time whether you process 10 or 10 million events (this is what "O(1)" means).
* **Fixed memory:** The counters are created once up front and reused forever, so memory use stays the same no matter how many events arrive.
* **Bounded tracking:** Optionally drop accounts that have been idle for a while, so tracking millions of distinct accounts doesn't grow memory forever.
* **Low latency:** Reads stay under 5 ms even while processing 100,000+ events.

---

## Installation

```bash
pip install -e .
```

> **Note:** A public PyPI release is in progress. Until then, install directly from source as shown above.

## Quick Start

```python
from stream_velocity import VelocityTracker

# Initialize tracker with a 5-minute rolling window (300s) divided into 5-second buckets
tracker = VelocityTracker(window_seconds=300, bucket_seconds=5)

# Record incoming real-time payment events
account_id = "acc_990142"

tracker.record_event(entity_id=account_id, amount=150.00)
tracker.record_event(entity_id=account_id, amount=45.50)
tracker.record_event(entity_id=account_id, amount=210.00)

# Get the current rolling counts and sums
velocity = tracker.get_velocity(account_id)

print(f"Entity ID: {velocity.entity_id}")
print(f"5-Minute Transaction Count: {velocity.count}")
print(f"5-Minute Volume: ${velocity.sum_amount:,.2f}")
```

## How It Works

A rolling window is split into a fixed number of small buckets (for example, a 5-minute window split into 60 buckets of 5 seconds each).

- **Finding the right bucket:** each event's timestamp is turned into a bucket number with simple division: `slot_idx = (timestamp // bucket_seconds) % num_buckets`. One arithmetic step, no searching.
- **Reusing old buckets:** when time moves past a bucket, its counters are wiped clean and reused, so the total number of buckets never changes.
- **Running totals:** the library keeps a running total of the count and amount. Recording an event adds to the totals; when a bucket expires, its value is subtracted. Reading the answer means just returning the two totals.
- **Only numbers are stored:** the library keeps only counts and sums — no raw transaction details are ever stored.

```
[ Incoming Event Stream ] ──► Slot Index Math: (ts // 5) % 60
                                      │
                                      ▼
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ 0 │ 1 │ 2 │...│ 57│ 58│ 59│ 0 │ 1 │ 2 │...│ 59│  ◄── Pre-allocated Ring Array
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
  ▲                       ▲
  │ (Overwritten In-Place)│ (Active Window Slot)
```

## Performance & Benchmarking

Run the automated latency SLA benchmark suite locally:

```bash
python3 tests/benchmark_latency.py
```

### How much memory does it use?

Run the memory benchmark to confirm the library uses a fixed amount of memory:

```bash
python3 tests/benchmark_memory.py
```

The library creates its counters once when it starts and reuses them as time
moves forward, so it never allocates more memory as events pile up. A typical
5-minute window uses only a few kilobytes — far below the 100 MB ceiling a
lightweight fraud service can run within.

For a long-running pipeline that sees millions of distinct accounts, pass
`idle_timeout` so idle accounts are dropped automatically:

```python
tracker = VelocityTracker(window_seconds=300, bucket_seconds=5, idle_timeout=300)
```

An account is removed once it has recorded no events for `idle_timeout`
seconds; by then its window is empty anyway, so nothing is lost.

### Benchmark Output (representative local run)

The following output was captured on an Apple M-series MacBook Air running Python 3.10.
Absolute timings vary by machine; the SLA assertion (`< 5.0 ms`) is enforced in CI on every push.

```
--- Running stream-velocity benchmark: 100,000 events ---
Total write time for 100,000 events: 61.88 ms
Average write latency per event: 0.62 µs
Read query latency: 0.0418 ms
Aggregation result: 100000 events, $1,000,000.00
SLA Target Passed: Query execution < 5.0 ms
```

## Running with Docker

A minimal image is provided that installs the library and runs the speed
benchmark:

```bash
docker build -t stream-velocity .
docker run --rm stream-velocity
```

The image is built on Alpine Linux so it stays small.

## Running Unit Tests

```bash
python3 -m unittest discover -s tests
```

## License

Distributed under the MIT License. See LICENSE for more information.
