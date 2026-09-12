# stream-velocity

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)

`stream-velocity` is a zero-dependency, bounded-memory Python library designed for real-time transaction velocity aggregations across sliding time windows. Built specifically for low-latency feature evaluation in financial risk processing pipelines, credit card fraud engines, and payment security middleware.

---

## Real-World Problem & Tradeoffs

Evaluating velocity rules (e.g., *"Has account X executed >5 payments in the last 300 seconds?"*) during real-time transaction processing typically presents three architectural compromises:

1. **Database Queries (`SELECT COUNT(*)`):** Scanning historical transaction rows in traditional databases introduces 50–100 ms query latencies, risking checkout timeouts under heavy load.
2. **Unbounded In-Memory Lists (`collections.deque`):** Storing raw event timestamps in dynamic arrays causes O(N) iteration delays and unbounded RAM growth, leading to memory leaks and Python garbage collection pauses during high-volume bot attacks.
3. **Heavy Streaming Infrastructure (Kafka/Flink/Redis Clusters):** Deploying distributed streaming clusters provides scale but adds significant financial overhead ($10k+/month) and infrastructure maintenance burden for early-stage fintechs.

`stream-velocity` provides an embedded alternative: an **O(1) time-complexity, memory-bounded ring buffer** that computes rolling counts and sums in under **2 milliseconds** without external database dependencies.

---

## Key Features

* **Zero Dependencies:** Pure Python 3.9+ with zero external database or C-extension requirements.
* **O(1) Execution:** Constant-time updates and queries using fixed array index arithmetic regardless of stream throughput.
* **Bounded RAM Footprint:** Pre-allocated integer/float primitives prevent memory fragmentation and eliminate dynamic array expansion.
* **Low Latency SLA:** Guaranteed sub-5ms read execution under sustained throughputs of 100,000+ synthetic events.

---

## Installation

```bash
pip install stream-velocity
```

## Quick Start

```python
import time
from stream_velocity import VelocityTracker

# Initialize tracker with a 5-minute sliding window (300s) divided into 5-second sub-buckets
tracker = VelocityTracker(window_seconds=300, bucket_seconds=5)

# Record incoming real-time payment events
account_id = "acc_990142"

tracker.record_event(entity_id=account_id, amount=150.00)
tracker.record_event(entity_id=account_id, amount=45.50)
tracker.record_event(entity_id=account_id, amount=210.00)

# Retrieve instant velocity metrics (<2ms latency)
velocity = tracker.get_velocity(account_id)

print(f"Entity ID: {velocity['entity_id']}")
print(f"5-Minute Transaction Count: {velocity['count']}")
print(f"5-Minute Volume: ${velocity['sum_amount']:,.2f}")
print(f"Execution Latency: {velocity['execution_latency_ms']} ms")
```

## Architecture & Data Structure

`stream-velocity` uses a Bucketed Ring Buffer algorithm:

- **Sub-Bucket Allocation:** A time window W (e.g., 300 seconds) is divided into N fixed sub-buckets B (e.g., 60 buckets of 5 seconds each).
- **Slot Computation:** Incoming event timestamps map directly to an array index via integer division: `slot_idx = (timestamp // bucket_seconds) % num_buckets`.
- **Automatic Stale Data Eviction:** When a slot is accessed from a previous window iteration, stale counters are reset in-place without dynamic memory allocation or deletion (malloc/free).
- **Data Reduction:** Raw transaction metadata (PII, cards, IP addresses) are discarded immediately after incrementing numeric aggregate counters.

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

### Benchmark Output Target

```
--- Running stream-velocity benchmark: 100,000 events ---
Total write time for 100,000 events: 82.40 ms
Average write latency per event: 0.82 µs
Read query latency: 0.3810 ms
Aggregation result: 100000 events, $1,000,000.00
SLA Target Passed: Query execution < 5.0 ms
```

## Running Unit Tests

```bash
python3 -m unittest discover -s tests
```

## License

Distributed under the MIT License. See LICENSE for more information.
