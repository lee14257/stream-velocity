import time
from stream_velocity import VelocityTracker

def run_benchmark(event_count=100000):
    tracker = VelocityTracker(window_seconds=300, bucket_seconds=5)
    entity_id = "bench_user_1"
    start_time = time.time()

    print(f"--- Running stream-velocity benchmark: {event_count:,} events ---")
    
    t0 = time.perf_counter()
    for i in range(event_count):
        tracker.record_event(entity_id=entity_id, amount=10.0, timestamp=start_time + (i * 0.001))
    t1 = time.perf_counter()

    write_total_ms = (t1 - t0) * 1000
    avg_write_us = (write_total_ms / event_count) * 1000

    t2 = time.perf_counter()
    metrics = tracker.get_velocity(entity_id=entity_id, timestamp=start_time + 100)
    t3 = time.perf_counter()

    query_latency_ms = (t3 - t2) * 1000

    print(f"Total write time for {event_count:,} events: {write_total_ms:.2f} ms")
    print(f"Average write latency per event: {avg_write_us:.2f} µs")
    print(f"Read query latency: {query_latency_ms:.4f} ms")
    print(f"Aggregation result: {metrics['count']} events, ${metrics['sum_amount']:,.2f}")

    assert query_latency_ms < 5.0, f"SLA Violation: Query took {query_latency_ms:.2f}ms (target <5ms)"
    print("SLA Target Passed: Query execution < 5.0 ms")

if __name__ == "__main__":
    run_benchmark()
