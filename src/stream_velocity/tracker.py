import time
from typing import Dict, Any
from .ring_buffer import BucketedRingBuffer

class VelocityTracker:
    """
    In-memory stateful entity manager for tracking transaction velocity across entities.
    """
    def __init__(self, window_seconds: int = 300, bucket_seconds: int = 5):
        self.window_seconds = window_seconds
        self.bucket_seconds = bucket_seconds
        self.entities: Dict[str, BucketedRingBuffer] = {}

    def record_event(self, entity_id: str, amount: float = 0.0, timestamp: float = None) -> None:
        if timestamp is None:
            timestamp = time.time()

        if entity_id not in self.entities:
            self.entities[entity_id] = BucketedRingBuffer(
                window_seconds=self.window_seconds,
                bucket_seconds=self.bucket_seconds
            )

        self.entities[entity_id].record(timestamp=timestamp, amount=amount)

    def get_velocity(self, entity_id: str, timestamp: float = None) -> Dict[str, Any]:
        start_time = time.perf_counter()
        if timestamp is None:
            timestamp = time.time()

        if entity_id not in self.entities:
            return {
                "entity_id": entity_id,
                "window_seconds": self.window_seconds,
                "count": 0,
                "sum_amount": 0.0,
                "execution_latency_ms": round((time.perf_counter() - start_time) * 1000, 4)
            }

        metrics = self.entities[entity_id].get_metrics(current_timestamp=timestamp)
        latency_ms = (time.perf_counter() - start_time) * 1000

        return {
            "entity_id": entity_id,
            "window_seconds": metrics["window_seconds"],
            "count": metrics["count"],
            "sum_amount": metrics["sum_amount"],
            "execution_latency_ms": round(latency_ms, 4)
        }
