import time
from typing import Dict, Any

class BucketedRingBuffer:
    """
    Fixed-memory ring buffer using time-bucketed aggregation for constant-time O(1)
    velocity calculations over a sliding time window.
    """
    __slots__ = ('window_seconds', 'bucket_seconds', 'num_buckets', 'counts', 'sums', 'last_bucket_idx')

    def __init__(self, window_seconds: int = 300, bucket_seconds: int = 5):
        if window_seconds % bucket_seconds != 0:
            raise ValueError("window_seconds must be evenly divisible by bucket_seconds")
        
        self.window_seconds = window_seconds
        self.bucket_seconds = bucket_seconds
        self.num_buckets = window_seconds // bucket_seconds
        
        self.counts = [0] * self.num_buckets
        self.sums = [0.0] * self.num_buckets
        self.last_bucket_idx = [0] * self.num_buckets

    def _get_current_slot(self, timestamp: float) -> tuple[int, int]:
        total_buckets = int(timestamp // self.bucket_seconds)
        slot_idx = total_buckets % self.num_buckets
        return slot_idx, total_buckets

    def record(self, timestamp: float, amount: float = 0.0) -> None:
        slot_idx, total_buckets = self._get_current_slot(timestamp)
        
        # Evict stale data if slot belongs to a previous window iteration
        if self.last_bucket_idx[slot_idx] != total_buckets:
            self.counts[slot_idx] = 0
            self.sums[slot_idx] = 0.0
            self.last_bucket_idx[slot_idx] = total_buckets

        self.counts[slot_idx] += 1
        self.sums[slot_idx] += amount

    def get_metrics(self, current_timestamp: float) -> Dict[str, Any]:
        current_total_buckets = int(current_timestamp // self.bucket_seconds)
        min_valid_bucket = current_total_buckets - self.num_buckets + 1

        total_count = 0
        total_sum = 0.0

        for i in range(self.num_buckets):
            if self.last_bucket_idx[i] >= min_valid_bucket:
                total_count += self.counts[i]
                total_sum += self.sums[i]

        return {
            "window_seconds": self.window_seconds,
            "count": total_count,
            "sum_amount": round(total_sum, 2)
        }
