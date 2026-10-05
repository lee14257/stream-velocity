from typing import Dict, Any

class BucketedRingBuffer:
    """
    Fixed-memory ring buffer using time-bucketed aggregation for constant-time O(1)
    velocity calculations over a sliding time window.

    Writes map timestamps to a slot index via integer division (O(1)). Reads return
    pre-computed running totals after lazily expiring stale buckets, making both
    operations constant-time regardless of the number of buckets in the window.
    """
    __slots__ = (
        'window_seconds', 'bucket_seconds', 'num_buckets',
        'counts', 'sums', 'last_bucket_idx',
        'running_count', 'running_sum', 'trailing_bucket',
    )

    def __init__(self, window_seconds: int = 300, bucket_seconds: int = 5):
        if window_seconds % bucket_seconds != 0:
            raise ValueError("window_seconds must be evenly divisible by bucket_seconds")

        self.window_seconds = window_seconds
        self.bucket_seconds = bucket_seconds
        self.num_buckets = window_seconds // bucket_seconds

        self.counts = [0] * self.num_buckets
        self.sums = [0.0] * self.num_buckets
        self.last_bucket_idx = [-1] * self.num_buckets

        self.running_count = 0
        self.running_sum = 0.0
        self.trailing_bucket = -1

    def _get_current_slot(self, timestamp: float) -> tuple[int, int]:
        total_buckets = int(timestamp // self.bucket_seconds)
        slot_idx = total_buckets % self.num_buckets
        return slot_idx, total_buckets

    def _evict_stale(self, current_total_buckets: int) -> None:
        min_valid_bucket = current_total_buckets - self.num_buckets + 1

        # First observation: nothing can be stale yet, so fast-forward the
        # trailing edge instead of scanning an empty buffer bucket-by-bucket.
        if self.trailing_bucket == -1:
            self.trailing_bucket = min_valid_bucket - 1
            return

        # Out-of-order guard: never regress the trailing edge backwards.
        if min_valid_bucket <= self.trailing_bucket:
            return

        while self.trailing_bucket < min_valid_bucket:
            self.trailing_bucket += 1
            slot_idx = self.trailing_bucket % self.num_buckets
            if self.last_bucket_idx[slot_idx] == self.trailing_bucket:
                self.running_count -= self.counts[slot_idx]
                self.running_sum -= self.sums[slot_idx]
                self.counts[slot_idx] = 0
                self.sums[slot_idx] = 0.0
                self.last_bucket_idx[slot_idx] = -1

    def record(self, timestamp: float, amount: float = 0.0) -> None:
        slot_idx, total_buckets = self._get_current_slot(timestamp)
        self._evict_stale(total_buckets)

        # Evict stale data if slot belongs to a previous window iteration.
        if self.last_bucket_idx[slot_idx] != total_buckets:
            if self.last_bucket_idx[slot_idx] != -1:
                self.running_count -= self.counts[slot_idx]
                self.running_sum -= self.sums[slot_idx]
            self.counts[slot_idx] = 0
            self.sums[slot_idx] = 0.0
            self.last_bucket_idx[slot_idx] = total_buckets

        self.counts[slot_idx] += 1
        self.sums[slot_idx] += amount
        self.running_count += 1
        self.running_sum += amount

    def get_metrics(self, current_timestamp: float) -> Dict[str, Any]:
        current_total_buckets = int(current_timestamp // self.bucket_seconds)
        self._evict_stale(current_total_buckets)

        return {
            "window_seconds": self.window_seconds,
            "count": self.running_count,
            "sum_amount": round(self.running_sum, 2)
        }
