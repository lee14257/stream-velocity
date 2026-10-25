import unittest
import time
from stream_velocity import VelocityTracker

class TestVelocityTracker(unittest.TestCase):

    def test_basic_aggregation(self):
        tracker = VelocityTracker(window_seconds=60, bucket_seconds=1)
        now = 1000.0

        tracker.record_event(entity_id="acc_1", amount=50.0, timestamp=now)
        tracker.record_event(entity_id="acc_1", amount=25.0, timestamp=now + 5)

        metrics = tracker.get_velocity(entity_id="acc_1", timestamp=now + 10)

        self.assertEqual(metrics.count, 2)
        self.assertEqual(metrics.sum_amount, 75.0)

    def test_window_expiration(self):
        tracker = VelocityTracker(window_seconds=10, bucket_seconds=1)
        now = 1000.0

        tracker.record_event(entity_id="acc_2", amount=100.0, timestamp=now)

        # Check inside window
        metrics_before = tracker.get_velocity(entity_id="acc_2", timestamp=now + 5)
        self.assertEqual(metrics_before.count, 1)

        # Check outside window (15 seconds later)
        metrics_after = tracker.get_velocity(entity_id="acc_2", timestamp=now + 15)
        self.assertEqual(metrics_after.count, 0)

    def test_out_of_order_events(self):
        tracker = VelocityTracker(window_seconds=60, bucket_seconds=1)
        now = 1000.0

        tracker.record_event(entity_id="acc_3", amount=10.0, timestamp=now)
        tracker.record_event(entity_id="acc_3", amount=10.0, timestamp=now + 10)

        # Event arriving late with an older timestamp must not regress the window
        tracker.record_event(entity_id="acc_3", amount=20.0, timestamp=now - 5)

        metrics = tracker.get_velocity(entity_id="acc_3", timestamp=now + 20)
        self.assertEqual(metrics.count, 3)
        self.assertEqual(metrics.sum_amount, 40.0)

    def test_many_bucket_window(self):
        tracker = VelocityTracker(window_seconds=100000, bucket_seconds=1)
        now = 1_000_000.0

        tracker.record_event(entity_id="acc_4", amount=1.0, timestamp=now)
        tracker.record_event(entity_id="acc_4", amount=1.0, timestamp=now + 50_000)

        metrics = tracker.get_velocity(entity_id="acc_4", timestamp=now + 60_000)
        self.assertEqual(metrics.count, 2)
        self.assertEqual(metrics.sum_amount, 2.0)

        # Event far outside the 100000s window must be expired
        expired = tracker.get_velocity(entity_id="acc_4", timestamp=now + 200_000)
        self.assertEqual(expired.count, 0)

    def test_idle_entity_eviction(self):
        tracker = VelocityTracker(
            window_seconds=60,
            bucket_seconds=1,
            idle_timeout=10.0,
            cleanup_interval=0.0,
        )

        tracker.record_event(entity_id="acc_a", amount=10.0, timestamp=1000.0)
        self.assertIn("acc_a", tracker.entities)

        # Advance time well beyond the idle timeout by recording another entity.
        tracker.record_event(entity_id="acc_b", amount=5.0, timestamp=2000.0)

        # acc_a has been idle for 1000s > idle_timeout, so it is evicted.
        self.assertNotIn("acc_a", tracker.entities)
        self.assertIn("acc_b", tracker.entities)

        # Querying an evicted entity reports an empty window, not an error.
        result = tracker.get_velocity(entity_id="acc_a", timestamp=2000.0)
        self.assertEqual(result.count, 0)
        self.assertEqual(result.sum_amount, 0.0)

    def test_idle_timeout_disabled_by_default(self):
        tracker = VelocityTracker(window_seconds=60, bucket_seconds=1)

        tracker.record_event(entity_id="acc_c", amount=10.0, timestamp=1000.0)
        tracker.record_event(entity_id="acc_d", amount=5.0, timestamp=1_000_000.0)

        # With no idle_timeout, entities are never evicted.
        self.assertIn("acc_c", tracker.entities)
        self.assertIn("acc_d", tracker.entities)

if __name__ == "__main__":
    unittest.main()
