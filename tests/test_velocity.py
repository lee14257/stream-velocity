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

        self.assertEqual(metrics["count"], 2)
        self.assertEqual(metrics["sum_amount"], 75.0)
        self.assertLess(metrics["execution_latency_ms"], 5.0)

    def test_window_expiration(self):
        tracker = VelocityTracker(window_seconds=10, bucket_seconds=1)
        now = 1000.0

        tracker.record_event(entity_id="acc_2", amount=100.0, timestamp=now)
        
        # Check inside window
        metrics_before = tracker.get_velocity(entity_id="acc_2", timestamp=now + 5)
        self.assertEqual(metrics_before["count"], 1)

        # Check outside window (15 seconds later)
        metrics_after = tracker.get_velocity(entity_id="acc_2", timestamp=now + 15)
        self.assertEqual(metrics_after["count"], 0)

    def test_out_of_order_events(self):
        tracker = VelocityTracker(window_seconds=60, bucket_seconds=1)
        now = 1000.0

        tracker.record_event(entity_id="acc_3", amount=10.0, timestamp=now)
        tracker.record_event(entity_id="acc_3", amount=10.0, timestamp=now + 10)

        # Event arriving late with an older timestamp must not regress the window
        tracker.record_event(entity_id="acc_3", amount=20.0, timestamp=now - 5)

        metrics = tracker.get_velocity(entity_id="acc_3", timestamp=now + 20)
        self.assertEqual(metrics["count"], 3)
        self.assertEqual(metrics["sum_amount"], 40.0)

    def test_many_bucket_window(self):
        tracker = VelocityTracker(window_seconds=100000, bucket_seconds=1)
        now = 1_000_000.0

        tracker.record_event(entity_id="acc_4", amount=1.0, timestamp=now)
        tracker.record_event(entity_id="acc_4", amount=1.0, timestamp=now + 50_000)

        metrics = tracker.get_velocity(entity_id="acc_4", timestamp=now + 60_000)
        self.assertEqual(metrics["count"], 2)
        self.assertEqual(metrics["sum_amount"], 2.0)

        # Event far outside the 100000s window must be expired
        expired = tracker.get_velocity(entity_id="acc_4", timestamp=now + 200_000)
        self.assertEqual(expired["count"], 0)

if __name__ == "__main__":
    unittest.main()
