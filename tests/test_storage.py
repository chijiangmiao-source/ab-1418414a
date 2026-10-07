"""存储层单元测试：事务原子性、幂等标识、截断边界。"""
import threading
import time
import unittest

from server.storage import ConflictError, Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.store.create_drill("d1", "演练1", time.time())

    def tearDown(self):
        self.store.close()

    def test_seq_strictly_increasing_and_projection(self):
        s1 = self.store.submit_event("d1", "e1", "A", "normal", 1.0)
        s2 = self.store.submit_event("d1", "e2", "A", "trip", 2.0)
        s3 = self.store.submit_event("d1", "e3", "B", "normal", 3.0)
        self.assertEqual([s1, s2, s3], [1, 2, 3])
        snap = self.store.snapshot("d1")
        self.assertEqual(snap["watermark"], 3)
        by_ch = {c["channel"]: c for c in snap["channels"]}
        self.assertEqual(by_ch["A"]["state"], "trip")
        self.assertEqual(by_ch["A"]["last_seq"], 2)
        self.assertEqual(by_ch["B"]["state"], "normal")

    def test_idempotent_retry_returns_same_seq(self):
        s1 = self.store.submit_event("d1", "dup", "A", "normal", 1.0)
        s2 = self.store.submit_event("d1", "dup", "A", "normal", 2.0)
        self.assertEqual(s1, s2)
        self.assertEqual(self.store.watermark("d1"), 1)
        self.assertEqual(len(self.store.events_after("d1", 0)), 1)

    def test_event_id_reuse_with_different_content_rejected(self):
        self.store.submit_event("d1", "k", "A", "normal", 1.0)
        with self.assertRaises(ConflictError):
            self.store.submit_event("d1", "k", "B", "normal", 2.0)
        with self.assertRaises(ConflictError):
            self.store.submit_event("d1", "k", "A", "trip", 3.0)
        # 投影未被改写
        snap = self.store.snapshot("d1")
        self.assertEqual(snap["channels"], [
            {"channel": "A", "state": "normal", "last_seq": 1}
        ])
        self.assertEqual(self.store.watermark("d1"), 1)

    def test_snapshot_then_incremental_partition(self):
        # 固定水位后，事件恰好分为“快照内”和“增量”两部分
        self.store.submit_event("d1", "e1", "A", "normal", 1.0)
        self.store.submit_event("d1", "e2", "B", "normal", 2.0)
        snap = self.store.snapshot("d1")
        self.assertEqual(snap["watermark"], 2)
        self.store.submit_event("d1", "e3", "A", "trip", 3.0)
        inc = self.store.events_after("d1", snap["watermark"])
        self.assertEqual([e["seq"] for e in inc], [3])
        covered = {c["last_seq"] for c in snap["channels"]} | {e["seq"] for e in inc}
        self.assertEqual(covered, {1, 2, 3})

    def test_trim_invalidates_old_cursor(self):
        for i in range(1, 6):
            self.store.submit_event("d1", f"e{i}", "A", "normal", float(i))
        self.assertEqual(self.store.recoverable_floor("d1"), 0)
        self.store.trim_events("d1", before_seq=4)
        # 1-3 被删，游标 3 仍可补齐（seq 4,5 都在），游标 2 存在空洞
        self.assertEqual(self.store.recoverable_floor("d1"), 3)
        self.assertEqual(
            [e["seq"] for e in self.store.events_after("d1", 3)], [4, 5]
        )

    def test_concurrent_writers_keep_projection_consistent(self):
        errors = []

        def worker(prefix):
            try:
                for i in range(50):
                    self.store.submit_event(
                        "d1", f"{prefix}-{i}", "A",
                        "trip" if i % 2 else "normal", float(i),
                    )
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(p,)) for p in ("x", "y")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.store.watermark("d1"), 100)
        # 投影的 last_seq 必然等于水位（最后一次写入都作用于通道 A）
        snap = self.store.snapshot("d1")
        self.assertEqual(snap["channels"][0]["last_seq"], 100)
        # 日志中序号严格连续递增
        seqs = [e["seq"] for e in self.store.events_after("d1", 0)]
        self.assertEqual(seqs, list(range(1, 101)))


if __name__ == "__main__":
    unittest.main()
