"""SQLite 存储层：事件日志（严格递增）+ 通道状态投影 + 幂等键。

所有写操作都在单个 ``BEGIN IMMEDIATE`` 事务中完成事件插入、投影更新与
幂等记录落盘，保证“同一事务同时更新状态投影和严格递增的事件日志”。

并发约定（与 SSE 订阅建立配合）：
- 所有数据库访问经 :attr:`_lock`（可重入）串行化；
- 订阅方在持有 :attr:`cond` 期间完成“固定水位 → 读快照 → 排空增量”；
- 事件写入先提交（持 _lock）再在 cond 上通知，因此订阅方要么在快照水位
  内看到事件，要么把事件作为增量收到，二者恰好其一；
- 日志截断整个事务在 cond 内执行：订阅方要么在截断前完成校验+排空，
  要么看到抬升后的截断下限而被要求重新获取快照，不存在空洞。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from typing import Optional


class ConflictError(Exception):
    """事件标识被不同内容（通道或状态）复用。HTTP 映射为 409。"""


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    # isolation_level=None：事务边界由我们显式控制。
    conn.isolation_level = None
    return conn


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        self._lock = threading.RLock()
        self._cond = threading.Condition()
        self._conn = _connect(path)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS drills (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    trim_floor INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    drill_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    state TEXT NOT NULL,
                    occurred_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channels (
                    drill_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    state TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    PRIMARY KEY (drill_id, channel)
                );
                -- 稳定事件标识：同一 (drill, event_id) 内容必须一致；
                -- 一致重传返回原序号，不一致复用直接拒绝。
                CREATE TABLE IF NOT EXISTS idempotency (
                    drill_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    state TEXT NOT NULL,
                    PRIMARY KEY (drill_id, event_id)
                );
                """
            )

    # ---- 演练 ----------------------------------------------------------------

    def create_drill(self, drill_id: str, name: str, now: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO drills(id, name, created_at, trim_floor) "
                "VALUES (?,?,?,0)",
                (drill_id, name, now),
            )

    def list_drills(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, name, created_at FROM drills ORDER BY id"
            ).fetchall()
        return [dict(r) for r in rows]

    def drill_exists(self, drill_id: str) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM drills WHERE id=?", (drill_id,)
            ).fetchone() is not None

    # ---- 事件写入（事务内同时更新投影与幂等表） ---------------------------------

    def submit_event(
        self,
        drill_id: str,
        event_id: str,
        channel: str,
        state: str,
        now: float,
    ) -> int:
        """提交通道状态变更，返回事件的全局序号。

        相同 ``(drill_id, event_id)`` + 相同内容：返回原序号（幂等重传）。
        相同标识但通道/状态不同：抛 :class:`ConflictError`，投影不被改写。
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                existing = cur.execute(
                    "SELECT seq, channel, state FROM idempotency "
                    "WHERE drill_id=? AND event_id=?",
                    (drill_id, event_id),
                ).fetchone()
                if existing is not None:
                    if existing["channel"] == channel and existing["state"] == state:
                        # 一致重传：原样返回已有序号，不插入、不改投影。
                        cur.execute("COMMIT")
                        seq = int(existing["seq"])
                    else:
                        raise ConflictError(
                            f"event_id {event_id!r} 已用于通道 "
                            f"{existing['channel']}={existing['state']}，"
                            f"拒绝以 {channel}={state} 复用"
                        )
                else:
                    row = cur.execute(
                        "INSERT INTO events(drill_id, event_id, channel, state, occurred_at) "
                        "VALUES (?,?,?,?,?)",
                        (drill_id, event_id, channel, state, now),
                    )
                    seq = int(row.lastrowid)
                    cur.execute(
                        "INSERT INTO channels(drill_id, channel, state, last_seq) "
                        "VALUES (?,?,?,?) "
                        "ON CONFLICT(drill_id, channel) DO UPDATE SET "
                        "state=excluded.state, last_seq=excluded.last_seq",
                        (drill_id, channel, state, seq),
                    )
                    cur.execute(
                        "INSERT INTO idempotency(drill_id, event_id, seq, channel, state) "
                        "VALUES (?,?,?,?,?)",
                        (drill_id, event_id, seq, channel, state),
                    )
                    cur.execute("COMMIT")
            except ConflictError:
                cur.execute("ROLLBACK")
                raise
            except Exception:
                cur.execute("ROLLBACK")
                raise
        # 提交完成后再通知等待中的订阅（先发生关系：提交 happens-before 通知）。
        with self._cond:
            self._cond.notify_all()
        return seq

    # ---- 读取 ----------------------------------------------------------------

    def watermark(self, drill_id: str) -> int:
        """当前已提交的最高全局序号（水位）。无事件时为 0。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM events WHERE drill_id=?",
                (drill_id,),
            ).fetchone()
        return int(row[0])

    def recoverable_floor(self, drill_id: str) -> int:
        """该演练仍可断线补齐的游标下限（含）。

        游标 ``after`` 必须 >= 该值；更小意味着中间事件已被截断删除。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT trim_floor FROM drills WHERE id=?", (drill_id,)
            ).fetchone()
        return int(row["trim_floor"]) if row else 0

    def snapshot(self, drill_id: str) -> dict:
        """固定水位并返回该水位内完整一致的快照（显式只读事务内读取）。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                wm = self.watermark_locked(drill_id)
                rows = cur.execute(
                    "SELECT channel, state, last_seq FROM channels "
                    "WHERE drill_id=? ORDER BY channel",
                    (drill_id,),
                ).fetchall()
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        return {
            "watermark": wm,
            "channels": [
                {"channel": r["channel"], "state": r["state"], "last_seq": r["last_seq"]}
                for r in rows
            ],
        }

    def watermark_locked(self, drill_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE drill_id=?",
            (drill_id,),
        ).fetchone()
        return int(row[0])

    def events_after(self, drill_id: str, after: int, limit: Optional[int] = None) -> list[dict]:
        sql = (
            "SELECT seq, event_id, channel, state, occurred_at FROM events "
            "WHERE drill_id=? AND seq>? ORDER BY seq"
        )
        params: tuple = (drill_id, after)
        if limit is not None:
            sql += " LIMIT ?"
            params = (drill_id, after, limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def _events_after_locked(self, drill_id: str, after: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT seq, event_id, channel, state, occurred_at FROM events "
            "WHERE drill_id=? AND seq>? ORDER BY seq",
            (drill_id, after),
        ).fetchall()
        return [dict(r) for r in rows]

    def open_fresh_subscription(self, drill_id: str) -> tuple[dict, list[dict]]:
        """建连固定水位：在订阅条件变量下原子取“水位内快照 + 已提交增量”。

        与写入/截断互斥，保证事件要么快照水位内可见、要么出现在增量中。
        """
        with self._cond:
            with self._lock:
                cur = self._conn.cursor()
                cur.execute("BEGIN")
                try:
                    wm = self.watermark_locked(drill_id)
                    rows = cur.execute(
                        "SELECT channel, state, last_seq FROM channels "
                        "WHERE drill_id=? ORDER BY channel",
                        (drill_id,),
                    ).fetchall()
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
                pending = self._events_after_locked(drill_id, wm)
        snap = {
            "watermark": wm,
            "channels": [
                {"channel": r["channel"], "state": r["state"], "last_seq": r["last_seq"]}
                for r in rows
            ],
        }
        return snap, pending

    def open_resume_subscription(
        self, drill_id: str, after: int
    ) -> tuple[str, dict]:
        """校验断线游标并原子排空增量（与日志截断互斥）。

        返回 ("ok", {"watermark", "events"})、
        ("stale", {...}) 或 ("ahead", {...})。
        """
        with self._cond:
            with self._lock:
                wm = self.watermark_locked(drill_id)
                row = self._conn.execute(
                    "SELECT trim_floor FROM drills WHERE id=?", (drill_id,)
                ).fetchone()
                floor = int(row["trim_floor"]) if row else 0
                if after < floor:
                    return "stale", {
                        "cursor": after, "recoverable_after": floor, "watermark": wm,
                    }
                if after > wm:
                    return "ahead", {"cursor": after, "watermark": wm}
                events = self._events_after_locked(drill_id, after)
        return "ok", {"watermark": wm, "events": events}

    def wait_events(self, drill_id: str, after: int, timeout: float) -> list[dict]:
        """等待并返回序号严格大于 ``after`` 的事件；超时返回空表。"""
        with self._cond:
            events = self._events_after_locked(drill_id, after)
            if not events:
                self._cond.wait(timeout=timeout)
                events = self._events_after_locked(drill_id, after)
        return events

    def events_recent(self, drill_id: str, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, event_id, channel, state, occurred_at FROM events "
                "WHERE drill_id=? ORDER BY seq DESC LIMIT ?",
                (drill_id, limit),
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # ---- 日志截断（可恢复范围管理）---------------------------------------------

    def trim_events(self, drill_id: str, before_seq: int) -> int:
        """删除 seq < before_seq 的历史事件并抬升可恢复下限。

        整个操作在订阅条件变量内完成：建连中的订阅要么先于截断排空这些事件，
        要么读到抬升后的下限而收到 snapshot_required；投影与幂等键保留。
        """
        with self._cond:
            with self._lock:
                cur = self._conn.cursor()
                cur.execute("BEGIN IMMEDIATE")
                try:
                    cur.execute(
                        "DELETE FROM events WHERE drill_id=? AND seq<?",
                        (drill_id, before_seq),
                    )
                    removed = cur.rowcount
                    # 可恢复游标下限抬至 before_seq-1：旧游标必然存在空洞。
                    cur.execute(
                        "UPDATE drills SET trim_floor=? "
                        "WHERE id=? AND trim_floor<?",
                        (before_seq - 1, drill_id, before_seq - 1),
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            self._cond.notify_all()
        return removed

    # ---- 订阅通知 -------------------------------------------------------------

    @property
    def cond(self) -> threading.Condition:
        return self._cond

    def close(self) -> None:
        with self._lock:
            self._conn.close()
        if self._path != ":memory:":
            for suffix in ("-wal", "-shm"):
                p = self._path + suffix
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except OSError:
                        pass
