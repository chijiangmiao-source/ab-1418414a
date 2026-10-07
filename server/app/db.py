"""SQLite persistence for the interlock monitor.

Guarantees implemented here:

* one global, strictly increasing event sequence (``events.seq`` AUTOINCREMENT)
* the state projection (``channel_state``) and the event log (``events``)
  are written in the SAME transaction, so a committed event is always
  reflected in the projection and vice versa
* idempotent submission keyed by the caller-supplied stable ``event_id``:
  an identical replay returns the original row, a conflicting reuse is
  rejected and never touches the projection
* bounded per-drill log retention with explicit cursor validation, so a
  subscriber can tell exactly whether its cursor is still recoverable or a
  fresh snapshot is required
"""
from __future__ import annotations

import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS drills (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL UNIQUE,
    drill_id   TEXT NOT NULL REFERENCES drills(id),
    channel    TEXT NOT NULL,
    state      TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_drill_seq ON events(drill_id, seq);

CREATE TABLE IF NOT EXISTS channel_state (
    drill_id   TEXT NOT NULL REFERENCES drills(id),
    channel    TEXT NOT NULL,
    state      TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (drill_id, channel)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class NotFoundError(Exception):
    """The requested drill does not exist."""


class ConflictError(Exception):
    """The event_id was already used with a different payload."""

    def __init__(self, existing: dict[str, Any]):
        super().__init__("event_id reused with a different payload")
        self.existing = existing


class CursorExpiredError(Exception):
    """The cursor cannot be served from the retained log; the client must
    re-fetch a snapshot instead of receiving deltas."""

    def __init__(self, reason: str, min_available_seq: int | None, watermark: int):
        super().__init__(reason)
        self.reason = reason
        self.min_available_seq = min_available_seq
        self.watermark = watermark


class Database:
    """Thread-safe SQLite store. A single connection guarded by an RLock:
    writers are fully serialised and multi-statement reads (snapshot) are
    taken under the same lock, so a snapshot can never observe a state
    between two commits."""

    def __init__(self, path: str, retention: int = 10_000):
        self.path = path
        self.retention = max(1, retention)
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)

    @classmethod
    def from_env(cls) -> "Database":
        return cls(
            path=os.environ.get("DATABASE_PATH", "./interlock.db"),
            retention=int(os.environ.get("EVENT_LOG_MAX_PER_DRILL", "10000")),
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- internal helpers (caller must hold the lock) ----------------------
    def _watermark_locked(self) -> int:
        row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS w FROM events").fetchone()
        return int(row["w"])

    def _drill_locked(self, drill_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM drills WHERE id = ?", (drill_id,)).fetchone()

    def _bounds_locked(self, drill_id: str) -> tuple[int | None, int | None]:
        row = self._conn.execute(
            "SELECT MIN(seq) AS lo, MAX(seq) AS hi FROM events WHERE drill_id = ?",
            (drill_id,),
        ).fetchone()
        return row["lo"], row["hi"]

    # -- drills -------------------------------------------------------------
    def create_drill(self, name: str) -> dict[str, Any]:
        drill = {"id": uuid.uuid4().hex, "name": name, "created_at": _now()}
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO drills(id, name, created_at) VALUES (:id, :name, :created_at)",
                    drill,
                )
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
        return drill

    def get_drill(self, drill_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._drill_locked(drill_id)
            return dict(row) if row is not None else None

    def list_drills(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM drills ORDER BY created_at, id"
            ).fetchall()
            return [dict(r) for r in rows]

    def drill_detail(self, drill_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._drill_locked(drill_id)
            if row is None:
                raise NotFoundError(drill_id)
            lo, hi = self._bounds_locked(drill_id)
            channels = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT channel, state, seq, updated_at FROM channel_state "
                    "WHERE drill_id = ? ORDER BY channel",
                    (drill_id,),
                ).fetchall()
            ]
            return {
                **dict(row),
                "watermark": self._watermark_locked(),
                "min_available_seq": lo,
                "max_event_seq": hi,
                "channels": channels,
            }

    # -- events -------------------------------------------------------------
    def submit_event(
        self, drill_id: str, event_id: str, channel: str, state: str
    ) -> tuple[dict[str, Any], bool]:
        """Append one event and update the projection in the same transaction.

        Returns ``(event_row, deduplicated)``. An identical replay of
        ``event_id`` returns the original row with ``deduplicated=True``;
        reusing ``event_id`` with a different drill/channel/state raises
        :class:`ConflictError` and leaves both log and projection untouched.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._drill_locked(drill_id) is None:
                    raise NotFoundError(drill_id)
                row = self._conn.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if row is not None:
                    if (
                        row["drill_id"] == drill_id
                        and row["channel"] == channel
                        and row["state"] == state
                    ):
                        self._conn.execute("COMMIT")
                        return dict(row), True
                    raise ConflictError(dict(row))
                now = _now()
                cur = self._conn.execute(
                    "INSERT INTO events(event_id, drill_id, channel, state, created_at) "
                    "VALUES (?,?,?,?,?)",
                    (event_id, drill_id, channel, state, now),
                )
                seq = int(cur.lastrowid)
                self._conn.execute(
                    """INSERT INTO channel_state(drill_id, channel, state, seq, updated_at)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(drill_id, channel) DO UPDATE SET
                           state = excluded.state,
                           seq = excluded.seq,
                           updated_at = excluded.updated_at""",
                    (drill_id, channel, state, seq, now),
                )
                # Bounded retention: prune this drill's oldest events in the
                # same transaction so the recoverable range is always exact.
                self._conn.execute(
                    """DELETE FROM events
                       WHERE drill_id = ?
                         AND seq <= (SELECT MAX(seq) FROM events WHERE drill_id = ?) - ?""",
                    (drill_id, drill_id, self.retention),
                )
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            row = self._conn.execute("SELECT * FROM events WHERE seq = ?", (seq,)).fetchone()
            return dict(row), False

    def watermark(self) -> int:
        with self._lock:
            return self._watermark_locked()

    def snapshot(self, drill_id: str, recent_limit: int = 50) -> dict[str, Any]:
        """Consistent point-in-time view: the watermark, the channel
        projection and the recent log are all read under the write lock, so
        nothing can commit between the watermark and the projection reads."""
        with self._lock:
            if self._drill_locked(drill_id) is None:
                raise NotFoundError(drill_id)
            watermark = self._watermark_locked()
            lo, _ = self._bounds_locked(drill_id)
            channels = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT channel, state, seq, updated_at FROM channel_state "
                    "WHERE drill_id = ? ORDER BY channel",
                    (drill_id,),
                ).fetchall()
            ]
            recent = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT * FROM events WHERE drill_id = ? AND seq <= ? "
                    "ORDER BY seq DESC LIMIT ?",
                    (drill_id, watermark, recent_limit),
                ).fetchall()
            ]
            return {
                "watermark": watermark,
                "min_available_seq": lo,
                "channels": channels,
                "recent_events": recent,
            }

    def check_cursor(self, drill_id: str, after: int) -> None:
        """Validate a resume cursor against the retained log.

        Raises :class:`CursorExpiredError` when the cursor is ahead of the
        committed watermark or when events the client has not yet applied
        have already been pruned — in both cases the client must re-fetch a
        snapshot.
        """
        with self._lock:
            if self._drill_locked(drill_id) is None:
                raise NotFoundError(drill_id)
            watermark = self._watermark_locked()
            if after < 0 or after > watermark:
                raise CursorExpiredError("cursor_out_of_range", None, watermark)
            lo, _ = self._bounds_locked(drill_id)
            if lo is not None and after < lo - 1:
                raise CursorExpiredError("cursor_expired", lo, watermark)

    def list_events_after(self, drill_id: str, after: int, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE drill_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (drill_id, after, limit),
            ).fetchall()
            return [dict(r) for r in rows]
