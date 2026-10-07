"""Subscription semantics: snapshot/delta boundary, resume, expired cursors."""
from __future__ import annotations

import itertools
import threading

import httpx

from .conftest import SseReader, make_drill, submit, wait_until

CHANNELS = ["BL-01", "BL-02", "BL-03"]
STATES = ["normal", "warning", "alarm", "bypass"]


def _submit_batch(client: httpx.Client, drill_id: str, prefix: str, count: int,
                  out: list, lock: threading.Lock) -> None:
    counter = itertools.count()
    for _ in range(count):
        i = next(counter)
        body = {
            "event_id": f"{prefix}-{i}",
            "channel": CHANNELS[i % len(CHANNELS)],
            "state": STATES[i % len(STATES)],
        }
        resp = client.post(f"/api/drills/{drill_id}/events", json=body)
        assert resp.status_code == 201, resp.text
        with lock:
            out.append((body, resp.json()["seq"]))


def test_snapshot_delta_boundary_under_concurrent_writes(server):
    """While a subscription is being established, every event must end up in
    exactly one bucket: inside the pinned snapshot (seq <= watermark) or in
    the delta stream (seq > watermark) — never both, never neither."""
    with httpx.Client(base_url=server.url, timeout=10) as client:
        drill_id = make_drill(client, "边界演练")
        submitted: list[tuple[dict, int]] = []
        lock = threading.Lock()

        _submit_batch(client, drill_id, "base", 5, submitted, lock)

        reader = SseReader(f"{server.url}/api/drills/{drill_id}/stream").start()
        assert wait_until(lambda: len(reader.frames_of("snapshot")) == 1, timeout=5)
        snapshot = reader.frames_of("snapshot")[0]
        watermark = snapshot["watermark"]

        writers = [
            threading.Thread(
                target=_submit_batch,
                args=(client, drill_id, f"w{n}", 12, submitted, lock),
            )
            for n in range(4)
        ]
        for t in writers:
            t.start()
        for t in writers:
            t.join()

        with lock:
            expected_delta = {seq for _, seq in submitted if seq > watermark}
        assert wait_until(
            lambda: expected_delta <= {e["seq"] for e in reader.frames_of("event")},
            timeout=15,
        )
        reader.stop()

        delta_seqs = [e["seq"] for e in reader.frames_of("event")]
        # exactly-once: no duplicates, nothing at or below the watermark,
        # and the delivered set is exactly the set committed after the pin.
        assert len(delta_seqs) == len(set(delta_seqs))
        assert all(seq > watermark for seq in delta_seqs)
        assert set(delta_seqs) == expected_delta
        assert delta_seqs == sorted(delta_seqs)

        # The snapshot itself is internally consistent: each channel row is
        # the state of the event with the same seq (same-transaction proof),
        # and every event at or below the watermark is reflected in it.
        with lock:
            by_seq = {seq: body for body, seq in submitted}
        for channel in snapshot["channels"]:
            source = by_seq[channel["seq"]]
            assert source["channel"] == channel["channel"]
            assert source["state"] == channel["state"]
        for body, seq in submitted:
            if seq <= watermark:
                row = next(c for c in snapshot["channels"] if c["channel"] == body["channel"])
                assert row["seq"] >= seq

        # Replaying every accepted event in seq order reproduces the final
        # projection exactly.
        with lock:
            ordered = sorted(submitted, key=lambda item: item[1])
        final = {}
        for body, _ in ordered:
            final[body["channel"]] = body["state"]
        snap_after = client.get(f"/api/drills/{drill_id}/snapshot").json()
        assert {c["channel"]: c["state"] for c in snap_after["channels"]} == final


def test_resume_after_disconnect_and_boundary_dedup(server):
    """A reconnect with the last applied seq receives exactly the missed
    events; the boundary event itself is never re-delivered."""
    with httpx.Client(base_url=server.url, timeout=10) as client:
        drill_id = make_drill(client, "断线演练")
        for i in range(5):
            resp = submit(client, drill_id, f"r-{i}", "BL-01", "normal")
            assert resp.status_code == 201

        # First connection applied events up to seq 3, then dropped.
        first = SseReader(f"{server.url}/api/drills/{drill_id}/stream?after=3").start()
        assert wait_until(lambda: len(first.frames_of("event")) >= 2, timeout=5)
        first.stop()
        assert [e["seq"] for e in first.frames_of("event")] == [4, 5]
        assert first.frames_of("resume")[0]["after"] == 3

        # While "disconnected", more events commit.
        for i in range(5, 8):
            submit(client, drill_id, f"r-{i}", "BL-02", "alarm")

        # Reconnect from the last applied seq: only the missed ones arrive.
        second = SseReader(f"{server.url}/api/drills/{drill_id}/stream?after=5").start()
        assert wait_until(lambda: len(second.frames_of("event")) >= 3, timeout=5)
        second.stop()
        assert [e["seq"] for e in second.frames_of("event")] == [6, 7, 8]

        # Boundary retransmission: reconnecting with the newest applied seq
        # must not re-deliver anything; the next committed event arrives once.
        third = SseReader(f"{server.url}/api/drills/{drill_id}/stream?after=8").start()
        assert wait_until(lambda: len(third.frames_of("resume")) == 1, timeout=5)
        submit(client, drill_id, "r-8", "BL-01", "bypass")
        assert wait_until(lambda: len(third.frames_of("event")) >= 1, timeout=5)
        third.stop()
        seqs = [e["seq"] for e in third.frames_of("event")]
        assert seqs == [9]
        assert all(s > 8 for s in seqs)


def test_snapshot_stream_has_no_cursor_requirement(server):
    """A fresh subscription always works, even when the log was truncated."""
    with httpx.Client(base_url=server.url, timeout=10) as client:
        drill_id = make_drill(client)
        submit(client, drill_id, "s-0", "BL-01", "normal")
        reader = SseReader(f"{server.url}/api/drills/{drill_id}/stream").start()
        assert wait_until(lambda: len(reader.frames_of("snapshot")) == 1, timeout=5)
        snap = reader.frames_of("snapshot")[0]
        assert snap["watermark"] == 1
        assert [c["channel"] for c in snap["channels"]] == ["BL-01"]
        assert [e["event_id"] for e in snap["recent_events"]] == ["s-0"]
        reader.stop()


def test_expired_cursor_requires_snapshot_refetch(server_small_log):
    """When the log is truncated or the cursor is out of range, both the REST
    API (410) and the SSE stream (resync frame) demand a snapshot refetch."""
    server = server_small_log
    with httpx.Client(base_url=server.url, timeout=10) as client:
        drill_id = make_drill(client, "截断演练")
        for i in range(30):
            resp = submit(client, drill_id, f"t-{i}", "BL-01", STATES[i % len(STATES)])
            assert resp.status_code == 201

        detail = client.get(f"/api/drills/{drill_id}").json()
        assert detail["min_available_seq"] == 11  # retention keeps the last 20
        assert detail["watermark"] == 30

        # REST: cursor below the retained range -> 410 resync_required.
        expired = client.get(f"/api/drills/{drill_id}/events", params={"after": 5})
        assert expired.status_code == 410
        body = expired.json()["detail"]
        assert body["code"] == "resync_required"
        assert body["reason"] == "cursor_expired"
        assert body["min_available_seq"] == 11
        assert body["watermark"] == 30

        # REST: cursor ahead of the watermark -> 410 as well.
        ahead = client.get(f"/api/drills/{drill_id}/events", params={"after": 999})
        assert ahead.status_code == 410
        assert ahead.json()["detail"]["reason"] == "cursor_out_of_range"

        # Boundary: after = min_available - 1 is still servable.
        edge = client.get(f"/api/drills/{drill_id}/events", params={"after": 10})
        assert edge.status_code == 200
        assert edge.json()["events"][0]["seq"] == 11

        # SSE: expired cursor -> a single resync frame, then the stream ends.
        reader = SseReader(f"{server.url}/api/drills/{drill_id}/stream?after=5").start()
        assert reader.done.wait(timeout=5)
        reader.stop()
        assert len(reader.frames) == 1
        kind, payload = reader.frames[0]
        assert kind == "resync"
        assert payload["code"] == "resync_required"
        assert payload["reason"] == "cursor_expired"

        # SSE: a valid boundary cursor resumes normally.
        ok = SseReader(f"{server.url}/api/drills/{drill_id}/stream?after=10").start()
        assert wait_until(lambda: len(ok.frames_of("event")) >= 20, timeout=5)
        ok.stop()
        assert [e["seq"] for e in ok.frames_of("event")] == list(range(11, 31))
