"""Idempotent submission semantics keyed by the stable event_id."""
from __future__ import annotations

import httpx

from .conftest import make_drill, submit


def test_identical_replay_returns_original_seq(client: httpx.Client):
    drill_id = make_drill(client)
    first = submit(client, drill_id, "idem-1", "BL-01", "alarm")
    assert first.status_code == 201
    original = first.json()

    replay = submit(client, drill_id, "idem-1", "BL-01", "alarm")
    assert replay.status_code == 200
    body = replay.json()
    assert body["deduplicated"] is True
    assert body["seq"] == original["seq"]

    # The log and the watermark were not advanced by the replay.
    detail = client.get(f"/api/drills/{drill_id}").json()
    assert detail["watermark"] == original["seq"]
    events = client.get(f"/api/drills/{drill_id}/events").json()["events"]
    assert len(events) == 1


def test_replay_survives_later_events(client: httpx.Client):
    drill_id = make_drill(client)
    original = submit(client, drill_id, "idem-old", "BL-01", "normal").json()
    for i in range(5):
        submit(client, drill_id, f"later-{i}", "BL-02", "warning")

    replay = submit(client, drill_id, "idem-old", "BL-01", "normal")
    assert replay.status_code == 200
    assert replay.json()["seq"] == original["seq"]


def test_conflicting_event_id_is_rejected_and_does_not_rewrite_projection(client: httpx.Client):
    drill_id = make_drill(client)
    assert submit(client, drill_id, "conf-1", "BL-01", "alarm").status_code == 201

    # Same id, different state -> 409, projection untouched.
    conflict = submit(client, drill_id, "conf-1", "BL-01", "normal")
    assert conflict.status_code == 409
    detail = conflict.json()["detail"]
    assert detail["code"] == "event_id_conflict"
    assert detail["existing"]["state"] == "alarm"

    # Same id, different channel -> 409 as well.
    assert submit(client, drill_id, "conf-1", "BL-99", "alarm").status_code == 409

    detail = client.get(f"/api/drills/{drill_id}").json()
    assert detail["watermark"] == 1
    assert detail["channels"] == [
        {"channel": "BL-01", "state": "alarm", "seq": 1, "updated_at": detail["channels"][0]["updated_at"]}
    ]


def test_event_id_is_unique_across_drills(client: httpx.Client):
    drill_a = make_drill(client, "A")
    drill_b = make_drill(client, "B")
    assert submit(client, drill_a, "shared-id", "L1", "normal").status_code == 201
    conflict = submit(client, drill_b, "shared-id", "L1", "normal")
    assert conflict.status_code == 409
    # Drill B's projection was not modified.
    assert client.get(f"/api/drills/{drill_b}").json()["channels"] == []
