"""Basic API behaviour: drills, submission, projection/log consistency, ordering."""
from __future__ import annotations

import httpx

from .conftest import make_drill, submit


def test_health(client: httpx.Client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["watermark"] == 0
    assert body["retention"] > 0


def test_create_and_get_drill(client: httpx.Client):
    drill_id = make_drill(client, "联锁演练A")
    drills = client.get("/api/drills").json()
    assert [d["id"] for d in drills] == [drill_id]

    detail = client.get(f"/api/drills/{drill_id}").json()
    assert detail["name"] == "联锁演练A"
    assert detail["watermark"] == 0
    assert detail["channels"] == []
    assert detail["min_available_seq"] is None


def test_unknown_drill_is_404(client: httpx.Client):
    assert client.get("/api/drills/nope").status_code == 404
    assert client.get("/api/drills/nope/snapshot").status_code == 404
    assert client.get("/api/drills/nope/events").status_code == 404
    assert client.get("/api/drills/nope/stream").status_code == 404
    assert submit(client, "nope", "e1", "L1", "normal").status_code == 404


def test_submit_updates_projection_and_log_in_same_transaction(client: httpx.Client):
    drill_id = make_drill(client)
    resp = submit(client, drill_id, "evt-1", "BL-01", "alarm")
    assert resp.status_code == 201
    body = resp.json()
    assert body["seq"] == 1
    assert body["deduplicated"] is False

    detail = client.get(f"/api/drills/{drill_id}").json()
    assert detail["watermark"] == 1
    channel = detail["channels"][0]
    # The projection row points at the very event that produced it: both were
    # written by the same transaction.
    assert channel["channel"] == "BL-01"
    assert channel["state"] == "alarm"
    assert channel["seq"] == body["seq"]

    events = client.get(f"/api/drills/{drill_id}/events").json()
    assert [e["seq"] for e in events["events"]] == [1]
    assert events["events"][0]["event_id"] == "evt-1"


def test_submit_validation(client: httpx.Client):
    drill_id = make_drill(client)
    assert submit(client, drill_id, "", "L1", "normal").status_code == 422
    assert submit(client, drill_id, "e1", "  ", "normal").status_code == 422
    assert submit(client, drill_id, "e1", "L1", "").status_code == 422
    assert client.post("/api/drills", json={"name": "  "}).status_code == 422


def test_global_sequence_is_strictly_increasing_across_drills(client: httpx.Client):
    drill_a = make_drill(client, "A")
    drill_b = make_drill(client, "B")
    seqs = []
    for i in range(10):
        drill_id = drill_a if i % 2 == 0 else drill_b
        resp = submit(client, drill_id, f"mix-{i}", "L1", "normal")
        assert resp.status_code == 201
        seqs.append(resp.json()["seq"])
    assert seqs == sorted(seqs)
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))


def test_events_endpoint_pagination_and_after(client: httpx.Client):
    drill_id = make_drill(client)
    for i in range(5):
        submit(client, drill_id, f"p-{i}", "L1", "normal")

    page = client.get(f"/api/drills/{drill_id}/events", params={"after": 2, "limit": 2}).json()
    assert [e["seq"] for e in page["events"]] == [3, 4]
    assert page["watermark"] == 5

    rest = client.get(f"/api/drills/{drill_id}/events", params={"after": 4}).json()
    assert [e["seq"] for e in rest["events"]] == [5]

    none_left = client.get(f"/api/drills/{drill_id}/events", params={"after": 5}).json()
    assert none_left["events"] == []
