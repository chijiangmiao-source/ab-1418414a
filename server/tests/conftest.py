from __future__ import annotations

import json
import socket
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import httpx
import pytest
import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from server.app.db import Database  # noqa: E402
from server.app.main import create_app  # noqa: E402


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _make_server(tmp_path: Path, retention: int) -> Any:
    db = Database(str(tmp_path / "test.db"), retention=retention)
    app = create_app(db=db, web_dist=None)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            httpx.get(url + "/api/health", timeout=1).raise_for_status()
            break
        except Exception:
            time.sleep(0.05)
    else:
        server.should_exit = True
        raise RuntimeError("test server did not start")
    return SimpleNamespace(url=url, db=db, server=server, thread=thread)


def _teardown(handle: Any) -> None:
    handle.server.should_exit = True
    handle.thread.join(timeout=5)
    handle.db.close()


@pytest.fixture()
def server(tmp_path):
    handle = _make_server(tmp_path, retention=10_000)
    yield handle
    _teardown(handle)


@pytest.fixture()
def server_small_log(tmp_path):
    """Server with a tiny retained log so truncation is easy to trigger."""
    handle = _make_server(tmp_path, retention=20)
    yield handle
    _teardown(handle)


@pytest.fixture()
def client(server) -> httpx.Client:
    with httpx.Client(base_url=server.url, timeout=10) as c:
        yield c


def make_drill(client: httpx.Client, name: str = "演练") -> str:
    resp = client.post("/api/drills", json={"name": name})
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def submit(client: httpx.Client, drill_id: str, event_id: str, channel: str, state: str) -> httpx.Response:
    return client.post(
        f"/api/drills/{drill_id}/events",
        json={"event_id": event_id, "channel": channel, "state": state},
    )


def wait_until(pred: Callable[[], bool], timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


class SseReader:
    """Background SSE consumer that parses frames into a shared list."""

    def __init__(self, url: str):
        self.url = url
        self.frames: list[tuple[str, dict[str, Any]]] = []
        self.error: Exception | None = None
        self.done = threading.Event()
        self._stop = threading.Event()
        self._client: httpx.Client | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "SseReader":
        self._thread.start()
        return self

    def _run(self) -> None:
        try:
            with httpx.Client(timeout=None) as client:
                self._client = client
                with client.stream("GET", self.url) as resp:
                    event, data = "message", []
                    for line in resp.iter_lines():
                        if self._stop.is_set():
                            break
                        if line == "":
                            if data:
                                payload = "\n".join(data)
                                self.frames.append((event, json.loads(payload)))
                            event, data = "message", []
                            continue
                        if line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            event = line[len("event:"):].strip()
                        elif line.startswith("data:"):
                            data.append(line[len("data:"):].strip())
        except Exception as exc:  # noqa: BLE001 - closing the client raises here
            if not self._stop.is_set():
                self.error = exc
        finally:
            self.done.set()

    def stop(self) -> None:
        self._stop.set()
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
        self._thread.join(timeout=5)

    def frames_of(self, kind: str) -> list[dict[str, Any]]:
        return [payload for name, payload in self.frames if name == kind]
