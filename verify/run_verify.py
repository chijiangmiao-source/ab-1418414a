#!/usr/bin/env python3
"""一次性验收服务（执行后退出，以退出码报告结果）。

覆盖内容：
  1. 代码测试        —— 运行 server/tests 下的 pytest 套件
  2. 页面构建检查    —— npm ci + npm run build，并校验构建产物
  3. API/HTTP 冒烟   —— 健康检查、演练创建、幂等重传、冲突拒绝、快照一致性、页面可达
  4. 场景：并发写入与订阅交界 —— 快照/增量恰好一次、不漏不重
  5. 场景：断线补齐与边界重发去重
  6. 场景：过期游标 —— REST 410 与 SSE resync 帧均要求重新获取快照

环境变量：
  APP_URL  被验收服务的地址（默认 http://127.0.0.1:8000）
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable

import httpx

ROOT = Path(__file__).resolve().parent.parent
APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8000").rstrip("/")
RESULTS: list[tuple[str, bool, str]] = []


# --------------------------------------------------------------------------
# SSE helper
# --------------------------------------------------------------------------
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
                                self.frames.append((event, json.loads("\n".join(data))))
                            event, data = "message", []
                            continue
                        if line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            event = line[len("event:"):].strip()
                        elif line.startswith("data:"):
                            data.append(line[len("data:"):].strip())
        except Exception as exc:  # closing the client surfaces here
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


def wait_until(pred: Callable[[], bool], timeout: float = 15.0, interval: float = 0.02) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------
def step_code_tests() -> None:
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "server/tests"],
        cwd=ROOT,
        env=env,
    )
    assert proc.returncode == 0, f"pytest 退出码 {proc.returncode}"


def step_web_build() -> None:
    web = ROOT / "web"
    proc = subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=web)
    assert proc.returncode == 0, "npm ci 失败"
    proc = subprocess.run(["npm", "run", "build"], cwd=web)
    assert proc.returncode == 0, "npm run build 失败"
    index = web / "dist" / "index.html"
    assert index.is_file(), "缺少 dist/index.html"
    assets = list((web / "dist" / "assets").glob("*.js"))
    assert assets, "缺少构建后的 JS 资产"
    html = index.read_text(encoding="utf-8")
    assert 'id="root"' in html, "index.html 缺少挂载点"


def step_wait_ready() -> None:
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{APP_URL}/api/health", timeout=2)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(1)
    raise AssertionError(f"服务未就绪：{APP_URL}")


def step_api_smoke() -> None:
    with httpx.Client(base_url=APP_URL, timeout=10) as client:
        health = client.get("/api/health").json()
        assert health["status"] == "ok"

        # 监看页面可达
        index = client.get("/")
        assert index.status_code == 200 and 'id="root"' in index.text, "监看页不可达"

        drill = client.post("/api/drills", json={"name": "验收演练-冒烟"}).json()
        drill_id = drill["id"]

        submitted = client.post(
            f"/api/drills/{drill_id}/events",
            json={"event_id": "smoke-1", "channel": "BL-01", "state": "alarm"},
        )
        assert submitted.status_code == 201, submitted.text
        seq = submitted.json()["seq"]

        # 相同标识及内容重传 -> 返回原序号
        replay = client.post(
            f"/api/drills/{drill_id}/events",
            json={"event_id": "smoke-1", "channel": "BL-01", "state": "alarm"},
        )
        assert replay.status_code == 200 and replay.json()["deduplicated"] is True
        assert replay.json()["seq"] == seq

        # 相同标识、不同内容 -> 拒绝且不改写投影
        conflict = client.post(
            f"/api/drills/{drill_id}/events",
            json={"event_id": "smoke-1", "channel": "BL-01", "state": "normal"},
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["code"] == "event_id_conflict"

        snapshot = client.get(f"/api/drills/{drill_id}/snapshot").json()
        row = next(c for c in snapshot["channels"] if c["channel"] == "BL-01")
        assert row["state"] == "alarm" and row["seq"] == seq
        assert snapshot["watermark"] >= seq

        events = client.get(f"/api/drills/{drill_id}/events", params={"after": 0}).json()
        assert any(e["seq"] == seq for e in events["events"])


def step_scenario_boundary() -> None:
    """并发写入与订阅交界：每个事件恰好归入快照或增量之一。"""
    with httpx.Client(base_url=APP_URL, timeout=10) as client:
        drill_id = client.post("/api/drills", json={"name": "验收演练-边界"}).json()["id"]
        submitted: list[tuple[dict[str, str], int]] = []
        lock = threading.Lock()

        def batch(prefix: str, count: int) -> None:
            for i in range(count):
                body = {
                    "event_id": f"boundary-{prefix}-{i}",
                    "channel": f"BL-0{1 + i % 3}",
                    "state": ["normal", "warning", "alarm", "bypass"][i % 4],
                }
                resp = client.post(f"/api/drills/{drill_id}/events", json=body)
                assert resp.status_code == 201, resp.text
                with lock:
                    submitted.append((body, resp.json()["seq"]))

        batch("base", 5)
        reader = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream").start()
        assert wait_until(lambda: len(reader.frames_of("snapshot")) == 1, timeout=10), "未收到快照帧"
        snapshot = reader.frames_of("snapshot")[0]
        watermark = snapshot["watermark"]

        writers = [threading.Thread(target=batch, args=(f"w{n}", 10)) for n in range(3)]
        for t in writers:
            t.start()
        for t in writers:
            t.join()

        expected = {seq for _, seq in submitted if seq > watermark}
        assert wait_until(
            lambda: expected <= {e["seq"] for e in reader.frames_of("event")}, timeout=20
        ), "增量流未覆盖全部水位后事件"
        reader.stop()

        delta = [e["seq"] for e in reader.frames_of("event")]
        assert len(delta) == len(set(delta)), "增量流出现重复事件"
        assert all(s > watermark for s in delta), "增量流混入水位内事件"
        assert set(delta) == expected, "水位后事件未恰好全部进入增量流"

        snap_state = {c["channel"]: c for c in snapshot["channels"]}
        for body, seq in submitted:
            if seq <= watermark:
                assert snap_state[body["channel"]]["seq"] >= seq, "快照漏掉了水位内事件"

        final: dict[str, str] = {}
        for body, seq in sorted(submitted, key=lambda item: item[1]):
            final[body["channel"]] = body["state"]
        after = client.get(f"/api/drills/{drill_id}/snapshot").json()
        assert {c["channel"]: c["state"] for c in after["channels"]} == final, "最终投影与事件重放不一致"


def step_scenario_resume() -> None:
    """断线补齐：以已应用序号恢复，只收到遗漏事件，边界不重发。"""
    with httpx.Client(base_url=APP_URL, timeout=10) as client:
        drill_id = client.post("/api/drills", json={"name": "验收演练-断线"}).json()["id"]

        def post(eid: str, channel: str, state: str) -> int:
            resp = client.post(
                f"/api/drills/{drill_id}/events",
                json={"event_id": eid, "channel": channel, "state": state},
            )
            assert resp.status_code == 201, resp.text
            return resp.json()["seq"]

        base = [post(f"resume-{i}", "BL-01", "normal") for i in range(5)]
        third = base[2]  # 前 3 条视为断线前已应用

        first = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream?after={third}").start()
        assert wait_until(lambda: len(first.frames_of("event")) >= 2, timeout=10)
        first.stop()
        assert [e["seq"] for e in first.frames_of("event")] == base[3:], "边界事件被重复推送"

        missed = [post(f"resume-{i}", "BL-02", "alarm") for i in range(5, 8)]
        last_applied = base[-1]
        second = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream?after={last_applied}").start()
        assert wait_until(lambda: len(second.frames_of("event")) >= len(missed), timeout=10)
        second.stop()
        assert [e["seq"] for e in second.frames_of("event")] == missed, "断线期间事件未恰好补齐"

        newest = missed[-1]
        third_conn = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream?after={newest}").start()
        assert wait_until(lambda: len(third_conn.frames_of("resume")) == 1, timeout=10)
        extra = post("resume-extra", "BL-01", "bypass")
        assert wait_until(lambda: len(third_conn.frames_of("event")) >= 1, timeout=10)
        third_conn.stop()
        seqs = [e["seq"] for e in third_conn.frames_of("event")]
        assert seqs == [extra], "最新游标重连出现重发或漏发"


def step_scenario_expired_cursor() -> None:
    """过期游标：REST 返回 410，SSE 返回 resync 帧，均要求重新获取快照。"""
    with httpx.Client(base_url=APP_URL, timeout=30) as client:
        retention = client.get("/api/health").json()["retention"]
        drill_id = client.post("/api/drills", json={"name": "验收演练-截断"}).json()["id"]

        total = retention + 25
        lock = threading.Lock()
        errors: list[str] = []

        def batch(worker: int, count: int) -> None:
            for i in range(count):
                resp = client.post(
                    f"/api/drills/{drill_id}/events",
                    json={
                        "event_id": f"trunc-{worker}-{i}",
                        "channel": "BL-01",
                        "state": "normal" if i % 2 == 0 else "warning",
                    },
                )
                if resp.status_code != 201:
                    with lock:
                        errors.append(resp.text)

        threads = [threading.Thread(target=batch, args=(w, total // 4 + (1 if w < total % 4 else 0))) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, f"写入失败: {errors[:3]}"

        detail = client.get(f"/api/drills/{drill_id}").json()
        min_available = detail["min_available_seq"]
        assert min_available is not None and min_available > 1, "日志未按保留策略截断"

        expired = client.get(f"/api/drills/{drill_id}/events", params={"after": min_available - 2})
        assert expired.status_code == 410, f"过期游标应返回 410，实际 {expired.status_code}"
        body = expired.json()["detail"]
        assert body["code"] == "resync_required" and body["reason"] == "cursor_expired"

        ahead = client.get(
            f"/api/drills/{drill_id}/events", params={"after": detail["watermark"] + 1000}
        )
        assert ahead.status_code == 410 and ahead.json()["detail"]["reason"] == "cursor_out_of_range"

        reader = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream?after={min_available - 2}").start()
        assert reader.done.wait(timeout=10), "过期游标的 SSE 未按要求结束"
        reader.stop()
        assert len(reader.frames) == 1 and reader.frames[0][0] == "resync", "缺少 resync 帧"
        assert reader.frames[0][1]["code"] == "resync_required"

        # 重新获取快照后订阅恢复正常
        fresh = SseReader(f"{APP_URL}/api/drills/{drill_id}/stream").start()
        assert wait_until(lambda: len(fresh.frames_of("snapshot")) == 1, timeout=10)
        snap = fresh.frames_of("snapshot")[0]
        assert snap["watermark"] == detail["watermark"]
        fresh.stop()


# --------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------
STEPS: list[tuple[str, Callable[[], None]]] = [
    ("代码测试 (pytest)", step_code_tests),
    ("页面构建检查 (npm build)", step_web_build),
    ("等待服务就绪", step_wait_ready),
    ("API/HTTP 冒烟", step_api_smoke),
    ("场景: 并发写入与订阅交界", step_scenario_boundary),
    ("场景: 断线补齐与边界重发去重", step_scenario_resume),
    ("场景: 过期游标要求重新获取快照", step_scenario_expired_cursor),
]


def main() -> int:
    print(f"验收目标: {APP_URL}", flush=True)
    for name, fn in STEPS:
        print(f"\n=== {name} ===", flush=True)
        started = time.time()
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - report and continue
            RESULTS.append((name, False, str(exc)))
            print(f"--- FAIL: {name}: {exc}", flush=True)
            traceback.print_exc()
        else:
            RESULTS.append((name, True, ""))
            print(f"--- PASS: {name} ({time.time() - started:.1f}s)", flush=True)

    print("\n================ 验收结果 ================", flush=True)
    failed = 0
    for name, ok, message in RESULTS:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        suffix = f" — {message}" if message else ""
        print(f"[{mark}] {name}{suffix}", flush=True)
    print(f"通过 {len(RESULTS) - failed}/{len(RESULTS)}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
