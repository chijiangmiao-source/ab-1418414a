#!/usr/bin/env python3
"""一次性验收服务 verify。

执行内容（全部针对真实 HTTP/SSE 接口的可观察结果）：
  1. 构建检查：Python 字节码编译、前端 JS 语法（node --check）、静态页面可访问；
  2. 代码测试：unittest 存储层用例（原子事务、幂等、并发写）；
  3. API/HTTP 冒烟：演练/事件 CRUD、400/404/409 状态码；
  4. 并发写入与订阅交界：建连固定水位 → 快照与增量恰好分区、无漏报无重复；
  5. 断线补齐：以前次已应用序号恢复，边界严格大于、重发去重；
  6. 过期游标：日志截断后 /resume 与 /stream 均 409 snapshot_required，
     重新获取快照后恢复正常；
  7. Compose 中必须存在一次性退出的 verify 服务定义。

退出码：全部通过 0，任一失败 1。
"""
from __future__ import annotations

import http.client
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from server.app import build_server  # noqa: E402

HOST = "127.0.0.1"


# --------------------------------------------------------------------------- #
# 最小 HTTP / SSE 客户端
# --------------------------------------------------------------------------- #

def http_request(port: str, method: str, path: str, body=None, timeout: float = 10.0):
    conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
    headers = {}
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=payload, headers=headers)
    resp = conn.getresponse()
    raw = resp.read()
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        parsed = None
    return resp.status, dict(resp.getheaders()), parsed, conn


def http_get_raw(port: str, path: str, timeout: float = 10.0):
    conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
    conn.request("GET", path)
    resp = conn.getresponse()
    text = resp.read().decode("utf-8", "replace")
    return resp.status, text, conn


class SSEClient:
    """后台线程读取 SSE，按帧解析 event/data。"""

    def __init__(self, port: int, path: str):
        self.conn = http.client.HTTPConnection(HOST, port, timeout=15)
        self.conn.request("GET", path, headers={"Accept": "text/event-stream"})
        self.resp = self.conn.getresponse()
        if self.resp.status != 200:
            raw = self.resp.read().decode("utf-8", "replace")
            self.conn.close()
            raise AssertionError(f"SSE 建连非 200: {self.resp.status} {raw}")
        ctype = self.resp.getheader("Content-Type", "")
        assert ctype.startswith("text/event-stream"), ctype
        self.q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._read_loop, daemon=True)
        self.thread.start()

    def _read_loop(self) -> None:
        event = "message"
        data_lines: list[str] = []
        try:
            while not self._stop.is_set():
                line = self.resp.readline()
                if not line:
                    break
                line = line.decode("utf-8").rstrip("\r\n")
                if line == "":
                    if data_lines:
                        try:
                            payload = json.loads("\n".join(data_lines))
                        except json.JSONDecodeError:
                            payload = {"_raw": "\n".join(data_lines)}
                        self.q.put((event, payload))
                    event, data_lines = "message", []
                    continue
                if line.startswith(":"):
                    continue  # 注释/心跳
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
        except Exception as exc:  # pragma: no cover
            self.q.put(("__error__", {"message": repr(exc)}))
        finally:
            self.q.put(None)

    def next_event(self, timeout: float = 10.0):
        item = self.q.get(timeout=timeout)
        if item is None:
            raise AssertionError("SSE 连接已关闭")
        if item[0] == "__error__":
            raise AssertionError(f"SSE 读取错误: {item[1]}")
        return item

    def drain(self, timeout: float = 1.0) -> list:
        out = []
        while True:
            try:
                item = self.q.get(timeout=timeout)
            except queue.Empty:
                return out
            if item is None:
                return out
            out.append(item)

    def close(self) -> None:
        self._stop.set()
        try:
            self.conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# 验收框架
# --------------------------------------------------------------------------- #

class Verify:
    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []
        self.port = 0
        self.httpd = None
        self.server_thread = None

    def check(self, name: str, fn) -> None:
        try:
            fn()
        except Exception as exc:
            self.results.append((name, False, f"{type(exc).__name__}: {exc}"))
            print(f"[FAIL] {name}: {exc}")
        else:
            self.results.append((name, True, ""))
            print(f"[PASS] {name}")

    def expect(self, cond: bool, msg: str) -> None:
        if not cond:
            raise AssertionError(msg)

    # ---- 各类验收 -------------------------------------------------------------

    def check_build(self) -> None:
        # 1) Python 字节码编译
        r = subprocess.run(
            [sys.executable, "-m", "compileall", "-q", "server", "verify", "tests"],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.expect(r.returncode == 0, f"compileall 失败: {r.stderr}")

        # 2) 前端 JS 语法
        r = subprocess.run(
            ["node", "--check", os.path.join("web", "app.js")],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.expect(r.returncode == 0, f"node --check 失败: {r.stderr}")

        # 3) 页面与静态资源可访问，且含关键标记
        for path, marker in (
            ("/", "束线联锁监看"),
            ("/app.js", "snapshot_required"),
            ("/styles.css", ".card"),
        ):
            status, text, conn = http_get_raw(self.port, path)
            conn.close()
            self.expect(status == 200, f"GET {path} 状态码 {status}")
            self.expect(marker in text, f"{path} 缺少标记 {marker}")
        # index 关键元素
        self.expect("当前水位" in self._index_html, "页面缺少当前水位")
        for eid in ("channels-table", "events-table", "snapshot-required", "re-snapshot-btn"):
            self.expect(eid in self._index_html, f"页面缺少元素 #{eid}")

    def check_unittests(self) -> None:
        r = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ROOT, "-v"],
            cwd=ROOT, capture_output=True, text=True,
        )
        if r.returncode != 0:
            raise AssertionError("unittest 失败:\n" + r.stdout + r.stderr)

    def check_compose(self) -> None:
        path = os.path.join(ROOT, "docker-compose.yml")
        self.expect(os.path.exists(path), "缺少 docker-compose.yml")
        text = open(path, encoding="utf-8").read()
        try:
            import yaml  # type: ignore
        except ImportError:
            # 无 PyYAML 时退化为结构性检查
            self.expect("verify:" in text, "compose 缺少 verify 服务")
            self.expect("run-verify" in text or "verify.run" in text,
                        "compose verify 未调用 verify 模块")
            self.expect('restart: "no"' in text or "restart: \"no\"" in text,
                        "verify 必须是一次性退出服务（restart: no）")
            return
        cfg = yaml.safe_load(text)
        services = cfg.get("services", {})
        self.expect("verify" in services, "compose 缺少 verify 服务")
        ver = services["verify"]
        self.expect(ver.get("restart") in ("no", None), "verify 不应被自动重启")
        cmd = ver.get("command", "")
        self.expect("verify.run" in str(cmd), "verify 服务命令须执行 verify.run")

    def _create_drill(self, did: str, name: str) -> None:
        status, _, body, conn = http_request(
            self.port, "POST", "/api/drills", {"id": did, "name": name}
        )
        conn.close()
        self.expect(status == 201 and body["id"] == did, f"创建演练失败 {status}")

    def _post_event(self, did: str, eid: str, ch: str, state: str):
        status, _, body, conn = http_request(
            self.port, "POST", f"/api/drills/{did}/events",
            {"event_id": eid, "channel": ch, "state": state},
        )
        conn.close()
        return status, body

    def check_http_smoke(self) -> None:
        did = "smoke"
        self._create_drill(did, "冒烟演练")

        status, body = self._post_event(did, "smoke-1", "BL-Shutter", "normal")
        self.expect(status == 201 and body["seq"] == 1, f"首次提交异常 {status} {body}")

        # 相同标识 + 相同内容重传 → 原序号
        status, body = self._post_event(did, "smoke-1", "BL-Shutter", "normal")
        self.expect(status == 201 and body["seq"] == 1, f"幂等重传应返回序号1: {status} {body}")

        # 相同标识 + 不同状态 → 拒绝
        status, body = self._post_event(did, "smoke-1", "BL-Shutter", "trip")
        self.expect(status == 409 and body["error"] == "event_id_conflict",
                    f"复用标识不同状态应 409: {status} {body}")
        # 相同标识 + 不同通道 → 拒绝
        status, body = self._post_event(did, "smoke-1", "BL-Other", "normal")
        self.expect(status == 409, f"复用标识不同通道应 409: {status} {body}")

        # 投影保持原值、水位不变
        status, _, snap, conn = http_request(self.port, "GET", f"/api/drills/{did}")
        conn.close()
        self.expect(snap["watermark"] == 1, f"冲突提交不得推进水位: {snap['watermark']}")
        ch = {c["channel"]: c for c in snap["channels"]}
        self.expect(ch["BL-Shutter"]["state"] == "normal", "冲突提交不得改写投影")
        self.expect(len(snap["recent_events"]) == 1, "冲突提交不得追加事件日志")

        # 参数校验
        status, body = self._post_event(did, "x", "c", "bogus")
        self.expect(status == 400, f"非法状态应 400: {status}")
        conn = http.client.HTTPConnection(HOST, self.port, timeout=10)
        conn.request("POST", f"/api/drills/{did}/events",
                     body=json.dumps({"channel": "c", "state": "normal"}),
                     headers={"Content-Type": "application/json"})
        self.expect(conn.getresponse().status == 400, "缺 event_id 应 400")
        conn.close()
        status, _, _, conn = http_request(self.port, "GET", "/api/drills/missing")
        conn.close()
        self.expect(status == 404, f"未知演练应 404: {status}")
        status, _, body, conn = http_request(self.port, "GET", "/api/drills")
        conn.close()
        ids = [d["id"] for d in body["drills"]]
        self.expect("smoke" in ids, "演练列表缺少 smoke")

    def check_concurrent_subscription(self) -> None:
        did = "conc"
        self._create_drill(did, "并发交界演练")
        # 全局序号跨演练连续递增，以实际返回序号为准。
        base = []
        for i in range(1, 4):
            status, body = self._post_event(did, f"c-pre-{i}", f"CH{i}", "normal")
            self.expect(status == 201, f"预置事件失败 {status}")
            base.append(body["seq"])
        wm0 = base[-1]
        self.expect(base == [wm0 - 2, wm0 - 1, wm0], f"预置序号不连续: {base}")

        sse = SSEClient(self.port, f"/api/drills/{did}/stream")
        try:
            name, snap = sse.next_event()
            self.expect(name == "snapshot", f"首帧应为 snapshot，实际 {name}")
            self.expect(snap["watermark"] == wm0,
                        f"快照水位应为 {wm0}: {snap['watermark']}")
            # 快照完整一致：每个通道的 last_seq 不超过固定水位
            for c in snap["channels"]:
                self.expect(c["last_seq"] <= wm0, f"快照中出现超水位序号: {c}")
            # 此刻无写入，REST 快照必须与 SSE 快照一致
            _, _, rest_snap, conn = http_request(self.port, "GET", f"/api/drills/{did}")
            conn.close()
            self.expect(rest_snap["watermark"] == wm0 and
                        rest_snap["channels"] == snap["channels"],
                        "SSE 快照与 REST 快照不一致")

            n_per, n_writers = 25, 2
            barrier = threading.Barrier(n_writers + 1)
            posted_seqs: list[int] = []
            seq_lock = threading.Lock()

            def writer(tag: str) -> None:
                barrier.wait()
                for i in range(n_per):
                    st, b = self._post_event(
                        did, f"c-{tag}-{i}", f"CH-{tag}", "trip" if i % 2 else "warn")
                    assert st == 201, (st, b)
                    with seq_lock:
                        posted_seqs.append(b["seq"])

            threads = [threading.Thread(target=writer, args=(t,)) for t in ("a", "b")]
            for t in threads:
                t.start()
            barrier.wait()  # 收到固定水位的快照后才放行并发写
            for t in threads:
                t.join()

            expected = list(range(wm0 + 1, wm0 + 1 + n_per * n_writers))
            got = []
            deadline = time.time() + 15
            while len(got) < len(expected) and time.time() < deadline:
                name, ev = sse.next_event(timeout=max(0.5, deadline - time.time()))
                self.expect(name == "event", f"增量帧类型应为 event: {name}")
                got.append(ev["seq"])
            self.expect(got == expected,
                        f"订阅漏报/重复：期望 {expected[0]}..{expected[-1]}，实得 {got}")
            self.expect(sorted(posted_seqs) == expected, "写入侧序号集合异常")

            # 第二段：全新演练，SSE 建连与并发写入同时开始（连接建立期间的事件）。
            did2 = "race"
            self._create_drill(did2, "建连接力演练")
            n2 = 20
            barrier2 = threading.Barrier(3)
            holder: dict = {}
            posted2: list[dict] = []
            lock2 = threading.Lock()

            def writer2(tag: str) -> None:
                barrier2.wait()
                for i in range(n2):
                    st, b = self._post_event(
                        did2, f"race-{tag}-{i}", f"CH-{tag}",
                        "trip" if i % 2 else "warn")
                    assert st == 201, (st, b)
                    with lock2:
                        posted2.append(b)

            def opener2() -> None:
                barrier2.wait()  # 与写入同时发起，请求处理期间事件持续提交
                holder["sse"] = SSEClient(
                    self.port, f"/api/drills/{did2}/stream")

            ts = [threading.Thread(target=writer2, args=(t,)) for t in ("a", "b")]
            ts.append(threading.Thread(target=opener2))
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            sse2 = holder["sse"]
            try:
                name, snap2 = sse2.next_event()
                self.expect(name == "snapshot", f"竞态首帧应为 snapshot: {name}")
                w2 = snap2["watermark"]
                in_snap = sorted(b["seq"] for b in posted2 if b["seq"] <= w2)
                in_inc = sorted(b["seq"] for b in posted2 if b["seq"] > w2)
                # 每个事件恰好归入其一
                self.expect(len(in_snap) + len(in_inc) == 2 * n2,
                            "事件分区计数不为总数")
                got2 = []
                deadline = time.time() + 15
                while len(got2) < len(in_inc) and time.time() < deadline:
                    nm, ev = sse2.next_event(max(0.5, deadline - time.time()))
                    self.expect(nm == "event", f"增量帧异常: {nm}")
                    got2.append(ev["seq"])
                self.expect(got2 == in_inc,
                            f"建连期间事件漏报/重复：增量应恰为 {len(in_inc)} 条")
                self.expect(len(set(got2)) == len(got2), "增量序号重复")
                # 快照投影必须等价于对 seq<=W 事件的回放
                folded: dict[str, str] = {}
                for b in sorted(posted2, key=lambda x: x["seq"]):
                    if b["seq"] <= w2:
                        folded[b["channel"]] = b["state"]
                snap_map = {c["channel"]: c["state"] for c in snap2["channels"]}
                self.expect(snap_map == folded,
                            f"快照投影与水位内事件回放不一致: {snap_map} vs {folded}")
            finally:
                sse2.close()
        finally:
            sse.close()

    def check_resume_dedup(self) -> None:
        did = "resume"
        self._create_drill(did, "断线恢复演练")
        seqs = []
        for i in range(1, 5):
            status, body = self._post_event(did, f"r-{i}", "CH1", "normal")
            self.expect(status == 201, f"r-{i} 提交失败")
            seqs.append(body["seq"])

        # 以 after=seq(r-2) 建立增量订阅：只有 resumed + 更后续事件
        sse = SSEClient(self.port, f"/api/drills/{did}/stream?after={seqs[1]}")
        try:
            name, info = sse.next_event()
            self.expect(name == "resumed" and info["after"] == seqs[1],
                        f"增量建连首帧异常: {name} {info}")
            pending = []
            while True:
                try:
                    name, ev = sse.next_event(timeout=1.0)
                except queue.Empty:
                    break
                self.expect(name == "event", f"非预期帧: {name}")
                pending.append(ev["seq"])
            self.expect(pending == seqs[2:4],
                        f"待补齐事件应为 {seqs[2:4]}: {pending}")
        finally:
            sse.close()

        # 模拟断线：再写两条，消费到最后一条后以其序号重连
        for i in (5, 6):
            status, body = self._post_event(
                did, f"r-{i}", "CH1", "warn" if i == 5 else "trip")
            self.expect(status == 201, f"r-{i} 提交失败")
            seqs.append(body["seq"])
        last_applied = seqs[5]
        sse = SSEClient(self.port, f"/api/drills/{did}/stream?after={last_applied}")
        try:
            name, _ = sse.next_event()
            self.expect(name == "resumed", "重连首帧应为 resumed")
            # 边界严格大于：重连后不得重放 seq<=last_applied
            self.expect(sse.drain(timeout=0.5) == [],
                        f"after={last_applied} 不应重放任何事件")
            status, body = self._post_event(did, "r-7", "CH1", "normal")
            self.expect(status == 201, "r-7 提交失败")
            seqs.append(body["seq"])
            name, ev = sse.next_event()
            self.expect(name == "event" and ev["seq"] == seqs[6],
                        f"边界重发去重失败: {ev}")
        finally:
            sse.close()

        # 重复恢复请求必须幂等；分区证明：游标相邻 1，边界事件恰好归一边
        status, _, body6, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after={seqs[5]}")
        conn.close()
        self.expect(status == 200 and [e["seq"] for e in body6["events"]] == seqs[6:],
                    f"after=r-6 应只返回 [r-7]: {body6}")
        status, _, body6b, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after={seqs[5]}")
        conn.close()
        self.expect(body6b["events"] == body6["events"], "重复 resume 必须幂等")
        _, _, body5, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after={seqs[4]}")
        conn.close()
        self.expect([e["seq"] for e in body5["events"]] == seqs[5:],
                    f"after=r-5 应返回 r-6,r-7: {body5}")

    def check_stale_cursor(self) -> None:
        did = "stale"
        self._create_drill(did, "截断演练")
        seqs = []
        for i in range(1, 7):
            status, body = self._post_event(did, f"s-{i}", "CH1", "normal")
            self.expect(status == 201, f"s-{i} 提交失败")
            seqs.append(body["seq"])

        # 截断第 4 条之前（删除该演练前 3 条），可恢复下限抬至其序号 -1
        cut = seqs[3]
        status, _, body, conn = http_request(
            self.port, "POST", f"/api/drills/{did}/trim", {"before_seq": cut})
        conn.close()
        floor = cut - 1
        self.expect(status == 200 and body["removed"] == 3,
                    f"截断返回异常: {status} {body}")
        self.expect(body["recoverable_after"] == floor,
                    f"可恢复下限应为 {floor}: {body}")

        # 过期游标：REST 恢复接口与 SSE 建连都必须明确拒绝
        stale_cursor = seqs[1]  # 中间存在已删除空洞
        status, _, body, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after={stale_cursor}")
        conn.close()
        self.expect(status == 409 and body["error"] == "snapshot_required",
                    f"过期游标 resume 应 409 snapshot_required: {status} {body}")
        self.expect(body["reason"] == "log_truncated" and
                    body["recoverable_after"] == floor, "409 载荷缺少可恢复范围")

        try:
            SSEClient(self.port, f"/api/drills/{did}/stream?after={stale_cursor}")
            raise AssertionError("过期游标 SSE 不应建立成功")
        except AssertionError as exc:
            self.expect("409" in str(exc) and "snapshot_required" in str(exc),
                        f"过期游标 SSE 应返回 409 JSON: {exc}")

        # 游标超前水位同样不可恢复
        status, _, body, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after=99999999")
        conn.close()
        self.expect(status == 409 and body["reason"] == "cursor_ahead_of_watermark",
                    f"超前游标应 409: {status} {body}")

        # 边界内游标仍可恢复（恰好等于下限时补齐剩余全部）
        status, _, body, conn = http_request(
            self.port, "GET", f"/api/drills/{did}/resume?after={seqs[2]}")
        conn.close()
        self.expect(status == 200 and [e["seq"] for e in body["events"]] == seqs[3:],
                    f"after=s-3 应返回 s-4..s-6: {status} {body}")

        # 明确要求重新获取快照：全新订阅给出当前水位的完整一致快照，
        # 截断只删日志，投影仍在；随后增量正常推送。
        sse = SSEClient(self.port, f"/api/drills/{did}/stream")
        try:
            name, snap = sse.next_event()
            self.expect(name == "snapshot" and snap["watermark"] == seqs[5],
                        f"重取快照异常: {name} {snap.get('watermark')}")
            self.expect(len(snap["channels"]) == 1 and
                        snap["channels"][0]["last_seq"] == seqs[5],
                        "快照投影在截断后必须保持完整")
            status, body = self._post_event(did, "s-7", "CH1", "trip")
            self.expect(status == 201, "s-7 提交失败")
            seqs.append(body["seq"])
            name, ev = sse.next_event()
            self.expect(name == "event" and ev["seq"] == seqs[6] and ev["state"] == "trip",
                        f"重取快照后增量异常: {name} {ev}")
        finally:
            sse.close()

    # ---- 主流程 ---------------------------------------------------------------

    def run(self) -> int:
        tmp = tempfile.mkdtemp(prefix="interlock-verify-")
        db_path = os.path.join(tmp, "verify.db")
        self.httpd = build_server(HOST, 0, db_path)
        self.port = self.httpd.server_address[1]
        self.server_thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.server_thread.start()
        time.sleep(0.2)

        with open(os.path.join(ROOT, "web", "index.html"), encoding="utf-8") as fh:
            self._index_html = fh.read()

        print(f"== verify 目标: http://{HOST}:{self.port} ==\n")
        groups = [
            ("构建检查（compileall / node --check / 静态页面）", self.check_build),
            ("代码测试（unittest）", self.check_unittests),
            ("API/HTTP 冒烟（CRUD/幂等/409/400/404）", self.check_http_smoke),
            ("并发写入与订阅交界（快照/增量恰好分区）", self.check_concurrent_subscription),
            ("断线补齐（游标恢复/边界严格大于/去重）", self.check_resume_dedup),
            ("过期游标与日志截断（snapshot_required/重取快照）", self.check_stale_cursor),
            ("Compose verify 一次性服务定义", self.check_compose),
        ]
        for name, fn in groups:
            print(f"\n--- {name} ---")
            self.check(name, fn)

        self.httpd.shutdown()
        self.httpd.server_close()

        passed = sum(1 for _, ok, _ in self.results if ok)
        total = len(self.results)
        print("\n================ SUMMARY ================")
        for name, ok, detail in self.results:
            print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"\n      {detail}"))
        print(f"\n{passed}/{total} 组验收通过")
        return 0 if passed == total else 1


def main() -> None:
    raise SystemExit(Verify().run())


if __name__ == "__main__":
    main()
