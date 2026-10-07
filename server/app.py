"""HTTP API + SSE 订阅 + 静态页面。

关键接口
--------
POST   /api/drills                       创建演练
GET    /api/drills                       演练列表
GET    /api/drills/{id}                  快照 + 最近事件（按全局序号排列）
POST   /api/drills/{id}/events           提交带稳定事件标识的通道状态变更
GET    /api/drills/{id}/resume?after=N   校验断线游标（409=snapshot_required）
GET    /api/drills/{id}/stream?after=N   SSE：建连固定水位，先快照后增量
POST   /api/drills/{id}/trim             日志截断（运维/演练用）

订阅边界：无游标时推送 ``event:snapshot``（水位 W 内一致快照），此后仅推
``seq > W`` 的事件；带游标且可恢复时仅推 ``seq > after``。边界严格大于，
重连边界事件天然不重复。
"""
from __future__ import annotations

import json
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .storage import ConflictError, Store

VALID_STATES = ("normal", "warn", "trip", "bypassed")
SNAPSHOT_REQUIRED = "snapshot_required"


class App:
    def __init__(self, store: Store) -> None:
        self.store = store


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def make_handler(app: App):
    store = app.store

    class Handler(BaseHTTPRequestHandler):
        server_version = "InterlockMonitor/1.0"

        def log_message(self, fmt, *args):  # 安静一些；verify 自行报告
            pass

        # ---- 工具 --------------------------------------------------------------

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
                return None
            if not isinstance(data, dict):
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
                return None
            return data

        def _require_drill(self, drill_id: str) -> bool:
            if not store.drill_exists(drill_id):
                _json_response(self, HTTPStatus.NOT_FOUND, {"error": "drill_not_found"})
                return False
            return True

        # ---- 路由 --------------------------------------------------------------

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            qs = parse_qs(parsed.query)

            if parts == ["api", "drills"]:
                _json_response(self, HTTPStatus.OK, {"drills": store.list_drills()})
                return
            if len(parts) == 3 and parts[:2] == ["api", "drills"]:
                drill_id = parts[2]
                if not self._require_drill(drill_id):
                    return
                payload = store.snapshot(drill_id)
                payload["drill_id"] = drill_id
                payload["recent_events"] = store.events_recent(drill_id, 50)
                _json_response(self, HTTPStatus.OK, payload)
                return
            if len(parts) == 4 and parts[:2] == ["api", "drills"] and parts[3] == "resume":
                self._handle_resume(parts[2], qs)
                return
            if len(parts) == 4 and parts[:2] == ["api", "drills"] and parts[3] == "stream":
                self._handle_stream(parts[2], qs)
                return
            if parsed.path in ("/", "/index.html"):
                self._serve_static("index.html", "text/html; charset=utf-8")
                return
            if parsed.path == "/app.js":
                self._serve_static("app.js", "application/javascript; charset=utf-8")
                return
            if parsed.path == "/styles.css":
                self._serve_static("styles.css", "text/css; charset=utf-8")
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:
            parts = [p for p in urlparse(self.path).path.split("/") if p]
            if parts == ["api", "drills"]:
                self._create_drill()
                return
            if len(parts) == 4 and parts[:2] == ["api", "drills"] and parts[3] == "events":
                self._submit_event(parts[2])
                return
            if len(parts) == 4 and parts[:2] == ["api", "drills"] and parts[3] == "trim":
                self._trim_log(parts[2])
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

        # ---- 业务处理 -----------------------------------------------------------

        def _create_drill(self) -> None:
            data = self._read_json()
            if data is None:
                return
            drill_id = str(data.get("id") or uuid.uuid4().hex[:12])
            name = str(data.get("name") or f"drill-{drill_id}")
            store.create_drill(drill_id, name, time.time())
            _json_response(self, HTTPStatus.CREATED, {"id": drill_id, "name": name})

        def _submit_event(self, drill_id: str) -> None:
            if not self._require_drill(drill_id):
                return
            data = self._read_json()
            if data is None:
                return
            event_id = data.get("event_id")
            channel = data.get("channel")
            state = data.get("state")
            if not isinstance(event_id, str) or not event_id.strip():
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "event_id_required"})
                return
            if not isinstance(channel, str) or not channel.strip():
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "channel_required"})
                return
            if state not in VALID_STATES:
                _json_response(
                    self, HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_state", "allowed": list(VALID_STATES)},
                )
                return
            try:
                seq = store.submit_event(drill_id, event_id.strip(), channel.strip(), state, time.time())
            except ConflictError as exc:
                _json_response(
                    self, HTTPStatus.CONFLICT,
                    {"error": "event_id_conflict", "message": str(exc)},
                )
                return
            _json_response(
                self, HTTPStatus.CREATED,
                {"seq": seq, "event_id": event_id, "channel": channel, "state": state},
            )

        def _trim_log(self, drill_id: str) -> None:
            if not self._require_drill(drill_id):
                return
            data = self._read_json()
            if data is None:
                return
            before = data.get("before_seq")
            if not isinstance(before, int) or before < 1:
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "before_seq_required"})
                return
            removed = store.trim_events(drill_id, before)
            _json_response(
                self, HTTPStatus.OK,
                {"removed": removed, "before_seq": before,
                 "recoverable_after": store.recoverable_floor(drill_id)},
            )

        def _handle_resume(self, drill_id: str, qs: dict) -> None:
            if not self._require_drill(drill_id):
                return
            try:
                after = int(qs.get("after", ["0"])[0])
            except ValueError:
                _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "after_must_be_int"})
                return
            kind, info = store.open_resume_subscription(drill_id, after)
            if kind == "stale":
                _json_response(
                    self, HTTPStatus.CONFLICT,
                    {
                        "error": SNAPSHOT_REQUIRED,
                        "reason": "log_truncated",
                        **info,
                        "message": "事件日志已截断，游标超出可恢复范围，必须重新获取快照",
                    },
                )
                return
            if kind == "ahead":
                _json_response(
                    self, HTTPStatus.CONFLICT,
                    {
                        "error": SNAPSHOT_REQUIRED,
                        "reason": "cursor_ahead_of_watermark",
                        **info,
                        "message": "游标超前于当前水位，必须重新获取快照",
                    },
                )
                return
            _json_response(
                self, HTTPStatus.OK,
                {"ok": True, "after": after, "watermark": info["watermark"],
                 "events": info["events"]},
            )

        # ---- SSE ----------------------------------------------------------------

        def _sse_start(self) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

        def _sse_send(self, event: str, data: dict) -> bool:
            payload = "".join(
                f"event: {event}\ndata: {line}\n\n"
                for line in json.dumps(data, ensure_ascii=False).splitlines()
            )
            try:
                self.wfile.write(payload.encode("utf-8"))
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError):
                return False

        def _stream_loop(self, drill_id: str, last_seq: int) -> None:
            """SSE 已建立：仅推送 seq 严格大于 last_seq 的事件。"""
            while True:
                evs = store.wait_events(drill_id, last_seq, timeout=15)
                if not evs:
                    try:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    continue
                for ev in evs:
                    if not self._sse_send("event", ev):
                        return
                    last_seq = ev["seq"]

        def _handle_stream(self, drill_id: str, qs: dict) -> None:
            if not self._require_drill(drill_id):
                return
            after_raw = qs.get("after", [None])[0]
            try:
                if after_raw is None:
                    # 建连固定水位 W：原子取得 W 内快照与 W 之后已提交事件。
                    snap, pending = store.open_fresh_subscription(drill_id)
                    self._sse_start()
                    if not self._sse_send("snapshot", snap):
                        return
                    last_seq = snap["watermark"]
                    for ev in pending:
                        if not self._sse_send("event", ev):
                            return
                        last_seq = ev["seq"]
                else:
                    try:
                        after = int(after_raw)
                    except ValueError:
                        _json_response(self, HTTPStatus.BAD_REQUEST,
                                       {"error": "after_must_be_int"})
                        return
                    kind, info = store.open_resume_subscription(drill_id, after)
                    # 过期/越界游标：在 SSE 建立前直接以 409 + JSON 拒绝，
                    # API/HTTP 可观察，页面据此明确要求重新获取快照。
                    if kind == "stale":
                        _json_response(
                            self, HTTPStatus.CONFLICT,
                            {
                                "error": SNAPSHOT_REQUIRED,
                                "reason": "log_truncated",
                                **info,
                                "message": "事件日志已截断，游标超出可恢复范围，必须重新获取快照",
                            },
                        )
                        return
                    if kind == "ahead":
                        _json_response(
                            self, HTTPStatus.CONFLICT,
                            {
                                "error": SNAPSHOT_REQUIRED,
                                "reason": "cursor_ahead_of_watermark",
                                **info,
                                "message": "游标超前于当前水位，必须重新获取快照",
                            },
                        )
                        return
                    self._sse_start()
                    if not self._sse_send(
                        "resumed",
                        {"after": after, "watermark": info["watermark"]},
                    ):
                        return
                    last_seq = after
                    for ev in info["events"]:
                        if not self._sse_send("event", ev):
                            return
                        last_seq = ev["seq"]
                # 此后只推送更高序号，直到客户端断开。
                self._stream_loop(drill_id, last_seq)
            except (BrokenPipeError, ConnectionResetError):
                return

        # ---- 静态资源 -----------------------------------------------------------

        def _serve_static(self, name: str, content_type: str) -> None:
            import os
            path = os.path.join(os.path.dirname(__file__), "..", "web", name)
            try:
                with open(path, "rb") as fh:
                    body = fh.read()
            except OSError:
                _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def build_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    store = Store(db_path)
    app = App(store)
    httpd = ThreadingHTTPServer((host, port), make_handler(app))
    httpd.store = store  # type: ignore[attr-defined]
    return httpd
