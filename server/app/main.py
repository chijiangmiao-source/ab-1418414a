"""HTTP API + SSE subscription for the interlock monitor.

Subscription semantics
----------------------
``GET /api/drills/{id}/stream`` (SSE):

* without ``after``: the server pins the current watermark and immediately
  sends one ``snapshot`` frame containing every channel state at that
  watermark; afterwards it only pushes ``event`` frames with a strictly
  higher ``seq``. Every event therefore falls into exactly one of the two
  buckets — snapshot or delta — never both, never neither.
* with ``after=N`` (resume after a disconnect): the cursor is validated
  against the retained log. If valid, a ``resume`` frame is sent and only
  events with ``seq > N`` follow, so a boundary retransmission never
  re-delivers an already-applied event. If the cursor is out of the
  recoverable range (log truncated / cursor ahead), a single ``resync``
  frame is sent and the stream ends, telling the client to re-fetch a
  snapshot.

``GET /api/drills/{id}/events?after=N`` exposes the same cursor rules over
plain REST and answers ``410 Gone`` with ``code=resync_required`` when the
cursor can no longer be served.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .db import ConflictError, CursorExpiredError, Database, NotFoundError

SSE_POLL_INTERVAL = float(os.environ.get("SSE_POLL_INTERVAL", "0.15"))
SSE_PING_EVERY = 5  # empty polls between keep-alive comments

DEFAULT_WEB_DIST = Path(__file__).resolve().parents[2] / "web" / "dist"


def _sse(frame: str, data: dict[str, Any]) -> str:
    return f"event: {frame}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _non_blank(v: str) -> str:
    v = v.strip()
    if not v:
        raise ValueError("must not be blank")
    return v


class DrillCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)

    _check = field_validator("name")(_non_blank)


class EventSubmit(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    channel: str = Field(min_length=1, max_length=64)
    state: str = Field(min_length=1, max_length=64)

    _check = field_validator("event_id", "channel", "state")(_non_blank)


def _not_found(drill_id: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"code": "drill_not_found", "drill_id": drill_id},
    )


def _resync_required(err: CursorExpiredError) -> HTTPException:
    return HTTPException(
        status_code=410,
        detail={
            "code": "resync_required",
            "reason": err.reason,
            "min_available_seq": err.min_available_seq,
            "watermark": err.watermark,
            "message": "游标已超出可恢复范围，请重新获取快照",
        },
    )


async def _event_generator(
    db: Database, drill_id: str, after: int | None, request: Request
) -> AsyncIterator[str]:
    try:
        if after is None:
            # Pin the watermark and ship the consistent snapshot within it.
            try:
                snap = await asyncio.to_thread(db.snapshot, drill_id)
            except NotFoundError:
                yield _sse("error", {"code": "drill_not_found", "drill_id": drill_id})
                return
            yield _sse("snapshot", snap)
            last = snap["watermark"]
        else:
            # Resume: only events with seq > after may follow.
            try:
                await asyncio.to_thread(db.check_cursor, drill_id, after)
            except CursorExpiredError as err:
                yield _sse(
                    "resync",
                    {
                        "code": "resync_required",
                        "reason": err.reason,
                        "min_available_seq": err.min_available_seq,
                        "watermark": err.watermark,
                        "message": "游标已超出可恢复范围，请重新获取快照",
                    },
                )
                return
            except NotFoundError:
                yield _sse("error", {"code": "drill_not_found", "drill_id": drill_id})
                return
            watermark = await asyncio.to_thread(db.watermark)
            yield _sse("resume", {"after": after, "watermark": watermark})
            last = after

        empty_polls = 0
        while True:
            if await request.is_disconnected():
                return
            events = await asyncio.to_thread(db.list_events_after, drill_id, last, 500)
            if events:
                for event in events:
                    yield _sse("event", event)
                    last = event["seq"]
                empty_polls = 0
            else:
                empty_polls += 1
                if empty_polls % SSE_PING_EVERY == 0:
                    yield ": ping\n\n"
            await asyncio.sleep(SSE_POLL_INTERVAL)
    except asyncio.CancelledError:  # client went away mid-send
        return


def create_app(db: Database | None = None, web_dist: str | None = None) -> FastAPI:
    app = FastAPI(title="束线联锁监看台", version="1.0.0")
    database = db or Database.from_env()
    app.state.db = database

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "watermark": database.watermark(),
            "retention": database.retention,
        }

    @app.post("/api/drills", status_code=201)
    def create_drill(body: DrillCreate) -> dict[str, Any]:
        return database.create_drill(body.name)

    @app.get("/api/drills")
    def list_drills() -> list[dict[str, Any]]:
        return database.list_drills()

    @app.get("/api/drills/{drill_id}")
    def drill_detail(drill_id: str) -> dict[str, Any]:
        try:
            return database.drill_detail(drill_id)
        except NotFoundError:
            raise _not_found(drill_id)

    @app.get("/api/drills/{drill_id}/snapshot")
    def snapshot(drill_id: str) -> dict[str, Any]:
        try:
            return database.snapshot(drill_id)
        except NotFoundError:
            raise _not_found(drill_id)

    @app.get("/api/drills/{drill_id}/events")
    def list_events(
        drill_id: str,
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> dict[str, Any]:
        try:
            database.check_cursor(drill_id, after)
            events = database.list_events_after(drill_id, after, limit)
            detail = database.drill_detail(drill_id)
        except NotFoundError:
            raise _not_found(drill_id)
        except CursorExpiredError as err:
            raise _resync_required(err)
        return {
            "watermark": detail["watermark"],
            "min_available_seq": detail["min_available_seq"],
            "events": events,
        }

    @app.post("/api/drills/{drill_id}/events", status_code=201)
    def submit_event(drill_id: str, body: EventSubmit, response: Response) -> dict[str, Any]:
        try:
            row, deduplicated = database.submit_event(
                drill_id, body.event_id, body.channel, body.state
            )
        except NotFoundError:
            raise _not_found(drill_id)
        except ConflictError as err:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "event_id_conflict",
                    "message": "事件标识已被不同内容占用，已拒绝且未改写投影",
                    "existing": err.existing,
                },
            )
        if deduplicated:
            response.status_code = 200
        return {**row, "deduplicated": deduplicated}

    @app.get("/api/drills/{drill_id}/stream")
    async def stream(drill_id: str, request: Request, after: int | None = None):
        if database.get_drill(drill_id) is None:
            raise _not_found(drill_id)
        return StreamingResponse(
            _event_generator(database, drill_id, after, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # Static monitoring page (built by `npm run build` in web/). Mounted
    # last so the /api routes above always win.
    dist = web_dist or os.environ.get("WEB_DIST") or str(DEFAULT_WEB_DIST)
    if os.path.isdir(dist):
        app.mount("/", StaticFiles(directory=dist, html=True), name="web")

    return app


def build_app() -> FastAPI:
    """Uvicorn factory entrypoint (avoids opening a database at import time)."""
    return create_app()
