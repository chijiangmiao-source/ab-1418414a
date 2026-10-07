"use strict";

// 监看页：建连先取“固定水位 + 水位内快照”，之后只接收更高序号事件；
// 断线以前次已应用序号恢复，收到 snapshot_required 则明确要求重新获取快照。

const state = {
  drillId: null,
  lastSeq: 0,          // 已应用的最大全局序号
  watermark: 0,        // 订阅建立时固定的水位
  channels: new Map(),
  events: [],
  source: null,
  resuming: false,
};

const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await resp.json().catch(() => ({}));
  return { ok: resp.ok, status: resp.status, body };
}

function setConn(on, text) {
  const dot = $("conn-dot");
  dot.className = "dot " + (on === true ? "on" : on === "err" ? "err" : "off");
  $("conn-text").textContent = text;
}

function showSnapshotRequired(reason) {
  $("snapshot-reason").textContent =
    reason || "游标已超出可恢复范围（日志截断或游标无效），增量无法补齐。";
  $("snapshot-required").classList.remove("hidden");
  setConn("err", "订阅失效：需要重新获取快照");
}

function hideSnapshotRequired() {
  $("snapshot-required").classList.add("hidden");
}

function render() {
  $("watermark").textContent = String(state.lastSeq || state.watermark || 0);

  const chBody = $("channels-table").querySelector("tbody");
  if (state.channels.size === 0) {
    chBody.innerHTML = '<tr><td colspan="3" class="muted">暂无数据</td></tr>';
  } else {
    chBody.innerHTML = [...state.channels.values()]
      .sort((a, b) => a.channel.localeCompare(b.channel))
      .map(
        (c) =>
          `<tr><td>${c.channel}</td>` +
          `<td class="state-${c.state}">${c.state}</td>` +
          `<td>${c.last_seq}</td></tr>`
      )
      .join("");
  }

  const evBody = $("events-table").querySelector("tbody");
  if (state.events.length === 0) {
    evBody.innerHTML = '<tr><td colspan="5" class="muted">暂无事件</td></tr>';
  } else {
    evBody.innerHTML = state.events
      .slice(-50)
      .map(
        (e) =>
          `<tr><td>${e.seq}</td><td>${e.event_id}</td><td>${e.channel}</td>` +
          `<td class="state-${e.state}">${e.state}</td>` +
          `<td>${new Date(e.occurred_at * 1000).toLocaleTimeString()}</td></tr>`
      )
      .join("");
  }
}

function applyEvent(ev) {
  // 边界严格大于：序号 <= 已应用序号的事件忽略（去重，防止任何重发）。
  if (ev.seq <= state.lastSeq) return;
  state.lastSeq = ev.seq;
  state.channels.set(ev.channel, {
    channel: ev.channel,
    state: ev.state,
    last_seq: ev.seq,
  });
  state.events.push(ev);
  if (state.events.length > 200) state.events = state.events.slice(-200);
  render();
}

function applySnapshot(snap) {
  state.watermark = snap.watermark;
  state.lastSeq = snap.watermark;
  state.channels = new Map(
    snap.channels.map((c) => [c.channel, c])
  );
  state.events = snap.recent_events ? [...snap.recent_events] : state.events;
  render();
}

function closeStream() {
  if (state.source) {
    state.source.onmessage = null;
    state.source.onerror = null;
    state.source.close();
    state.source = null;
  }
}

// 不带游标建连：服务端固定水位 W 并返回 W 内快照，此后仅推 seq > W。
function subscribeFresh() {
  hideSnapshotRequired();
  closeStream();
  state.resuming = false;
  const src = new EventSource(`/api/drills/${state.drillId}/stream`);
  state.source = src;
  setConn(true, "已连接（快照水位 " + state.watermark + "）");

  src.addEventListener("snapshot", (msg) => {
    const snap = JSON.parse(msg.data);
    state.watermark = snap.watermark;
    state.lastSeq = snap.watermark;
    state.channels = new Map(snap.channels.map((c) => [c.channel, c]));
    render();
    setConn(true, "已连接（快照水位 " + snap.watermark + "）");
  });

  src.addEventListener("resumed", (msg) => {
    const info = JSON.parse(msg.data);
    setConn(true, `断线恢复（after=${info.after}，水位 ${info.watermark}）`);
  });

  src.addEventListener("event", (msg) => {
    applyEvent(JSON.parse(msg.data));
  });

  src.onerror = () => {
    // EventSource 会自动重连，但自动重连不带游标、会重发快照边界事件；
    // 为保证“以前次已应用序号恢复”，改为受控的 resume 流程。
    closeStream();
    void resume();
  };
}

// 断线恢复：先经 /resume 校验游标可恢复性，再以 after=lastSeq 建立增量订阅。
async function resume() {
  if (state.resuming || !state.drillId) return;
  state.resuming = true;
  setConn(false, "连接中断，尝试以序号 " + state.lastSeq + " 补齐…");
  try {
    const { ok, body } = await api(
      `/api/drills/${state.drillId}/resume?after=${state.lastSeq}`
    );
    if (!ok) {
      if (body.error === "snapshot_required") {
        showSnapshotRequired(body.message);
      } else {
        showSnapshotRequired("恢复校验失败，必须重新获取快照。");
      }
      return;
    }
    // 先应用补齐区间内的事件，再建立只推更高序号的 SSE。
    for (const ev of body.events) applyEvent(ev);

    const src = new EventSource(
      `/api/drills/${state.drillId}/stream?after=${state.lastSeq}`
    );
    state.source = src;
    setConn(true, "已恢复（after=" + state.lastSeq + "）");

    src.addEventListener("resumed", (msg) => {
      const info = JSON.parse(msg.data);
      setConn(true, `断线恢复（after=${info.after}，水位 ${info.watermark}）`);
    });
    src.addEventListener("event", (msg) => applyEvent(JSON.parse(msg.data)));
    src.onerror = () => {
      closeStream();
      state.resuming = false;
      void resume();
    };
  } finally {
    state.resuming = false;
  }
}

async function selectDrill(drillId) {
  state.drillId = drillId;
  const { body } = await api(`/api/drills/${drillId}`);
  applySnapshot(body);
  subscribeFresh();
}

async function loadDrills(selectId) {
  const { body } = await api("/api/drills");
  const sel = $("drill-select");
  sel.innerHTML = "";
  for (const d of body.drills || []) {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = `${d.name} (${d.id})`;
    sel.appendChild(opt);
  }
  if (selectId) sel.value = selectId;
  return body.drills || [];
}

async function init() {
  const drills = await loadDrills();
  if (drills.length > 0) {
    await selectDrill(drills[0].id);
  } else {
    setConn(false, "请先新建演练");
  }

  $("drill-select").addEventListener("change", (e) => selectDrill(e.target.value));
  $("re-snapshot-btn").addEventListener("click", () => {
    hideSnapshotRequired();
    void selectDrill(state.drillId);
  });

  $("new-drill-btn").addEventListener("click", async () => {
    const name = $("new-drill-name").value.trim();
    const { body } = await api("/api/drills", {
      method: "POST",
      body: JSON.stringify(name ? { name } : {}),
    });
    await loadDrills(body.id);
    await selectDrill(body.id);
  });

  $("event-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const msg = $("form-msg");
    const payload = {
      event_id: $("event-id").value.trim(),
      channel: $("event-channel").value.trim(),
      state: $("event-state").value,
    };
    const { ok, body } = await api(
      `/api/drills/${state.drillId}/events`,
      { method: "POST", body: JSON.stringify(payload) }
    );
    if (ok) {
      msg.className = "form-msg ok";
      msg.textContent = `已提交，全局序号 ${body.seq}`;
    } else {
      msg.className = "form-msg err";
      msg.textContent = body.message || body.error || "提交失败";
    }
  });
}

init();
