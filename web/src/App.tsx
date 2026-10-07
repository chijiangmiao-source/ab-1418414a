import { FormEvent, useEffect, useRef, useState } from "react";
import {
  ApiError,
  api,
  type ChannelStateRow,
  type Drill,
  type EventRow,
  type Snapshot,
} from "./api";
import { DrillStream, type StreamStatus } from "./stream";

const STATE_OPTIONS = [
  { value: "normal", label: "正常" },
  { value: "warning", label: "预警" },
  { value: "alarm", label: "告警" },
  { value: "bypass", label: "旁路" },
];

const STATE_LABELS: Record<string, string> = Object.fromEntries(
  STATE_OPTIONS.map((o) => [o.value, o.label]),
);

const STATUS_TEXT: Record<StreamStatus, string> = {
  connecting: "连接中…",
  live: "实时监看中",
  reconnecting: "连接中断，重连中…",
  resync: "游标过期，正在重新获取快照…",
};

const MAX_RECENT_EVENTS = 100;

function stateLabel(value: string): string {
  return STATE_LABELS[value] ?? value;
}

function shortId(id: string): string {
  return id.length <= 12 ? id : `${id.slice(0, 8)}…`;
}

function fmtTime(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString();
}

interface Feedback {
  kind: "ok" | "dup" | "err";
  text: string;
}

export default function App() {
  const [drills, setDrills] = useState<Drill[]>([]);
  const [drillId, setDrillId] = useState<string | null>(null);
  const [newDrillName, setNewDrillName] = useState("");

  const [channels, setChannels] = useState<Record<string, ChannelStateRow>>({});
  const [events, setEvents] = useState<EventRow[]>([]);
  const [watermark, setWatermark] = useState(0);
  const [appliedSeq, setAppliedSeq] = useState(0);
  const [minAvailable, setMinAvailable] = useState<number | null>(null);
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const [deduped, setDeduped] = useState(0);
  const [resyncInfo, setResyncInfo] = useState<string | null>(null);

  const [eventId, setEventId] = useState<string>(() => crypto.randomUUID());
  const [channel, setChannel] = useState("BL-01");
  const [state, setState] = useState("normal");
  const [feedback, setFeedback] = useState<Feedback | null>(null);

  const appliedSeqRef = useRef(0);
  const streamRef = useRef<DrillStream | null>(null);

  useEffect(() => {
    void refreshDrills();
  }, []);

  async function refreshDrills() {
    try {
      const list = await api.listDrills();
      setDrills(list);
      setDrillId((current) => current ?? list[0]?.id ?? null);
    } catch {
      /* server not reachable yet; the stream status shows it */
    }
  }

  useEffect(() => {
    if (!drillId) return;

    appliedSeqRef.current = 0;
    setAppliedSeq(0);
    setWatermark(0);
    setMinAvailable(null);
    setChannels({});
    setEvents([]);
    setDeduped(0);
    setResyncInfo(null);
    setFeedback(null);

    const applySnapshot = (snap: Snapshot) => {
      appliedSeqRef.current = snap.watermark;
      setAppliedSeq(snap.watermark);
      setWatermark(snap.watermark);
      setMinAvailable(snap.min_available_seq);
      setChannels(
        Object.fromEntries(snap.channels.map((row) => [row.channel, row])),
      );
      setEvents(snap.recent_events.slice(0, MAX_RECENT_EVENTS));
    };

    const stream = new DrillStream(
      drillId,
      {
        onSnapshot: applySnapshot,
        onEvent: (event) => {
          if (event.seq <= appliedSeqRef.current) {
            // Boundary retransmission or overlapping reconnect: drop it.
            setDeduped((n) => n + 1);
            return;
          }
          appliedSeqRef.current = event.seq;
          setAppliedSeq(event.seq);
          setWatermark((w) => Math.max(w, event.seq));
          setChannels((prev) => ({
            ...prev,
            [event.channel]: {
              channel: event.channel,
              state: event.state,
              seq: event.seq,
              updated_at: event.created_at,
            },
          }));
          setEvents((prev) => [event, ...prev].slice(0, MAX_RECENT_EVENTS));
        },
        onStatus: setStatus,
        onResync: (reason) => {
          setResyncInfo(
            `服务要求重新同步（${reason}）：游标已超出可恢复范围，已自动重新获取快照。`,
          );
        },
      },
      () => appliedSeqRef.current,
    );
    streamRef.current = stream;
    stream.start();
    return () => {
      stream.stop();
      if (streamRef.current === stream) streamRef.current = null;
    };
  }, [drillId]);

  async function onCreateDrill(e: FormEvent) {
    e.preventDefault();
    const name = newDrillName.trim();
    if (!name) return;
    try {
      const drill = await api.createDrill(name);
      setNewDrillName("");
      await refreshDrills();
      setDrillId(drill.id);
    } catch {
      setFeedback({ kind: "err", text: "创建演练失败" });
    }
  }

  async function onSubmitEvent(e: FormEvent) {
    e.preventDefault();
    if (!drillId) return;
    try {
      const result = await api.submitEvent(drillId, {
        event_id: eventId.trim(),
        channel: channel.trim(),
        state,
      });
      setFeedback(
        result.deduplicated
          ? { kind: "dup", text: `相同事件标识重传：已去重，返回原序号 #${result.seq}` }
          : { kind: "ok", text: `已受理，全局序号 #${result.seq}` },
      );
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        const detail = (err.detail as { detail?: { existing?: EventRow } })?.detail;
        setFeedback({
          kind: "err",
          text: `事件标识冲突：该标识已用于通道 ${detail?.existing?.channel ?? "?"}（${stateLabel(
            detail?.existing?.state ?? "?",
          )}），已拒绝且未改写投影`,
        });
      } else {
        setFeedback({ kind: "err", text: "提交失败，请检查输入" });
      }
    }
  }

  const channelRows = Object.values(channels).sort((a, b) =>
    a.channel.localeCompare(b.channel),
  );
  const knownChannels = channelRows.map((c) => c.channel);

  return (
    <div className="app">
      <header className="topbar">
        <h1>束线联锁监看台</h1>
        <span className={`status-pill status-${status}`}>{STATUS_TEXT[status]}</span>
      </header>

      <div className="layout">
        <aside>
          <section className="card">
            <h2>演练</h2>
            <label className="field">
              <span>当前演练</span>
              <select
                value={drillId ?? ""}
                onChange={(e) => setDrillId(e.target.value || null)}
              >
                {drills.length === 0 && <option value="">（暂无演练）</option>}
                {drills.map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.name}
                  </option>
                ))}
              </select>
            </label>
            <form onSubmit={onCreateDrill} className="inline-form">
              <input
                value={newDrillName}
                onChange={(e) => setNewDrillName(e.target.value)}
                placeholder="新演练名称"
                maxLength={120}
              />
              <button type="submit">创建演练</button>
            </form>
          </section>

          <section className="card">
            <h2>提交通道状态变更</h2>
            <form onSubmit={onSubmitEvent} className="stack-form">
              <label className="field">
                <span>事件标识（稳定 ID）</span>
                <div className="id-row">
                  <input
                    value={eventId}
                    onChange={(e) => setEventId(e.target.value)}
                    maxLength={128}
                    required
                  />
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => setEventId(crypto.randomUUID())}
                    title="生成新的事件标识"
                  >
                    换一批
                  </button>
                </div>
              </label>
              <label className="field">
                <span>通道</span>
                <input
                  value={channel}
                  onChange={(e) => setChannel(e.target.value)}
                  list="known-channels"
                  maxLength={64}
                  required
                />
                <datalist id="known-channels">
                  {knownChannels.map((c) => (
                    <option key={c} value={c} />
                  ))}
                </datalist>
              </label>
              <label className="field">
                <span>状态</span>
                <select value={state} onChange={(e) => setState(e.target.value)}>
                  {STATE_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
              </label>
              <button type="submit" disabled={!drillId}>
                提交变更
              </button>
            </form>
            {feedback && <p className={`feedback feedback-${feedback.kind}`}>{feedback.text}</p>}
          </section>
        </aside>

        <main>
          {resyncInfo && (
            <div className="banner">
              <span>{resyncInfo}</span>
              <button className="ghost" onClick={() => streamRef.current?.resyncNow()}>
                重新获取快照
              </button>
              <button className="ghost" onClick={() => setResyncInfo(null)}>
                知道了
              </button>
            </div>
          )}

          <div className="stat-row">
            <div className="stat">
              <span>当前水位</span>
              <strong>{watermark}</strong>
            </div>
            <div className="stat">
              <span>已应用序号</span>
              <strong>{appliedSeq}</strong>
            </div>
            <div className="stat">
              <span>边界重发已去重</span>
              <strong>{deduped}</strong>
            </div>
            <div className="stat">
              <span>可恢复起点</span>
              <strong>{minAvailable ?? "—"}</strong>
            </div>
          </div>

          <section className="card">
            <h2>通道状态</h2>
            {channelRows.length === 0 ? (
              <p className="empty">暂无通道数据，请先提交状态变更。</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>通道</th>
                    <th>状态</th>
                    <th>来源序号</th>
                    <th>更新时间</th>
                  </tr>
                </thead>
                <tbody>
                  {channelRows.map((row) => (
                    <tr key={row.channel}>
                      <td className="mono">{row.channel}</td>
                      <td>
                        <span className={`badge state-${row.state}`}>
                          {stateLabel(row.state)}
                        </span>
                      </td>
                      <td className="mono">#{row.seq}</td>
                      <td>{fmtTime(row.updated_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>

          <section className="card">
            <h2>最近事件（按全局序号排列）</h2>
            {events.length === 0 ? (
              <p className="empty">暂无事件。</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>序号</th>
                    <th>通道</th>
                    <th>状态</th>
                    <th>事件标识</th>
                    <th>时间</th>
                  </tr>
                </thead>
                <tbody>
                  {events.map((event) => (
                    <tr key={event.seq}>
                      <td className="mono">#{event.seq}</td>
                      <td className="mono">{event.channel}</td>
                      <td>
                        <span className={`badge state-${event.state}`}>
                          {stateLabel(event.state)}
                        </span>
                      </td>
                      <td className="mono" title={event.event_id}>
                        {shortId(event.event_id)}
                      </td>
                      <td>{fmtTime(event.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </main>
      </div>
    </div>
  );
}
