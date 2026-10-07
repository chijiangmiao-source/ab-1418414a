import { api, type EventRow, type Snapshot } from "./api";

export type StreamStatus = "connecting" | "live" | "reconnecting" | "resync";

export interface StreamHandlers {
  /** Full consistent snapshot at a pinned watermark (initial load or resync). */
  onSnapshot: (snapshot: Snapshot) => void;
  /** One delta event, always with seq greater than everything applied so far. */
  onEvent: (event: EventRow) => void;
  onStatus: (status: StreamStatus) => void;
  /** Server told us the cursor is unrecoverable; a snapshot refetch follows. */
  onResync: (reason: string) => void;
}

/**
 * Maintains the SSE subscription for one drill.
 *
 * First connect uses no cursor: the server pins a watermark and sends the
 * snapshot, then only higher-seq deltas. Reconnects resume with
 * `after=<last applied seq>` so boundary events are never re-delivered.
 * When the server answers with a `resync` frame (log truncated / cursor out
 * of range) the manager fetches a fresh snapshot over REST and resumes from
 * its watermark.
 */
export class DrillStream {
  private es: EventSource | null = null;
  private stopped = false;
  private retries = 0;
  private timer: number | null = null;

  constructor(
    private readonly drillId: string,
    private readonly handlers: StreamHandlers,
    private readonly getAppliedSeq: () => number,
  ) {}

  start(): void {
    this.stopped = false;
    this.open(false);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer !== null) {
      window.clearTimeout(this.timer);
      this.timer = null;
    }
    this.close();
  }

  /** Manual "重新获取快照" button. */
  resyncNow(): void {
    void this.handleResync("manual");
  }

  private close(): void {
    if (this.es) {
      this.es.close();
      this.es = null;
    }
  }

  private open(withCursor: boolean): void {
    this.close();
    const applied = this.getAppliedSeq();
    const url =
      withCursor && applied > 0
        ? `/api/drills/${this.drillId}/stream?after=${applied}`
        : `/api/drills/${this.drillId}/stream`;
    this.handlers.onStatus(withCursor ? "reconnecting" : "connecting");
    const es = new EventSource(url);
    this.es = es;

    es.addEventListener("snapshot", (msg) => {
      this.retries = 0;
      this.handlers.onSnapshot(JSON.parse((msg as MessageEvent).data) as Snapshot);
      this.handlers.onStatus("live");
    });
    es.addEventListener("resume", () => {
      this.retries = 0;
      this.handlers.onStatus("live");
    });
    es.addEventListener("event", (msg) => {
      this.handlers.onEvent(JSON.parse((msg as MessageEvent).data) as EventRow);
    });
    es.addEventListener("resync", (msg) => {
      const info = JSON.parse((msg as MessageEvent).data) as { reason?: string };
      void this.handleResync(info.reason ?? "cursor_expired");
    });
    es.onerror = () => {
      if (this.stopped) return;
      this.close();
      this.handlers.onStatus("reconnecting");
      const delay = Math.min(5000, 500 * 2 ** this.retries++);
      this.timer = window.setTimeout(() => {
        if (!this.stopped) this.open(true);
      }, delay);
    };
  }

  private async handleResync(reason: string): Promise<void> {
    this.close();
    this.handlers.onResync(reason);
    this.handlers.onStatus("resync");
    try {
      const snapshot = await api.snapshot(this.drillId);
      this.handlers.onSnapshot(snapshot);
    } catch {
      // Snapshot fetch failed; the reconnect below will retry with cursor.
    }
    if (!this.stopped) {
      this.open(true);
    }
  }
}
