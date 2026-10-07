export interface Drill {
  id: string;
  name: string;
  created_at: string;
}

export interface ChannelStateRow {
  channel: string;
  state: string;
  seq: number;
  updated_at: string;
}

export interface EventRow {
  seq: number;
  event_id: string;
  drill_id: string;
  channel: string;
  state: string;
  created_at: string;
}

export interface Snapshot {
  watermark: number;
  min_available_seq: number | null;
  channels: ChannelStateRow[];
  recent_events: EventRow[];
}

export interface SubmitResult extends EventRow {
  deduplicated: boolean;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public detail: unknown,
  ) {
    super(`HTTP ${status}`);
  }
}

async function req<T>(method: string, url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let detail: unknown = null;
    try {
      detail = await res.json();
    } catch {
      /* keep null */
    }
    throw new ApiError(res.status, detail);
  }
  return (await res.json()) as T;
}

export const api = {
  listDrills: () => req<Drill[]>("GET", "/api/drills"),
  createDrill: (name: string) => req<Drill>("POST", "/api/drills", { name }),
  snapshot: (drillId: string) => req<Snapshot>("GET", `/api/drills/${drillId}/snapshot`),
  submitEvent: (drillId: string, body: { event_id: string; channel: string; state: string }) =>
    req<SubmitResult>("POST", `/api/drills/${drillId}/events`, body),
};
