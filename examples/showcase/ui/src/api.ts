// Typed client for the showcase UI's JSON API (web/server.go).

export type Config = { namespace: string; embedding: "auto" | "voyage" | "none"; temporal_ui: string };

export type Hit = { id: string; subject: string; resolution?: string; score: number; text_rank?: number; vector_rank?: number };

export type TicketState = {
  ticket: { id: string; customer_id: string; subject: string; body: string };
  category: string;
  priority: number;
  status: string;
  agent?: string;
  comments?: string[];
  suggestions?: Hit[];
  escalated: boolean;
  resolution?: string;
};

export type Summary = {
  id: string;
  workflow: string;
  status: string;
  started: string;
  closed?: string;
  customer_id?: string;
  category?: string;
  priority?: number;
  ticket_status?: string;
  summary?: string;
};

export type SearchResults = { mode: Config["embedding"]; text: Hit[]; vector: Hit[]; hybrid: Hit[] };

export type Report = { at: string; by_status: Record<string, number>; escalated: string[] | null };

export class ApiError extends Error {}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (res.status === 204) return undefined as T;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError((data as { error?: string }).error ?? `request failed (${res.status})`);
  return data as T;
}

const q = encodeURIComponent;

export const api = {
  config: () => call<Config>("GET", "/api/config"),
  tickets: () => call<Summary[]>("GET", "/api/tickets"),
  workflows: (query: string) => call<Summary[]>("GET", `/api/workflows?query=${q(query)}`),
  ticket: (id: string) => call<TicketState>("GET", `/api/tickets/${q(id)}`),
  open: (customer: string, subject: string, body: string) =>
    call<{ id: string }>("POST", "/api/tickets", { customer, subject, body }),
  assign: (id: string, agent: string) => call<void>("POST", `/api/tickets/${q(id)}/assign`, { agent }),
  resolve: (id: string, resolution: string) => call<void>("POST", `/api/tickets/${q(id)}/resolve`, { resolution }),
  comment: (id: string, text: string) => call<{ comments: number }>("POST", `/api/tickets/${q(id)}/comment`, { text }),
  search: (text: string) => call<SearchResults>("GET", `/api/search?q=${q(text)}`),
  report: () => call<Report>("POST", "/api/report"),
};
