// Pure helpers, unit-tested in lib.test.ts.

/** A link to a workflow in the Temporal UI. */
export function workflowLink(base: string, namespace: string, id: string): string {
  return `${base.replace(/\/+$/, "")}/namespaces/${encodeURIComponent(namespace)}/workflows/${encodeURIComponent(id)}`;
}

/** "12s ago", "5m ago", "3h ago", "2d ago". */
export function ago(iso: string, now: number = Date.now()): string {
  const s = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export type Tone = "ok" | "warn" | "bad" | "muted";

/** Colour for a ticket or workflow status. */
export function tone(status: string | undefined): Tone {
  switch ((status ?? "").toLowerCase()) {
    case "resolved":
    case "completed":
      return "ok";
    case "escalated":
    case "failed":
    case "terminated":
    case "timedout":
      return "bad";
    case "open":
    case "assigned":
    case "running":
      return "warn";
    default:
      return "muted";
  }
}

/** The app's routes, from the URL hash. */
export type Route =
  | { page: "tickets" }
  | { page: "ticket"; id: string }
  | { page: "new" }
  | { page: "search" }
  | { page: "operator" };

export function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  switch (parts[0]) {
    case "tickets":
      return parts[1] ? { page: "ticket", id: decodeURIComponent(parts[1]) } : { page: "tickets" };
    case "new":
      return { page: "new" };
    case "search":
      return { page: "search" };
    case "operator":
      return { page: "operator" };
    default:
      return { page: "tickets" };
  }
}

/** Visibility queries the operator view offers as presets. */
export const presets: { label: string; query: string; explains: string }[] = [
  { label: "Billing tickets", query: "Category = 'billing'", explains: "keyword attribute" },
  { label: "High priority", query: "Priority >= 3", explains: "integer range" },
  { label: "Escalated", query: "TicketStatus = 'escalated'", explains: "set by the SLA timer" },
  { label: "Mentions “unavailable”", query: "Summary = 'unavailable'", explains: "full-text on a Text attribute" },
  { label: "Running tickets", query: "WorkflowType = 'TicketWorkflow' AND ExecutionStatus = 'Running'", explains: "system attributes" },
  { label: "SLA reports", query: "WorkflowType = 'SLAReportWorkflow' ORDER BY StartTime DESC", explains: "the scheduled workflow" },
];
