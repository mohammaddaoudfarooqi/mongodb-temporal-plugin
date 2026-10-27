import { describe, expect, it } from "vitest";
import { ago, parseRoute, presets, tone, workflowLink } from "./lib";

describe("workflowLink", () => {
  it("encodes the namespace and workflow ID and trims the base", () => {
    expect(workflowLink("http://localhost:8080/", "support", "ticket-acme 1")).toBe(
      "http://localhost:8080/namespaces/support/workflows/ticket-acme%201",
    );
  });
});

describe("ago", () => {
  const now = Date.parse("2026-10-27T10:00:00Z");
  it.each([
    ["2026-10-27T09:59:48Z", "12s ago"],
    ["2026-10-27T09:55:00Z", "5m ago"],
    ["2026-10-27T07:00:00Z", "3h ago"],
    ["2026-10-25T10:00:00Z", "2d ago"],
    ["2026-10-27T10:00:05Z", "0s ago"],
  ])("%s is %s", (iso, want) => expect(ago(iso, now)).toBe(want));
});

describe("tone", () => {
  it("maps ticket and workflow statuses", () => {
    expect(tone("resolved")).toBe("ok");
    expect(tone("Completed")).toBe("ok");
    expect(tone("escalated")).toBe("bad");
    expect(tone("Running")).toBe("warn");
    expect(tone("open")).toBe("warn");
    expect(tone("assigned")).toBe("warn");
    expect(tone(undefined)).toBe("muted");
  });
});

describe("parseRoute", () => {
  it.each([
    ["", { page: "tickets" }],
    ["#/tickets", { page: "tickets" }],
    ["#/tickets/ticket-acme-1", { page: "ticket", id: "ticket-acme-1" }],
    ["#/new", { page: "new" }],
    ["#/search", { page: "search" }],
    ["#/operator", { page: "operator" }],
    ["#/nowhere", { page: "tickets" }],
  ])("%s", (hash, want) => expect(parseRoute(hash)).toEqual(want));
});

describe("presets", () => {
  it("are non-empty visibility queries", () => {
    expect(presets.length).toBeGreaterThan(3);
    for (const p of presets) expect(p.query.trim()).not.toBe("");
  });
});
