import { useEffect, useRef, useState, type FormEvent } from "react";
import { api, type Config, type Report, type Summary } from "../api";
import { ago, presets, workflowLink } from "../lib";
import { Badge } from "./Badge";

export default function Operator({ config }: { config: Config }) {
  const [query, setQuery] = useState(presets[0].query);
  const [rows, setRows] = useState<Summary[]>();
  const [ran, setRan] = useState<string>();
  const [error, setError] = useState<string>();
  const [report, setReport] = useState<Report>();
  const [reportBusy, setReportBusy] = useState(false);
  const latest = useRef(0);
  const run = async (q: string, e?: FormEvent) => {
    e?.preventDefault();
    setQuery(q);
    setError(undefined);
    const n = ++latest.current;
    try {
      const result = await api.workflows(q);
      if (n !== latest.current) return; // a newer query has started
      setRows(result);
      setRan(q);
    } catch (err) {
      if (n === latest.current) setError((err as Error).message);
    }
  };
  useEffect(() => {
    void run(presets[0].query);
  }, []);
  const buildReport = async () => {
    setReportBusy(true);
    try {
      setReport(await api.report());
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setReportBusy(false);
    }
  };
  return (
    <section>
      <h1>Operator</h1>
      <p className="lede">
        Temporal visibility queries over every workflow in the <code>{config.namespace}</code> namespace. The MongoDB
        plugin translates each one into an Atlas Search query; Text attributes such as <code>Summary</code> get full-text
        matching.
      </p>
      <div className="presets">
        {presets.map((p) => (
          <button key={p.query} className={`chip ${ran === p.query ? "on" : ""}`} onClick={() => run(p.query)} title={p.query}>
            {p.label}
            <small>{p.explains}</small>
          </button>
        ))}
      </div>
      <form className="searchbar" onSubmit={(e) => run(query, e)}>
        <input className="mono" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Visibility query" maxLength={1000} />
        <button>Run query</button>
      </form>
      {error && <p className="error">{error}</p>}
      {rows && (
        <div className="card">
          <h2>{rows.length} workflow{rows.length === 1 ? "" : "s"} <code className="small">{ran}</code></h2>
          {rows.length > 0 && (
            <table>
              <thead><tr><th>Workflow</th><th>Type</th><th>Attributes</th><th>Status</th><th>Started</th></tr></thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <a href={workflowLink(config.temporal_ui, config.namespace, r.id)} target="_blank" rel="noreferrer">{r.id}</a>
                    </td>
                    <td>{r.workflow}</td>
                    <td className="attrs">
                      {[r.category && `Category=${r.category}`, r.priority && `Priority=${r.priority}`, r.ticket_status && `TicketStatus=${r.ticket_status}`]
                        .filter(Boolean).join(" · ")}
                    </td>
                    <td><Badge value={r.status} /></td>
                    <td>{ago(r.started)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
      <div className="card">
        <div className="heading">
          <h2>SLA report</h2>
          <button onClick={buildReport} disabled={reportBusy}>{reportBusy ? "Building…" : "Build now"}</button>
        </div>
        <p className="hint">
          A workflow that counts tickets by status with visibility queries. A schedule also runs it every 5 minutes.
        </p>
        {report && (
          <>
            <div className="stats">
              {Object.entries(report.by_status).map(([k, v]) => (
                <div key={k} className="stat"><span>{v}</span><Badge value={k} /></div>
              ))}
            </div>
            {(report.escalated ?? []).length > 0 && <p className="meta">Escalated: {(report.escalated ?? []).join(", ")}</p>}
          </>
        )}
      </div>
    </section>
  );
}
