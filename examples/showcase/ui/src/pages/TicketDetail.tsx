import { useState, type FormEvent } from "react";
import { api, type Config } from "../api";
import { usePoll } from "../hooks";
import { workflowLink } from "../lib";
import { Badge } from "./Badge";

function Action({ label, placeholder, run }: { label: string; placeholder: string; run: (v: string) => Promise<unknown> }) {
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string }>();
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setMsg(undefined);
    try {
      await run(value);
      setValue("");
      setMsg({ ok: true, text: "Sent" });
    } catch (err) {
      setMsg({ ok: false, text: (err as Error).message });
    } finally {
      setBusy(false);
    }
  };
  return (
    <form className="action" onSubmit={submit}>
      <input value={value} onChange={(e) => setValue(e.target.value)} placeholder={placeholder} aria-label={label} />
      <button disabled={busy}>{label}</button>
      {msg && <span className={msg.ok ? "ok-text" : "error-text"}>{msg.text}</span>}
    </form>
  );
}

export default function TicketDetail({ id, config }: { id: string; config: Config }) {
  const { data: st, error, reload } = usePoll(() => api.ticket(id), 2000, [id]);
  const after = (p: Promise<unknown>) => p.then(reload);
  return (
    <section>
      <a href="#/tickets" className="back">← Tickets</a>
      {error && !st && <p className="error">{error}</p>}
      {error && st && <p className="error">Live updates are failing ({error}); the state below may be out of date.</p>}
      {st && (
        <>
          <div className="heading">
            <h1>{st.ticket.subject}</h1>
            <Badge value={st.status} />
            {st.escalated && <Badge value="escalated" />}
          </div>
          <p className="meta">
            <span className="id">{id}</span> · customer <strong>{st.ticket.customer_id}</strong> · {st.category} · P
            {st.priority}
            {st.agent && <> · assigned to <strong>{st.agent}</strong></>}
            {" · "}
            <a href={workflowLink(config.temporal_ui, config.namespace, id)} target="_blank" rel="noreferrer">
              workflow history ↗
            </a>
          </p>
          <p className="body">{st.ticket.body}</p>

          <div className="grid2">
            <div className="card">
              <h2>Actions</h2>
              {st.status === "resolved" ? (
                <>
                  <p className="hint">The workflow has completed; its state is still readable with the query.</p>
                  <p className="resolution">Resolved: {st.resolution}</p>
                </>
              ) : (
                <>
                  <p className="hint">Assign and resolve are signals; comment is an update the workflow validates.</p>
                  <Action label="Assign" placeholder="Agent name" run={(v) => after(api.assign(id, v))} />
                  <Action label="Comment" placeholder="Comment" run={(v) => after(api.comment(id, v))} />
                  <Action label="Resolve" placeholder="Resolution" run={(v) => after(api.resolve(id, v))} />
                </>
              )}
            </div>
            <div className="card">
              <h2>Suggested fixes</h2>
              <p className="hint">Hybrid search over resolved tickets, run by the workflow when it triaged this one.</p>
              {(st.suggestions ?? []).length === 0 && <p className="empty">None yet.</p>}
              <ol className="hits">
                {(st.suggestions ?? []).map((h) => (
                  <li key={h.id}>
                    <strong>{h.subject}</strong>
                    {h.resolution && <div className="resolution">{h.resolution}</div>}
                  </li>
                ))}
              </ol>
            </div>
          </div>

          <div className="card">
            <h2>Comments</h2>
            {(st.comments ?? []).length === 0 && <p className="empty">No comments.</p>}
            <ul className="comments">{(st.comments ?? []).map((c, i) => <li key={i}>{c}</li>)}</ul>
          </div>
          <p className="hint">State is read live from the running workflow with a query, every two seconds.</p>
        </>
      )}
    </section>
  );
}
