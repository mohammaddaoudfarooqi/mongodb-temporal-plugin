import { useState, type FormEvent } from "react";
import { api } from "../api";

const examples = [
  { subject: "Refund for duplicate charge", body: "My card was charged twice for the invoice, please refund" },
  { subject: "Production API outage", body: "All requests failing with 503, service unavailable" },
  { subject: "How do I paginate list results?", body: "The list call only returns the first page" },
];

export default function NewTicket() {
  const [customer, setCustomer] = useState("acme");
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(undefined);
    try {
      const { id } = await api.open(customer, subject, body);
      window.location.hash = `#/tickets/${encodeURIComponent(id)}`;
    } catch (err) {
      setError((err as Error).message);
      setBusy(false);
    }
  };
  return (
    <section>
      <h1>New ticket</h1>
      <p className="lede">
        Opening a ticket starts a workflow. It triages the ticket, finds suggested fixes in the knowledge base, and
        escalates it if the SLA timer fires before it is resolved.
      </p>
      <form className="card form" onSubmit={submit}>
        <label>Customer<input value={customer} onChange={(e) => setCustomer(e.target.value)} required maxLength={40} /></label>
        <label>Subject<input value={subject} onChange={(e) => setSubject(e.target.value)} required maxLength={200} /></label>
        <label>Description<textarea value={body} onChange={(e) => setBody(e.target.value)} rows={5} maxLength={5000} /></label>
        <div className="row">
          <button disabled={busy}>Open ticket</button>
          <span className="hint">Try:</span>
          {examples.map((x) => (
            <button type="button" className="link" key={x.subject} onClick={() => (setSubject(x.subject), setBody(x.body))}>
              {x.subject}
            </button>
          ))}
        </div>
        {error && <p className="error">{error}</p>}
      </form>
    </section>
  );
}
