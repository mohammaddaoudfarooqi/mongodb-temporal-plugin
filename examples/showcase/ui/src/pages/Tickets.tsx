import { api } from "../api";
import { usePoll } from "../hooks";
import { ago } from "../lib";
import { Badge } from "./Badge";

export default function Tickets() {
  const { data, error } = usePoll(api.tickets, 3000);
  return (
    <section>
      <div className="heading">
        <h1>Tickets</h1>
        <a className="button" href="#/new">New ticket</a>
      </div>
      <p className="lede">
        Every ticket is a workflow. This list is a Temporal visibility query, answered by the MongoDB plugin from Atlas
        Search; it refreshes every few seconds.
      </p>
      {error && <p className="error">{error}</p>}
      {data && data.length === 0 && <p className="empty">No tickets yet. Open one, or run the tour.</p>}
      {data && data.length > 0 && (
        <table>
          <thead>
            <tr><th>Ticket</th><th>Customer</th><th>Category</th><th>Priority</th><th>Status</th><th>Opened</th></tr>
          </thead>
          <tbody>
            {data.map((t) => (
              <tr key={t.id} onClick={() => (window.location.hash = `#/tickets/${encodeURIComponent(t.id)}`)}>
                <td className="summary"><a href={`#/tickets/${encodeURIComponent(t.id)}`} title={t.summary}>{t.summary || t.id}</a><div className="id">{t.id}</div></td>
                <td>{t.customer_id}</td>
                <td>{t.category}</td>
                <td>{t.priority ? `P${t.priority}` : ""}</td>
                <td><Badge value={t.ticket_status || t.status} /></td>
                <td title={t.started}>{ago(t.started)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
