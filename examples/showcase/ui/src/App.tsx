import { useEffect, useState } from "react";
import { api, type Config } from "./api";
import { useRoute } from "./hooks";
import Tickets from "./pages/Tickets";
import TicketDetail from "./pages/TicketDetail";
import NewTicket from "./pages/NewTicket";
import Search from "./pages/Search";
import Operator from "./pages/Operator";

const tabs = [
  { href: "#/tickets", label: "Tickets", pages: ["tickets", "ticket"] },
  { href: "#/new", label: "New ticket", pages: ["new"] },
  { href: "#/search", label: "Knowledge base", pages: ["search"] },
  { href: "#/operator", label: "Operator", pages: ["operator"] },
];

export default function App() {
  const route = useRoute();
  const [config, setConfig] = useState<Config>();
  const [error, setError] = useState<string>();
  useEffect(() => {
    api.config().then(setConfig, (e: Error) => setError(e.message));
  }, []);

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="leaf" aria-hidden>●</span>
          <div>
            <strong>Support desk</strong>
            <small>Temporal on MongoDB</small>
          </div>
        </div>
        <nav>
          {tabs.map((t) => (
            <a key={t.href} href={t.href} className={t.pages.includes(route.page) ? "active" : ""}>
              {t.label}
            </a>
          ))}
        </nav>
        {config && (
          <a className="external" href={config.temporal_ui} target="_blank" rel="noreferrer">
            Temporal UI ↗
          </a>
        )}
      </header>
      <main>
        {error && <p className="error">Cannot reach the showcase API: {error}</p>}
        {config && route.page === "tickets" && <Tickets />}
        {config && route.page === "ticket" && <TicketDetail id={route.id} config={config} />}
        {config && route.page === "new" && <NewTicket />}
        {config && route.page === "search" && <Search />}
        {config && route.page === "operator" && <Operator config={config} />}
      </main>
      <footer>
        Tickets are durable Temporal workflows persisted in MongoDB. Workflow search runs on Atlas Search, and the
        knowledge base's full-text and vector search run on the same cluster.
      </footer>
    </div>
  );
}
