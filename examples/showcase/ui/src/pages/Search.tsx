import { useEffect, useState, type FormEvent } from "react";
import { api, type Hit, type SearchResults } from "../api";

function Column({ title, how, hits, off }: { title: string; how: string; hits: Hit[]; off?: string }) {
  return (
    <div className="card">
      <h2>{title}</h2>
      <p className="hint">{how}</p>
      {off ? <p className="empty">{off}</p> : hits.length === 0 ? <p className="empty">No matches.</p> : (
        <ol className="hits">
          {hits.map((h) => (
            <li key={h.id}>
              <strong>{h.subject}</strong>
              {h.resolution && <div className="resolution">{h.resolution}</div>}
              {!!(h.text_rank || h.vector_rank) && (
                <div className="ranks">
                  {h.text_rank ? <span className="badge muted">text #{h.text_rank}</span> : null}
                  {h.vector_rank ? <span className="badge muted">vector #{h.vector_rank}</span> : null}
                </div>
              )}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

export default function Search() {
  const [text, setText] = useState("charged twice refund");
  const [res, setRes] = useState<SearchResults>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const submit = async (e?: FormEvent) => {
    e?.preventDefault();
    setBusy(true);
    setError(undefined);
    try {
      setRes(await api.search(text));
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    void submit();
    // Run the example query once, so the page opens with results.
  }, []);
  const vectorOff =
    res?.mode === "none"
      ? "Vector search is off. Set VOYAGE_API_KEY (an Atlas model API key), or SHOWCASE_EMBEDDING=auto on Atlas, to turn it on."
      : undefined;
  return (
    <section>
      <h1>Knowledge base</h1>
      <p className="lede">
        Resolved tickets, searched three ways on the same MongoDB collection: Atlas Search full-text, Atlas Vector Search
        on Voyage AI embeddings, and both fused with reciprocal rank fusion.
      </p>
      <form className="searchbar" onSubmit={submit}>
        <input value={text} onChange={(e) => setText(e.target.value)} aria-label="Search" maxLength={500} />
        <button disabled={busy || !text.trim()}>Search</button>
      </form>
      {error && <p className="error">{error}</p>}
      {res && (
        <div className="grid3">
          <Column title="Full-text" how="$search on the tickets_text index" hits={res.text} />
          <Column title="Vector" how="$vectorSearch on the tickets_vector index" hits={res.vector} off={vectorOff} />
          <Column title="Hybrid" how="Both lists, fused (RRF, k = 60)" hits={res.hybrid} />
        </div>
      )}
    </section>
  );
}
