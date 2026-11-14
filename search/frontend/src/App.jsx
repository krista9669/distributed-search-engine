import { useEffect, useState } from "react";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";

function App() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState("");
  const [health, setHealth] = useState(null);
  const [searchTime, setSearchTime] = useState(0);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const response = await fetch(`${API_URL}/health`);
        if (!response.ok) throw new Error("Backend unavailable");
        setHealth(await response.json());
      } catch {
        setHealth(null);
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  const handleSearch = async (event) => {
    event.preventDefault();

    if (!query.trim()) return;

    setLoading(true);
    setError("");
    setSearched(true);
    setResults([]);

    try {
      const params = new URLSearchParams({
        q: query.trim(),
        top_k: "10",
      });

      const response = await fetch(`${API_URL}/search?${params}`);

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Search failed");
      }

      const data = await response.json();
      setResults(data.results);
      setSearchTime(data.search_time_ms);
    } catch (err) {
      setError(err.message || "Unable to connect to backend");
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-icon">DS</span>
          <span>Distributed Search</span>
        </div>

        <div className="connection">
          <span className={`status-dot ${health ? "online" : ""}`} />
          {health ? "Backend connected" : "Backend disconnected"}
        </div>
      </header>

      <section className="hero">
        <div className="eyebrow">DISTRIBUTED INFORMATION RETRIEVAL</div>

        <h1>
          Find information.
          <br />
          <span>Discover connections.</span>
        </h1>

        <p className="subtitle">
          Search across documents using BM25 relevance and PageRank scoring.
        </p>

        <form className="search-form" onSubmit={handleSearch}>
          <span className="search-icon">⌕</span>

          <input
            type="text"
            placeholder="Search for anything..."
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />

          <button type="submit" disabled={loading || !query.trim()}>
            {loading ? "Searching..." : "Search"}
          </button>
        </form>

        <div className="search-meta">
          <span>BM25 + PageRank</span>
          <span>70% relevance · 30% authority</span>
        </div>
      </section>

      <section className="results-section">
        {loading && <p className="message">Searching documents...</p>}

        {error && <p className="error-message">{error}</p>}

        {!loading && searched && !error && (
          <>
            <div className="results-heading">
              <h2>Search Results</h2>
              <span>
                {results.length} results · {searchTime.toFixed(2)} ms
              </span>
            </div>

            {results.length === 0 ? (
              <p className="message">No matching documents found.</p>
            ) : (
              <div className="results-list">
                {results.map((result, index) => (
                  <article className="result-card" key={result.doc_id}>
                    <div className="result-top">
                      <span className="rank">#{index + 1}</span>
                      <span className="score">
                        Score {result.final_score.toFixed(4)}
                      </span>
                    </div>

                    <h3>{result.title}</h3>

                    <a
                      href={result.url}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {result.url}
                    </a>

                    <p className="snippet">{result.snippet}</p>

                    <div className="score-details">
                      <span>BM25: {result.bm25_score.toFixed(4)}</span>
                      <span>
                        PageRank: {result.pagerank_score.toFixed(4)}
                      </span>
                      <span>Final: {result.final_score.toFixed(4)}</span>
                    </div>
                  </article>
                ))}
              </div>
            )}
          </>
        )}

        {!searched && !loading && (
          <div className="empty-state">
            <div className="empty-icon">⌕</div>
            <h2>Start exploring</h2>
            <p>Enter a query above to search the document collection.</p>
          </div>
        )}
      </section>

      <footer>
        Distributed Search Engine · Node 4 · FastAPI + React
      </footer>
    </main>
  );
}

export default App;
