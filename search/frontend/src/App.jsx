
import { useEffect, useState } from "react";
import "./App.css";

const API_URL =
  import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";

function App() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState("");
  const [backendOnline, setBackendOnline] = useState(false);
  const [documentCount, setDocumentCount] = useState(0);

  const [theme, setTheme] = useState(
    () => localStorage.getItem("search-theme") || "light"
  );

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("search-theme", theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme((current) => current === "light" ? "dark" : "light");
  };

  useEffect(() => {
    const checkBackend = async () => {
      try {
        const response = await fetch(`${API_URL}/health`);
        const data = await response.json();

        setBackendOnline(response.ok);
        setDocumentCount(data.documents_loaded ?? 0);
      } catch {
        setBackendOnline(false);
      }
    };

    checkBackend();
  }, []);

  const performSearch = async (event) => {
    event?.preventDefault();

    if (!query.trim()) return;

    setLoading(true);
    setError("");
    setSearched(true);
    setResults([]);

    try {
      const response = await fetch(
        `${API_URL}/search?q=${encodeURIComponent(query.trim())}&top_k=10`
      );

      if (!response.ok) {
        throw new Error("Search request failed. Please try again.");
      }

      const data = await response.json();
      setResults(data.results ?? []);
    } catch (err) {
      setError(err.message || "Unable to connect to the search service.");
    } finally {
      setLoading(false);
    }
  };

  const clearSearch = () => {
    setQuery("");
    setResults([]);
    setError("");
    setSearched(false);
  };

  const formatScore = (score) =>
    typeof score === "number" ? score.toFixed(4) : "N/A";

  return (
    <div className={`app ${theme}`}>
      <header className="header">
        <div className="header-content">
          <button className="brand" onClick={clearSearch}>
            <span className="brand-icon">S</span>
            <span>Distributed Search Engine</span>
          </button>

          <div className="header-actions">
            <button
              className="theme-toggle"
              onClick={toggleTheme}
              aria-label={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
              title="Toggle theme"
            >
              {theme === "light" ? "☾ Dark" : "☀ Light"}
            </button>

            <div className="connection-status">
              <span
                className={`status-dot ${
                  backendOnline ? "online" : "offline"
                }`}
              />
              {backendOnline ? "Backend connected" : "Backend disconnected"}
            </div>
          </div>
        </div>
      </header>

      <main className="main-content">
        <section className="hero">
          <div className="eyebrow">
            DISTRIBUTED INFORMATION RETRIEVAL
          </div>

          <h1>
            Find information <span>faster.</span>
          </h1>

          <p className="subtitle">
            Search across documents using distributed indexing,
            BM25 retrieval and PageRank-based ranking.
          </p>

          <form className="search-form" onSubmit={performSearch}>
            <span className="search-icon">⌕</span>

            <input
              type="text"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Enter keywords to search documents..."
              aria-label="Search documents"
            />

            {query && (
              <button
                type="button"
                className="clear-button"
                onClick={clearSearch}
                aria-label="Clear search"
              >
                ×
              </button>
            )}

            <button
              type="submit"
              className="search-button"
              disabled={loading || !query.trim()}
            >
              {loading ? "Searching..." : "Search"}
            </button>
          </form>

          <div className="search-info">
            <span>BM25</span>
            <span className="info-separator">•</span>
            <span>PageRank</span>
            <span className="info-separator">•</span>
            <span>Apache Spark</span>
            <span className="info-separator">•</span>
            <span>Apache Hadoop</span>
          </div>
        </section>

        <section className="results-section">
          {searched && (
            <div className="results-header">
              <div>
                <h2>Search Results</h2>
                <p>
                  {loading
                    ? "Searching documents..."
                    : `${results.length} result(s) found`}
                </p>
              </div>
            </div>
          )}

          {loading && (
            <div className="message-card">
              <div className="loader" />
              <p>Searching through the document collection...</p>
            </div>
          )}

          {error && !loading && (
            <div className="message-card error-message">
              <h3>Something went wrong</h3>
              <p>{error}</p>
              <button onClick={performSearch}>Try again</button>
            </div>
          )}

          {!loading && !error && searched && results.length === 0 && (
            <div className="message-card">
              <h3>No results found</h3>
              <p>Try different keywords or a broader search query.</p>
            </div>
          )}

          {!loading && !error && results.length > 0 && (
            <div className="results-list">
              {results.map((result, index) => (
                <article
                  className="result-card"
                  key={result.doc_id ?? index}
                >
                  <div className="result-topline">
                    <span className="result-number">
                      {String(index + 1).padStart(2, "0")}
                    </span>

                    <span className="score-label">
                      Score: {formatScore(result.final_score)}
                    </span>
                  </div>

                  <h3 className="result-title">
                    {result.url ? (
                      <a
                        href={result.url}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {result.title || result.doc_id || "Untitled document"}
                      </a>
                    ) : (
                      result.title || result.doc_id || "Untitled document"
                    )}
                  </h3>

                  {result.url && (
                    <p className="result-url">{result.url}</p>
                  )}

                  <p className="result-snippet">
                    {result.snippet || result.text || "No preview available."}
                  </p>

                  <div className="result-scores">
                    <span>
                      <strong>BM25:</strong>{" "}
                      {formatScore(result.bm25_score)}
                    </span>

                    <span>
                      <strong>PageRank:</strong>{" "}
                      {formatScore(result.pagerank_score)}
                    </span>
                  </div>
                </article>
              ))}
            </div>
          )}

          {!searched && (
            <div className="welcome-section">
              <div className="welcome-icon">⌕</div>
              <h2>Start your search</h2>
              <p>
                Enter a query above to retrieve relevant documents
                from the indexed collection.
              </p>
            </div>
          )}
        </section>

        <section className="system-info">
          <div className="system-info-item">
            <span className="system-info-label">INDEXED DOCUMENTS</span>
            <strong>{documentCount.toLocaleString()}</strong>
          </div>

          <div className="system-info-item">
            <span className="system-info-label">RETRIEVAL MODEL</span>
            <strong>BM25</strong>
          </div>

          <div className="system-info-item">
            <span className="system-info-label">RANKING MODEL</span>
            <strong>PageRank</strong>
          </div>
        </section>
      </main>

      <footer className="footer">
        Distributed Search Engine · Big Data Project
      </footer>
    </div>
  );
}

export default App;
