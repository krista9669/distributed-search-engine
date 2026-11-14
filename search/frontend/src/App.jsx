import { useEffect, useState } from "react";
import "./App.css";

const API_URL = (import.meta.env.VITE_API_URL || "http://127.0.0.1:8000").replace(
  /\/$/,
  ""
);

const suggestions = ["distributed systems", "machine learning", "data indexing"];

function SearchIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="10.8" cy="10.8" r="6.8" />
      <path d="m16 16 4.2 4.2" />
    </svg>
  );
}

function ArrowIcon() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      <path d="M4 10h11m-4-4 4 4-4 4" />
    </svg>
  );
}

function App() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState("");
  const [backendStatus, setBackendStatus] = useState("checking");
  const [documentCount, setDocumentCount] = useState(0);
  const [totalResults, setTotalResults] = useState(0);
  const [searchTime, setSearchTime] = useState(null);
  const [theme, setTheme] = useState(() => {
    const savedTheme = localStorage.getItem("search-theme");
    return savedTheme === "dark" ? "dark" : "light";
  });

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("search-theme", theme);
  }, [theme]);

  useEffect(() => {
    const checkBackend = async () => {
      try {
        const response = await fetch(`${API_URL}/health`);
        if (!response.ok) {
          setBackendStatus("offline");
          return;
        }

        const data = await response.json();
        setDocumentCount(data.documents_loaded ?? 0);
        setBackendStatus(data.index_loaded ? "ready" : "unindexed");
      } catch {
        setBackendStatus("offline");
      }
    };

    checkBackend();
  }, []);

  const performSearch = async (searchTerm = query) => {
    const normalizedQuery = searchTerm.trim();
    if (!normalizedQuery || loading) return;

    setQuery(normalizedQuery);
    setLoading(true);
    setError("");
    setSearched(true);
    setResults([]);
    setTotalResults(0);
    setSearchTime(null);

    try {
      const params = new URLSearchParams({ q: normalizedQuery, top_k: "10" });
      const response = await fetch(`${API_URL}/search?${params}`);
      const data = await response.json();

      if (!response.ok) {
        throw new Error(
          typeof data.detail === "string"
            ? data.detail
            : "Search request failed. Please try again."
        );
      }

      setResults(data.results ?? []);
      setTotalResults(data.total_results ?? data.results?.length ?? 0);
      setSearchTime(data.search_time_ms ?? null);
    } catch (searchError) {
      setError(searchError.message || "Unable to connect to the search service.");
    } finally {
      setLoading(false);
    }
  };

  const clearSearch = () => {
    setQuery("");
    setResults([]);
    setError("");
    setSearched(false);
    setTotalResults(0);
    setSearchTime(null);
  };

  const formatScore = (score) =>
    typeof score === "number" ? score.toFixed(4) : "—";

  const statusText = {
    checking: "Checking service",
    ready: "Search index ready",
    unindexed: "Index not loaded",
    offline: "Service unavailable",
  }[backendStatus];

  return (
    <div className="app">
      <header className="site-header">
        <div className="header-inner">
          <button className="brand" onClick={clearSearch} aria-label="Go to home">
            <span className="brand-name">Distributed Search</span>
          </button>

          <div className="header-actions">
            <div className={`service-status ${backendStatus}`} role="status">
              <span className="status-indicator" />
              <span>{statusText}</span>
            </div>
            <span className="header-divider" aria-hidden="true" />
            <button
              className="theme-toggle"
              onClick={() => setTheme((current) => current === "light" ? "dark" : "light")}
              aria-label={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
              title={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
            >
              {theme === "light" ? (
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M20.1 15.4A8.3 8.3 0 0 1 8.6 3.9 8.4 8.4 0 1 0 20.1 15.4Z" />
                </svg>
              ) : (
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <circle cx="12" cy="12" r="4" />
                  <path d="M12 2v2m0 16v2M4.93 4.93l1.42 1.42m11.3 11.3 1.42 1.42M2 12h2m16 0h2M4.93 19.07l1.42-1.42m11.3-11.3 1.42-1.42" />
                </svg>
              )}
            </button>
          </div>
        </div>
      </header>

      <main className="main-content">
        <section className="hero">
          <div className="hero-copy">
            <div className="eyebrow"><span /> SEARCH, WITH CONTEXT</div>
            <h1>Find the signal<br />in <span>all that data.</span></h1>
            <p className="hero-description">
              A smarter way to explore your document collection. Search by
              relevance, then let link authority surface what matters.
            </p>
          </div>

          <aside className="engine-card" aria-label="Search engine status">
            <div className="engine-card-top">
              <span className="engine-orbit"><span /></span>
              <span className="engine-label">YOUR SEARCH ENGINE</span>
              <span className="engine-version">NODE 04</span>
            </div>
            <div className="engine-card-divider" />
            <div className="engine-stat">
              <span>Indexed documents</span>
              <strong>{documentCount.toLocaleString()}</strong>
            </div>
            <div className="engine-card-foot">
              <span className={`engine-live ${backendStatus}`} />
              {backendStatus === "ready" ? "Ready to search" : statusText}
            </div>
          </aside>
        </section>

        <section className="search-area" aria-label="Document search">
          <form
            className="search-form"
            onSubmit={(event) => {
              event.preventDefault();
              performSearch();
            }}
          >
            <span className="search-icon"><SearchIcon /></span>
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="What are you looking for?"
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
              <span>{loading ? "Searching" : "Search"}</span>
              {!loading && <ArrowIcon />}
              {loading && <span className="button-spinner" />}
            </button>
          </form>

          {!searched && (
            <div className="suggestions">
              <span>TRY SEARCHING</span>
              {suggestions.map((suggestion) => (
                <button
                  type="button"
                  key={suggestion}
                  onClick={() => performSearch(suggestion)}
                  disabled={loading}
                >
                  {suggestion}
                </button>
              ))}
            </div>
          )}
        </section>

        {searched ? (
          <section className="results-section" aria-live="polite">
            <div className="results-heading">
              <div>
                <div className="results-kicker">SEARCH RESULTS</div>
                <h2>{error ? "We hit a snag" : `Results for “${query}”`}</h2>
              </div>
              {!error && !loading && (
                <div className="results-meta">
                  <span>{totalResults.toLocaleString()} {totalResults === 1 ? "result" : "results"}</span>
                  {searchTime !== null && (
                    <>
                      <span className="meta-dot" />
                      <span>{searchTime < 1 ? "< 1" : searchTime.toFixed(1)} ms</span>
                    </>
                  )}
                </div>
              )}
            </div>

            {loading && (
              <div className="state-card">
                <span className="loader" />
                <div>
                  <strong>Searching your collection</strong>
                  <p>Ranking documents by relevance and authority…</p>
                </div>
              </div>
            )}

            {error && !loading && (
              <div className="state-card error-card" role="alert">
                <span className="state-icon">!</span>
                <div>
                  <strong>Search couldn’t be completed</strong>
                  <p>{error}</p>
                  <button className="retry-button" onClick={() => performSearch()}>
                    Try again <ArrowIcon />
                  </button>
                </div>
              </div>
            )}

            {!loading && !error && results.length === 0 && (
              <div className="state-card empty-card">
                <span className="state-icon"><SearchIcon /></span>
                <div>
                  <strong>No matching documents</strong>
                  <p>Try a different phrase or use fewer, broader keywords.</p>
                </div>
              </div>
            )}

            {!loading && !error && results.length > 0 && (
              <div className="results-list">
                {results.map((result, index) => (
                  <article className="result-card" key={result.doc_id ?? index}>
                    <div className="result-rank">{String(index + 1).padStart(2, "0")}</div>
                    <div className="result-content">
                      <div className="result-source">
                        <span className="source-mark">DOC</span>
                        <span>{result.doc_id || "Indexed document"}</span>
                      </div>
                      <h3 className="result-title">
                        {result.url ? (
                          <a href={result.url} target="_blank" rel="noreferrer">
                            {result.title || result.doc_id || "Untitled document"}
                          </a>
                        ) : (
                          result.title || result.doc_id || "Untitled document"
                        )}
                      </h3>
                      {result.url && <p className="result-url">{result.url}</p>}
                      <p className="result-snippet">
                        {result.snippet || result.text || "No preview available."}
                      </p>
                      <div className="result-signals">
                        <span><i className="signal-dot relevance" /> BM25 <strong>{formatScore(result.bm25_score)}</strong></span>
                        <span><i className="signal-dot authority" /> PageRank <strong>{formatScore(result.pagerank_score)}</strong></span>
                      </div>
                    </div>
                    <div className="final-score">
                      <span>RANK SCORE</span>
                      <strong>{formatScore(result.final_score)}</strong>
                    </div>
                  </article>
                ))}
              </div>
            )}
          </section>
        ) : (
          <section className="discovery" aria-label="How search works">
            <div className="discovery-intro">
              <span className="section-index">01 / BUILT FOR DISCOVERY</span>
              <h2>More than matching words.</h2>
              <p>Every result is evaluated from more than one angle, so the best answers rise to the top.</p>
            </div>
            <div className="feature-grid">
              <article className="feature-card">
                <span className="feature-number">01</span>
                <div className="feature-icon relevance-icon"><SearchIcon /></div>
                <h3>Relevant by design</h3>
                <p>BM25 understands how well a document matches the words you search for.</p>
                <span className="feature-tag">BM25 RETRIEVAL</span>
              </article>
              <article className="feature-card">
                <span className="feature-number">02</span>
                <div className="feature-icon authority-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <circle cx="12" cy="12" r="2.4" />
                    <circle cx="5" cy="6" r="1.5" />
                    <circle cx="19" cy="6" r="1.5" />
                    <circle cx="5" cy="18" r="1.5" />
                    <circle cx="19" cy="18" r="1.5" />
                    <path d="m10.3 10.3-4.1-3m7.5 3 4.1-3m-7.5 6.4-4.1 3m7.5-3 4.1 3" />
                  </svg>
                </div>
                <h3>Authority matters</h3>
                <p>PageRank adds the importance of a document’s place in the wider link graph.</p>
                <span className="feature-tag">PAGERANK SIGNAL</span>
              </article>
              <article className="feature-card infrastructure-card">
                <span className="feature-number">03</span>
                <div className="feature-icon infrastructure-icon">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M4 7h16M4 12h16M4 17h16" />
                    <circle cx="8" cy="7" r="2" />
                    <circle cx="15" cy="12" r="2" />
                    <circle cx="10" cy="17" r="2" />
                  </svg>
                </div>
                <h3>Built to scale</h3>
                <p>Distributed indexing helps search work across growing document collections.</p>
                <span className="feature-tag">DISTRIBUTED INDEX</span>
              </article>
            </div>
          </section>
        )}

        <footer className="site-footer">
          <span>Distributed Search Engine</span>
          <span>Relevance, with a wider view.</span>
        </footer>
      </main>
    </div>
  );
}

export default App;
