# Data Schemas

Three data shapes matter: the ingestion inputs, the Iceberg table, and the serving Parquet export. This document is derived from the code in `ingestion/`, `export/` and `search/backend/`.

## 1. Ingestion inputs (Node 1)

Both inputs are JSON Lines: one JSON object per line. Rules are implemented in `ingestion/core.py` and configured in `ingestion/config.py`.

### `pages.jsonl` (Phase 0) and `changes.jsonl` (Phase 1)

| Field | Type | UPSERT | DELETE | Notes |
|---|---|---|---|---|
| `op` | string | optional | required | `UPSERT` or `DELETE` (case-insensitive). A line without `op` is an UPSERT, which is how Phase 0 records look |
| `doc_id` | string or integer | required | required | Must be non-empty; integers are converted to strings |
| `url` | string | required, non-empty | ignored | |
| `title` | string | required, non-empty | ignored | |
| `text` | string | required, non-empty | ignored | |
| `links` | list of strings | optional | ignored | Targets are `doc_id` values. Missing means `[]` |
| `version` | integer | optional | optional | Defaults to the phase number (0 or 1). Must be an integer if present |

Examples:

```json
{"doc_id": "112", "url": "https://simple.wikipedia.org/wiki/Computer", "title": "Computer", "text": "A computer is ...", "links": ["709", "905"]}
{"op": "UPSERT", "doc_id": "112", "url": "...", "title": "...", "text": "...", "links": ["709"]}
{"op": "DELETE", "doc_id": "153"}
```

### Validation and conflict rules

- A line must be a JSON object. Blank lines are skipped, not rejected.
- Invalid lines are counted and written to `logs/rejects_phase<N>.jsonl`; they are never applied. Reasons: malformed JSON, not an object, invalid `op`, missing or empty `doc_id`, non-integer `version`, missing required field, empty `url`/`title`/`text`, `links` not a list of strings.
- Within one input, records are deduplicated by `doc_id`: the highest `version` wins; on a tie the later line wins. Same-version records with different content are counted as conflicts.
- An UPSERT whose version is lower than the version already in the table is ignored (stale).
- Link targets that are not documents in the resulting table are removed, preserving the order of the rest.
- If the table already contains a higher phase than the one being run, the run is skipped and changes nothing.
- To make `links` mandatory, add `"links"` to `REQUIRED_FIELDS` in `ingestion/config.py`.

## 2. Iceberg table `lake.search.documents`

Format version 2, in the warehouse `hdfs://10.145.6.245:9000/warehouse`. Created by `ingest.py`.

| Column | Type | Notes |
|---|---|---|
| `doc_id` | string | Unique per row |
| `url` | string | |
| `title` | string | |
| `text` | string | |
| `links` | array&lt;string&gt; | Every entry is the `doc_id` of a document in the table |
| `version` | int | The record's `version`, else the phase number |
| `phase` | int | Phase that last wrote the row |
| `content_hash` | string | SHA-256 of `url`, `title`, `text`, `links`; used to detect real changes |

Invariants checked by `ingest.py` and `verify.py`:

- `COUNT(*) = COUNT(DISTINCT doc_id)`
- `url`, `title` and `text` are never empty
- No dangling links
- One snapshot per phase that changed the table; the Phase 0 snapshot stays readable by time travel

Inspect snapshots with `python ingestion/verify.py ...` (it prints them), or in Spark SQL:

```sql
SELECT snapshot_id, committed_at, operation FROM lake.search.documents.snapshots;
```

## 3. Derived data on HDFS (Node 3)

| Path | Columns |
|---|---|
| `/derived/<version>/index` | `term` string, `df` int, `postings` array&lt;struct&lt;`doc_id`, `tf`&gt;&gt; |
| `/derived/<version>/doc_stats` | `doc_id` string, `doc_len` int |
| `/derived/<version>/pagerank` | `doc_id` string, `pagerank_score` double |

Tokenizer for the index: lowercase text, tokens matching `\b[a-z0-9]+\b`, over `title + " " + text`. It must match `search/backend/services/bm25_service.py`.

## 4. Serving Parquet export (Node 3 to Node 4)

Published to `/export/<version>` on HDFS by `export/export.py`, 2 files by default.

| Field | Type | Required by Node 4 | Notes |
|---|---|---|---|
| `doc_id` | string | Yes | Unique, never null |
| `title` | string | Yes | Never null |
| `url` | string | Yes | Never null |
| `text` | string | Yes | Used for BM25 and snippets |
| `pagerank_score` | double | No | Defaults to 0 if absent; the export always includes it |

The backend also accepts alternate column names (`id`, `content`, `pagerank`, and others; see the README) and a single `.parquet` file or a directory of them. `SEARCH_EXPORT_PATH` must point at the export, never at the Iceberg warehouse.

## 5. API response

`GET /search` returns:

| Field | Description |
|---|---|
| `query` | The query as sent |
| `total_results` | Number of results returned (at most `top_k`) |
| `search_time_ms` | Server-side time to search and rank |
| `results[].doc_id` | Document identifier |
| `results[].title` | Title |
| `results[].url` | Source URL |
| `results[].snippet` | Cleaned snippet, at most 240 characters |
| `results[].bm25_score` | Raw BM25 score |
| `results[].pagerank_score` | PageRank score |
| `results[].final_score` | `0.7 * (bm25 / max_bm25) + 0.3 * (pagerank / max_pagerank)`, maxima taken over all matching documents |
