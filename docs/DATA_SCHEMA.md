# Data Schemas

Three data shapes matter: the ingestion inputs, the Iceberg table, and the serving Parquet export.

> Fields marked **confirm** are inferred from the workflow description and must be checked against the ingestion code before this document is treated as authoritative.

## 1. Ingestion inputs (Node 1)

Both files are JSON Lines: one JSON object per line.

### `pages.jsonl` (Phase 0)

Initial corpus. The verified baseline has 1,000 records.

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | Unique identifier (confirm exact name and type) |
| `url` | string | Document URL; must be non-empty |
| `title` | string | Must be non-empty |
| `text` | string | Document body; must be non-empty |
| `links` | array of `doc_id` strings | Outgoing links used for PageRank (confirm exact name and shape) |

### `changes.jsonl` (Phase 1)

Change batch. The rehearsal batch had 100 lines (50 UPSERT, 50 DELETE).

| Field | Type | Notes |
|---|---|---|
| `op` | string | `UPSERT` or `DELETE` (confirm field name) |
| `doc_id` | string | Target document |
| `url`, `title`, `text`, `links` | as above | Present for UPSERT; not needed for DELETE |

### Validation rules

- Every line must parse as JSON.
- Required fields must be present and non-empty for the operation type.
- The operation must be one of the supported values.
- Invalid lines are counted and reported as rejected, not applied.
- Repeated operations on the same `doc_id` are deduplicated; contradictory operations are resolved by the ingestion rules (document the exact rule here once confirmed, for example "last operation wins").

## 2. Iceberg table: `lake.search.documents`

Stored under `hdfs://10.145.6.245:9000/warehouse`.

| Column | Type | Notes |
|---|---|---|
| `doc_id` | string | Primary identifier; unique per row |
| `url` | string | |
| `title` | string | |
| `text` | string | |
| `links` | array of string | Outgoing references; no dangling targets after processing |
| phase marker | string or int | Used by verification to report the phase distribution (confirm column name) |

Invariants checked by verification:

- `COUNT(*) = COUNT(DISTINCT doc_id)`
- `url`, `title`, and `text` are never empty
- Every id in `links` exists in the table (no dangling links)
- Snapshots exist for Phase 0 and Phase 1; the Phase 0 snapshot can be read by time travel

Inspect snapshots:

```sql
SELECT snapshot_id, committed_at, operation FROM lake.search.documents.snapshots;
SELECT COUNT(*) FROM lake.search.documents VERSION AS OF <snapshot_id>;
```

## 3. Serving Parquet export (Node 4)

| Field | Type | Required | Notes |
|---|---|---|---|
| `doc_id` | string | Yes | Unique |
| `title` | string | Yes | |
| `url` | string | Yes | |
| `text` | string | Yes | Used for BM25 and snippets |
| `pagerank_score` | double | No | Defaults to 0 if absent |

- Verified Phase 0 export: 1,000 rows, these five fields.
- The backend normalizes supported alternate column names when loading.
- `SEARCH_EXPORT_PATH` must point at the export directory, never at the Iceberg warehouse.

## 4. API response shape

`GET /search` returns query metadata and a list of results:

| Field | Description |
|---|---|
| `doc_id` | Document identifier |
| `title` | Title |
| `url` | Source URL |
| `snippet` | Cleaned display snippet |
| `bm25_score` | Raw BM25 score |
| `pagerank_score` | PageRank score |
| `final_score` | `0.7 * norm(bm25) + 0.3 * norm(pagerank)` |
