# Distributed Search Engine

A distributed search engine built using Apache Spark, Hadoop HDFS, Apache Iceberg, FastAPI, and React. The system processes web page data, builds a distributed search index, calculates PageRank scores, and provides a web interface for searching and retrieving ranked results.

## Overview

The project implements a distributed pipeline for:

- Ingesting and validating web page datasets.
- Cleaning and deduplicating document records.
- Storing versioned data using Apache Iceberg.
- Building an inverted index using Apache Spark.
- Computing PageRank scores across linked documents.
- Exporting indexed data in Parquet format.
- Serving search results through a FastAPI backend and React frontend.

The search service combines BM25 relevance scores with PageRank scores to rank documents.

## System Architecture

The project follows a four-node distributed architecture.

| Node | Responsibility | Components |
|---|---|---|
| Node 1 | Data ingestion and validation | Python, PySpark, Apache Iceberg |
| Node 2 | HDFS storage and Spark worker | Hadoop HDFS, Apache Spark |
| Node 3 | Distributed processing and indexing | Apache Spark, PageRank, Inverted Index |
| Node 4 | Search service and frontend | FastAPI, React, Vite |

### Data Flow

1. **Ingestion:** The dataset is uploaded, validated, cleaned, and deduplicated.
2. **Storage:** Validated records are stored in the distributed data lake using Apache Iceberg.
3. **Processing:** Spark processes documents and their link relationships.
4. **Indexing:** A distributed inverted index is generated.
5. **PageRank:** PageRank scores are calculated using the document link graph.
6. **Export:** Processed documents and ranking information are exported as Parquet files.
7. **Search:** The FastAPI service loads the exported data and performs BM25-based retrieval.
8. **Ranking:** BM25 and PageRank scores are combined to produce the final ranking.
9. **Presentation:** Search results are displayed through the React frontend.

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python |
| Distributed Storage | Hadoop HDFS |
| Distributed Processing | Apache Spark |
| Data Lakehouse | Apache Iceberg |
| Data Format | Apache Parquet |
| Search Retrieval | BM25 |
| Link Analysis | PageRank |
| Backend | FastAPI |
| Frontend | React |
| Frontend Build Tool | Vite |
| Data Processing | Pandas, PyArrow |
| Inter-node Networking | ZeroTier |

## Repository Structure

```text
distributed-search-engine/
│
├── data/
│   ├── export/
│   │   └── .gitkeep
│   ├── phase0/
│   └── phase1/
│
├── ingestion/
│
├── indexing/
│
├── pagerank/
│
├── export/
│
├── warehouse/
│
├── search/
│   ├── backend/
│   │   ├── main.py
│   │   ├── routers/
│   │   ├── schemas/
│   │   │   └── search.py
│   │   └── services/
│   │       ├── bm25_service.py
│   │       ├── parquet_loader.py
│   │       ├── ranking_service.py
│   │       └── search_service.py
│   │
│   └── frontend/
│       ├── public/
│       ├── src/
│       │   ├── assets/
│       │   ├── App.jsx
│       │   ├── App.css
│       │   ├── index.css
│       │   └── main.jsx
│       ├── package.json
│       └── vite.config.js
│
├── tests/
│   ├── backend/
│   │   ├── test_api.py
│   │   └── test_search_services.py
│   └── conftest.py
│
├── requirements.txt
└── .gitignore
```

*Some directories are reserved for the distributed pipeline components and may be populated as cluster integration progresses.*

## Search Ranking

The search service combines two ranking signals:

- **BM25:** Measures the relevance of a document to the user's query.
- **PageRank:** Measures the importance of a document based on its incoming links.

The final score uses the following weighted combination:

\[
FinalScore = 0.7 \times NormalizedBM25
+ 0.3 \times NormalizedPageRank
\]

Current backend implementation uses min-max normalization for BM25 candidate scores and maximum-score normalization for PageRank.

## Node 4: Search Service

Node 4 hosts the search application.

### Backend

The FastAPI backend provides:

- Parquet data loading.
- Document normalization.
- BM25 index construction.
- Query processing and retrieval.
- Combined BM25 and PageRank ranking.
- Search result snippets.
- Health monitoring.

### Frontend

The React frontend provides:

- Search input.
- Search result presentation.
- Document links.
- Relevance and PageRank information.
- Backend health status.

## Installation and Setup

### Prerequisites

- Python 3.10
- Node.js and npm
- Git

### 1. Clone the repository

```bash
git clone git@github.com:krista9669/distributed-search-engine.git
cd distributed-search-engine
```

### 2. Set up the Python environment

```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 3. Configure the search export

The backend expects a Parquet file or directory containing Parquet files.

By default, it looks for:

```text
data/export/phase0/
```

Set a custom path if needed:

```bash
export SEARCH_EXPORT_PATH=data/export/phase0
```

The export must contain the following fields:

| Field | Description |
|---|---|
| `doc_id` | Unique document identifier |
| `title` | Document title |
| `url` | Document URL |
| `text` | Document content |
| `pagerank_score` | PageRank score (optional; defaults to 0) |

The loader supports alternative column names for these fields.

### 4. Start the backend

From the repository root:

```bash
source .venv/bin/activate

uvicorn search.backend.main:app --host 0.0.0.0 --port 8000
```

Backend endpoints:

- API root: `http://localhost:8000/`
- Health: `http://localhost:8000/health`
- Search: `http://localhost:8000/search`
- Interactive API documentation: `http://localhost:8000/docs`

### 5. Set up the frontend

Open another terminal:

```bash
cd search/frontend
npm install
```

Create a local environment file:

```bash
cp .env.example .env.local
```

Configure the backend address in `.env.local`:

```env
VITE_API_URL=http://127.0.0.1:8000
```

For access from other machines, replace the address with the Node 4 ZeroTier IP.

Start the frontend:

```bash
npm run dev -- --host 0.0.0.0
```

Open:

```text
http://localhost:5173
```

## API Reference

### Health Check

**GET** `/health`

Returns the backend and index loading status.

Example response:

```json
{
  "status": "healthy",
  "index_loaded": true,
  "documents_loaded": 1000
}
```

### Search

**GET** `/search`

Query parameters:

| Parameter | Description | Default |
|---|---|---|
| `q` | Search query | Required |
| `top_k` | Maximum number of results (1–50) | 10 |

Example:

```text
GET /search?q=machine%20learning&top_k=10
```

The response includes the query, total results, search execution time, and ranked documents.

Each result contains:

- Document ID
- Title
- URL
- Text snippet
- BM25 score
- PageRank score
- Final ranking score

## Testing

Run the backend tests from the repository root:

```bash
source .venv/bin/activate

python -m pytest -v tests/backend/
```

The current backend test suite covers:

- Parquet loading.
- Missing export paths.
- BM25 retrieval.
- Empty queries.
- Ranking behavior.
- API endpoints and validation.

## Development Status

| Component | Status |
|---|---|
| Node 4 FastAPI backend | Implemented |
| Parquet loader | Implemented |
| BM25 retrieval | Implemented |
| BM25 + PageRank ranking | Implemented |
| React search frontend | Implemented |
| Backend API tests | Passing |
| Frontend production build | Successful |
| Distributed cluster integration | In progress |
| End-to-end distributed pipeline | Pending integration |

## Contributors

This project is developed collaboratively by a four-member team using a distributed four-node architecture.

| Node | Team Member (PES ID) | Responsibility |
|---|---|---|
| Node 1 | PES2UG24CS011 | Data Ingestion and Validation |
| Node 2 | PES2UG24CS361 | HDFS Storage and Spark Worker |
| Node 3 | PES2UG24CS200 | Distributed Processing, Inverted Index and PageRank |
| Node 4 | PES2UG24CS234 | FastAPI Search Service and React Frontend |

Each team member is responsible for implementing and integrating their assigned component into the distributed search engine.

---

**Distributed Search Engine — Building scalable document search through distributed processing and intelligent ranking.**
