# Distributed Search frontend

The React and Vite frontend provides a responsive search experience for the
FastAPI service in `search/backend`.

## Run locally

From this directory, install the locked dependencies and start the dev server:

```bash
npm ci
npm run dev
```

By default, the app connects to `http://127.0.0.1:8000`. To use another backend,
set `VITE_API_URL` in a local `.env.local` file:

```env
VITE_API_URL=http://127.0.0.1:8000
```

The frontend checks `/health` to show whether the API and search index are ready.
Search results include BM25 and PageRank signals, the combined ranking score,
result count, and search duration. Theme preference is saved in browser storage.

## Checks

```bash
npm run lint
npm run build
```
