import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from search.backend.schemas.search import SearchResponse
from search.backend.services.search_service import SearchService


EXPORT_PATH = os.getenv(
    "SEARCH_EXPORT_PATH",
    "data/export/phase0"
)

search_service = SearchService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        count = search_service.load(EXPORT_PATH)
        print(f"Loaded {count} documents from {EXPORT_PATH}")
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"Search index not loaded: {exc}")

    yield


app = FastAPI(
    title="Distributed Search Engine API",
    description="Backend API for the distributed search engine",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://10.145.6.187:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "message": "Distributed Search Engine API is running!"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "index_loaded": search_service.loaded,
        "documents_loaded": len(search_service.documents)
    }


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1, description="Search query"),
    top_k: int = Query(default=10, ge=1, le=50)
):
    if not search_service.loaded:
        raise HTTPException(
            status_code=503,
            detail="Search index is not loaded. Check the Phase 0 export."
        )

    try:
        response = search_service.search(q.strip(), top_k)

        return {
            "query": q,
            **response
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(exc)}"
        ) from exc
