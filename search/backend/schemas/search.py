from pydantic import BaseModel, Field


class SearchResult(BaseModel):
    doc_id: str
    title: str
    url: str
    snippet: str
    bm25_score: float = 0.0
    pagerank_score: float = 0.0
    final_score: float = 0.0


class SearchResponse(BaseModel):
    query: str
    total_results: int
    search_time_ms: float
    results: list[SearchResult] = Field(default_factory=list)
