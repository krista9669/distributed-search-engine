from time import perf_counter

from search.backend.services.bm25_service import BM25Service
from search.backend.services.parquet_loader import load_documents
from search.backend.services.ranking_service import RankingService
from search.backend.services.snippet_cleaner import clean_wikipedia_text


class SearchService:
    def __init__(self):
        self.documents = []
        self.bm25 = BM25Service()
        self.loaded = False

    def load(self, export_path: str):
        """Load documents and build the BM25 index."""
        documents = load_documents(export_path)

        self.documents = documents
        self.bm25.build_index(documents)
        self.loaded = True

        return len(documents)

    def search(self, query: str, top_k: int = 10):
        """Search documents and return combined BM25/PageRank rankings."""
        if not self.loaded:
            raise RuntimeError("Search index has not been loaded.")

        start = perf_counter()

        bm25_results = self.bm25.search(
            query,
            top_k=len(self.documents)
        )

        ranked_results = RankingService.rank(
            bm25_results,
            top_k=top_k
        )

        results = []

        for item in ranked_results:
            document = item["document"]
            text = document.get("text", "")

            results.append({
                "doc_id": document["doc_id"],
                "title": document["title"],
                "url": document["url"],
                "snippet": clean_wikipedia_text(text, max_length=240),
                "bm25_score": item["bm25_score"],
                "pagerank_score": item["pagerank_score"],
                "final_score": item["final_score"]
            })

        elapsed_ms = (perf_counter() - start) * 1000

        return {
            "total_results": len(results),
            "search_time_ms": round(elapsed_ms, 3),
            "results": results
        }
