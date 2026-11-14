class RankingService:
    BM25_WEIGHT = 0.7
    PAGERANK_WEIGHT = 0.3

    @staticmethod
    def rank(bm25_results: list[dict], top_k: int = 10):
        if not bm25_results:
            return []

        bm25_scores = [
            item["bm25_score"] for item in bm25_results
        ]

        pagerank_scores = [
            float(item["document"].get("pagerank_score", 0.0))
            for item in bm25_results
        ]

        min_bm25 = min(bm25_scores)
        max_bm25 = max(bm25_scores)
        max_pagerank = max(pagerank_scores)

        ranked_results = []

        for item in bm25_results:
            document = item["document"]

            bm25_score = item["bm25_score"]
            pagerank_score = float(
                document.get("pagerank_score", 0.0)
            )

            if max_bm25 > min_bm25:
                normalized_bm25 = (
                    (bm25_score - min_bm25)
                    / (max_bm25 - min_bm25)
                )
            else:
                normalized_bm25 = 1.0

            normalized_pagerank = (
                pagerank_score / max_pagerank
                if max_pagerank > 0 else 0.0
            )

            final_score = (
                RankingService.BM25_WEIGHT * normalized_bm25
                + RankingService.PAGERANK_WEIGHT * normalized_pagerank
            )

            ranked_results.append({
                "document": document,
                "bm25_score": bm25_score,
                "pagerank_score": pagerank_score,
                "final_score": final_score
            })

        ranked_results.sort(
            key=lambda item: item["final_score"],
            reverse=True
        )

        return ranked_results[:top_k]
