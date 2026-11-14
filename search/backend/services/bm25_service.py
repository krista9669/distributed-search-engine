import re
from rank_bm25 import BM25Okapi


def tokenize(text: str) -> list[str]:
    """Convert text into lowercase word tokens."""
    return re.findall(r"\b[a-z0-9]+\b", text.lower())


class BM25Service:
    def __init__(self):
        self.documents = []
        self.tokenized_documents = []
        self.index = None

    def build_index(self, documents: list[dict]):
        self.documents = documents

        self.tokenized_documents = [
            tokenize(f"{doc.get('title', '')} {doc.get('text', '')}")
            for doc in documents
        ]

        if self.tokenized_documents:
            self.index = BM25Okapi(
                self.tokenized_documents,
                k1=1.2,
                b=0.75
            )
        else:
            self.index = None

    def search(self, query: str, top_k: int = 10):
        query_tokens = tokenize(query)

        if self.index is None or not query_tokens:
            return []

        scores = self.index.get_scores(query_tokens)
        query_terms = set(query_tokens)

        ranked_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True
        )

        results = []

        for index in ranked_indices:
            document_terms = set(self.tokenized_documents[index])

            # Exclude documents that do not contain any query term.
            if not query_terms.intersection(document_terms):
                continue

            results.append({
                "document": self.documents[index],
                "bm25_score": float(scores[index])
            })

            if len(results) >= top_k:
                break

        return results
