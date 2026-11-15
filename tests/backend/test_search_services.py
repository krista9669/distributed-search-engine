
import pandas as pd
import pytest

from search.backend.services.parquet_loader import load_documents
from search.backend.services.bm25_service import BM25Service
from search.backend.services.ranking_service import RankingService


@pytest.fixture
def sample_documents():
    return [
        {
            "doc_id": "1",
            "title": "Machine Learning",
            "url": "https://example.com/ml",
            "text": "Machine learning is a branch of artificial intelligence.",
            "pagerank_score": 0.85
        },
        {
            "doc_id": "2",
            "title": "Deep Learning",
            "url": "https://example.com/dl",
            "text": "Deep learning uses neural networks.",
            "pagerank_score": 0.65
        },
        {
            "doc_id": "3",
            "title": "Database Systems",
            "url": "https://example.com/db",
            "text": "Database systems store structured information.",
            "pagerank_score": 0.45
        }
    ]


def test_parquet_loader(tmp_path, sample_documents):
    dataframe = pd.DataFrame(sample_documents)
    dataframe.to_parquet(tmp_path / "documents.parquet", index=False)

    documents = load_documents(tmp_path)

    assert len(documents) == 3
    assert documents[0]["doc_id"] == "1"
    assert documents[0]["pagerank_score"] == 0.85


def test_parquet_loader_missing_path():
    with pytest.raises(FileNotFoundError):
        load_documents("data/nonexistent")


def test_bm25_returns_relevant_documents(sample_documents):
    service = BM25Service()
    service.build_index(sample_documents)

    results = service.search("machine learning", top_k=10)

    assert len(results) > 0
    assert results[0]["document"]["doc_id"] == "1"


def test_bm25_empty_query(sample_documents):
    service = BM25Service()
    service.build_index(sample_documents)

    assert service.search("") == []


def test_ranking_service(sample_documents):
    bm25_results = [
        {"document": sample_documents[0], "bm25_score": 2.0},
        {"document": sample_documents[1], "bm25_score": 1.0}
    ]

    results = RankingService.rank(bm25_results)

    assert len(results) == 2
    assert results[0]["document"]["doc_id"] == "1"
    assert results[0]["final_score"] == pytest.approx(1.0)


def test_ranking_uses_max_normalization(sample_documents):
    bm25_results = [
        {"document": sample_documents[0], "bm25_score": 10.0},
        {"document": sample_documents[1], "bm25_score": 5.0}
    ]

    results = RankingService.rank(bm25_results)

    second = next(
        item for item in results
        if item["document"]["doc_id"] == "2"
    )

    expected = 0.7 * (5.0 / 10.0) + 0.3 * (0.65 / 0.85)

    assert second["final_score"] == pytest.approx(expected)


def test_ranking_uses_exact_70_30_weights(sample_documents):
    bm25_results = [
        {"document": sample_documents[0], "bm25_score": 8.0},
        {"document": sample_documents[1], "bm25_score": 4.0}
    ]

    results = RankingService.rank(bm25_results)

    second = next(
        item for item in results
        if item["document"]["doc_id"] == "2"
    )

    expected = 0.7 * 0.5 + 0.3 * (0.65 / 0.85)

    assert second["final_score"] == pytest.approx(expected)


def test_ranking_handles_zero_scores(sample_documents):
    documents = [
        {**sample_documents[0], "pagerank_score": 0.0},
        {**sample_documents[1], "pagerank_score": 0.0}
    ]

    bm25_results = [
        {"document": documents[0], "bm25_score": 0.0},
        {"document": documents[1], "bm25_score": 0.0}
    ]

    results = RankingService.rank(bm25_results)

    assert all(item["final_score"] == pytest.approx(0.0)
               for item in results)


def test_ranking_empty_results():
    assert RankingService.rank([]) == []
