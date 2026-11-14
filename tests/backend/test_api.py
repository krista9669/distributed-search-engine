import pytest
from fastapi.testclient import TestClient

from search.backend.main import app, search_service


@pytest.fixture
def client():
    original_documents = search_service.documents
    original_bm25 = search_service.bm25
    original_loaded = search_service.loaded

    search_service.load("data/export/test")

    with TestClient(app) as test_client:
        yield test_client

    search_service.documents = original_documents
    search_service.bm25 = original_bm25
    search_service.loaded = original_loaded


def test_root_endpoint(client):
    response = client.get("/")

    assert response.status_code == 200
    assert "running" in response.json()["message"]


def test_health_endpoint(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["index_loaded"] is True
    assert response.json()["documents_loaded"] == 2


def test_search_endpoint(client):
    response = client.get("/search", params={"q": "machine learning"})

    assert response.status_code == 200

    data = response.json()

    assert data["total_results"] == 2
    assert data["results"][0]["title"] == "Machine Learning"


def test_search_empty_query(client):
    response = client.get("/search", params={"q": ""})

    assert response.status_code == 422


def test_search_top_k_limit(client):
    response = client.get(
        "/search",
        params={"q": "machine learning", "top_k": 100}
    )

    assert response.status_code == 422
