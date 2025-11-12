from fastapi.testclient import TestClient

from cineflow.api.main import app

client = TestClient(app)


def test_health_inmemory() -> None:
    r = client.get("/health")
    assert r.status_code == 200


def test_movies_top_inmemory() -> None:
    r = client.get("/movies/top?limit=5")
    assert r.status_code == 200

