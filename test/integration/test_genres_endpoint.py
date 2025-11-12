import subprocess
import time

import requests


def test_genres_top_endpoint() -> None:
    api = subprocess.Popen(
        [
            "poetry",
            "run",
            "uvicorn",
            "cineflow.api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8002",
        ]
    )
    try:
        time.sleep(2.0)
        r = requests.get("http://127.0.0.1:8000/genres/top?limit=5", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert "items" in data
    finally:
        api.terminate()
