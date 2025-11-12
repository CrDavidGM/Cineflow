import os
import signal
import subprocess
import sys
import time

import requests


def test_genres_top_endpoint() -> None:
    env = os.environ.copy()
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("MONGO_URI", "mongodb://cineflow:cineflow@localhost:27017/?authSource=admin")

    p = subprocess.Popen(
        [
            "poetry",
            "run",
            "uvicorn",
            "cineflow.api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8002",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # Esperar hasta que /health responda (máx. 6 s)
        for _ in range(30):
            try:
                r = requests.get("http://127.0.0.1:8002/health", timeout=1.0)
                if r.status_code == 200:
                    break
            except Exception:
                time.sleep(0.2)
        else:
            raise RuntimeError("API no respondió /health")

        r = requests.get("http://127.0.0.1:8002/genres/top?limit=5", timeout=10)
        assert r.status_code == 200
        assert "items" in r.json()
    finally:
        if sys.platform.startswith("win"):
            p.terminate()
        else:
            os.kill(p.pid, signal.SIGTERM)

        if p.stdout:
            try:
                out = p.stdout.read()
                if out:
                    print("\n--- uvicorn logs ---\n", out)
            except Exception:
                pass
