# tests/integration/test_pipeline_and_api.py
import os
import sys
import time
import requests
import subprocess
from pathlib import Path

def _env_utf8() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("UVICORN_ACCESS_LOG", "false")  # menos ruido
    return env

def test_pipeline_and_api_smoke() -> None:
    # 1) CSVs presentes
    samples = Path("data/samples")
    assert (samples / "ratings.csv").exists()
    assert (samples / "movies.csv").exists()

    # 2) Ejecuta el pipeline (sin Poetry, usando el Python actual)
    run = subprocess.run(
        [sys.executable, "-m", "cineflow.runner", "--skip-validate"],
        check=True,
        capture_output=True,
        text=True,
        env=_env_utf8(),
    )
    # Si falla, ver stdout/stderr en el reporte
    print(run.stdout)
    print(run.stderr)

    # 3) Levanta la API en background (puerto 8001)
    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "cineflow.api.main:app",
         "--host", "127.0.0.1", "--port", "8001"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        env=_env_utf8(),
    )

    try:
        # Espera con reintentos a que arranque
        base = "http://127.0.0.1:8001"
        for _ in range(40):  # ~10s máx
            try:
                r = requests.get(f"{base}/health", timeout=0.25)
                if r.status_code == 200:
                    break
            except Exception:
                time.sleep(0.25)
        else:
            # si nunca respondió, vuelca logs
            if api.stdout:
                print(api.stdout.read())
            raise AssertionError("La API no inició a tiempo")

        # Smoke endpoints
        r = requests.get(f"{base}/health", timeout=5)
        assert r.status_code == 200
        assert r.json().get("status") == "ok"

        r = requests.get(f"{base}/movies/top?limit=5", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert "items" in data and len(data["items"]) > 0

    finally:
        api.terminate()
        try:
            api.wait(timeout=5)
        except subprocess.TimeoutExpired:
            api.kill()
