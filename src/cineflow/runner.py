# src/cineflow/runner.py
from __future__ import annotations

import argparse
import os
import sys
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from cineflow.pipelines.ingest_raw import main as step_ingest
from cineflow.dq.checks import validate_ratings, validate_movies
from cineflow.pipelines.load_warehouse import main as step_load
from cineflow.storage.postgres_admin import create_indexes_and_views

import io
from typing import Iterator, Optional, cast


def _supports_unicode() -> bool:
    enc = (sys.stdout.encoding or "").lower()
    return "utf" in enc

_USE_EMOJI = (
    os.getenv("NO_EMOJI", "").lower() not in {"1", "true"} and _supports_unicode()
)

SYMS = {
    "play": "▶" if _USE_EMOJI else ">",
    "ok": "✔" if _USE_EMOJI else "OK",
    "fail": "✖" if _USE_EMOJI else "FAIL",
    "warn": "⚠" if _USE_EMOJI else "WARN",
    "arrow": "→" if _USE_EMOJI else "->",
    "done": "✅" if _USE_EMOJI else "DONE",
    "boom": "❌" if _USE_EMOJI else "ERROR",
}

try:
    stdout = cast(io.TextIOWrapper, sys.stdout)
    stderr = cast(io.TextIOWrapper, sys.stderr)
    stdout.reconfigure(encoding="utf-8", errors="replace")
    stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


@contextmanager
def timed(label: str) -> Iterator[None]:
    t0 = time.perf_counter()
    print(f"{SYMS['play']} {label} ...", flush=True)
    try:
        yield
        dt = time.perf_counter() - t0
        print(f"{SYMS['ok']} {label} OK ({dt:.2f}s)")
    except Exception as e:
        dt = time.perf_counter() - t0
        # En stderr para que CI lo muestre bien
        print(f"{SYMS['fail']} {label} FAILED ({dt:.2f}s): {e}", file=sys.stderr)
        raise


def run(only: Optional[str] = None, skip_validate: bool = False) -> None:
    """
    Ejecuta el flujo completo. Flags:
      - only: 'ingest' | 'validate' | 'load' | 'admin' (ejecuta solo esa etapa)
      - skip_validate: salta validaciones DQ (útil en dev rápido)
    """
    if only:
        if only == "ingest":
            with timed("Ingest (Mongo raw)"):
                step_ingest()
        elif only == "validate":
            with timed("DQ ratings"):
                validate_ratings()
            with timed("DQ movies"):
                validate_movies()
        elif only == "load":
            with timed(f"Load {SYMS['arrow']} Postgres"):
                step_load()
        elif only == "admin":
            with timed("Postgres admin (índices & MV)"):
                create_indexes_and_views()
        else:
            raise SystemExit(f"--only inválido: {only}")
        return

    # Flujo completo
    with timed("Ingest (Mongo raw)"):
        step_ingest()

    if not skip_validate:
        with timed("DQ ratings"):
            validate_ratings()
        with timed("DQ movies"):
            validate_movies()
    else:
        print(f"{SYMS['warn']} Validaciones DQ saltadas por --skip-validate")

    with timed(f"Load {SYMS['arrow']} Postgres"):
        step_load()

    with timed("Postgres admin (índices & MV)"):
        create_indexes_and_views()


def main() -> None:
    parser = argparse.ArgumentParser(description="CineFlow runner")
    parser.add_argument(
        "--only",
        choices=["ingest", "validate", "load", "admin"],
        help="Ejecuta solo una etapa",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Salta validaciones DQ",
    )
    args = parser.parse_args()
    t0 = time.perf_counter()
    try:
        run(only=args.only, skip_validate=args.skip_validate)
        dt = time.perf_counter() - t0
        print(f"\n{SYMS['done']} Pipeline completo OK en {dt:.2f}s")
    except Exception:
        dt = time.perf_counter() - t0
        print(f"\n{SYMS['boom']} Pipeline FAILED en {dt:.2f}s", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
