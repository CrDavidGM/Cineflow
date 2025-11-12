"""Carga CSVs de MovieLens a MongoDB (colección ratings_raw y movies_raw)."""
from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any, Tuple

from cineflow.storage.mongo_client import get_mongo

SAMPLES = Path("data/samples")


def main() -> None:
    """Carga ratings.csv y movies.csv a MongoDB (colecciones ratings_raw y movies_raw)."""
    client, db = get_mongo()  

    ratings_csv = SAMPLES / "ratings.csv"
    movies_csv = SAMPLES / "movies.csv"

    if not ratings_csv.exists() or not movies_csv.exists():
        raise FileNotFoundError("Coloca ratings.csv y movies.csv en data/samples/")

    ratings_df = pd.read_csv(ratings_csv)
    movies_df = pd.read_csv(movies_csv)

    # Limpieza y carga 
    db.ratings_raw.drop()
    db.movies_raw.drop()

    db.ratings_raw.insert_many(ratings_df.to_dict(orient="records"))
    db.movies_raw.insert_many(movies_df.to_dict(orient="records"))

    print(f"Insertados: ratings={len(ratings_df)} | movies={len(movies_df)}")


if __name__ == "__main__":
    main()
