"""Conversión MovieLens 100K: u.data/u.item -> ratings.csv/movies.csv."""
from __future__ import annotations

from pathlib import Path
from typing import List, Any, Dict, Optional, cast

import pandas as pd

BASE = Path("data/samples")


def read_genres_list(base: Path) -> List[str]:
    """Lee u.genre si existe; si no, devuelve el fallback estándar del ML-100K."""
    ugenre = base / "u.genre"
    if ugenre.exists():
        lines = ugenre.read_text(encoding="latin-1").splitlines()
        genres: List[str] = [
            line.split("|")[0].strip()
            for line in lines
            if line.strip() and "|" in line
        ]
        # Evita listas vacías por líneas en blanco al final
        return [g for g in genres if g]
    # Fallback estándar (19 flags)
    return [
        "unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
        "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery",
        "Romance", "Sci-Fi", "Thriller", "War", "Western",
    ]


def convert_ratings(base: Path) -> pd.DataFrame:
    """Convierte u.data (TSV) a ratings.csv con columnas: userId, movieId, rating, timestamp."""
    udata = base / "u.data"
    if not udata.exists():
        raise FileNotFoundError(f"No existe {udata}. Descarga el ML-100K y colócalo en {base}/")

    df = pd.read_csv(
        udata,
        sep="\t",
        header=None,
        names=["userId", "movieId", "rating", "timestamp"],
        encoding="latin-1",
    )
    return cast(pd.DataFrame, df)


def convert_movies(base: Path) -> pd.DataFrame:
    """Convierte u.item + u.genre (si existe) a movies.csv: movieId, title, genres (pipe)."""
    uitem = base / "u.item"
    if not uitem.exists():
        raise FileNotFoundError(f"No existe {uitem}. Descarga el ML-100K y colócalo en {base}/")

    genres_list = read_genres_list(base)

    raw = pd.read_csv(uitem, sep="|", header=None, encoding="latin-1")
    ncols = raw.shape[1]
    g = len(genres_list)

    # u.item: [movieId, title, release_date, video_release_date, imdb_url] + g flags
    min_expected = 5 + g
    if ncols < min_expected:
        raise ValueError(f"u.item tiene {ncols} columnas, pero se esperaban al menos {min_expected}.")

    # Tomamos movieId(0), title(1) y los últimos g flags
    core = raw.iloc[:, [0, 1] + list(range(ncols - g, ncols))].copy()
    core.columns = ["movieId", "title"] + genres_list

    def flags_to_genres(row: pd.Series) -> str:  # type: ignore[type-arg]
        present = [name for name in genres_list if row[name] == 1]
        return "|".join(present) if present else "(no genres listed)"

    core["genres"] = core.apply(flags_to_genres, axis=1)
    movies = core[["movieId", "title", "genres"]].copy()
    return cast(pd.DataFrame, movies)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    ratings_out = BASE / "ratings.csv"
    movies_out = BASE / "movies.csv"

    ratings = convert_ratings(BASE)
    write_csv(ratings, ratings_out)
    print(f"[OK] Escribí {ratings_out} con {len(ratings)} filas")

    movies = convert_movies(BASE)
    write_csv(movies, movies_out)
    print(f"[OK] Escribí {movies_out} con {len(movies)} filas")


if __name__ == "__main__":
    main()
