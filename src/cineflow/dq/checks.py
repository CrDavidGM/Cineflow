from pathlib import Path

import pandas as pd

BASE = Path("data/samples")


def validate_ratings() -> None:
    df = pd.read_csv(BASE / "ratings.csv")
    assert df["rating"].between(0, 5).all(), "Ratings fuera de 0–5"
    assert df[["userId", "movieId", "timestamp"]].notna().all().all(), "Nulos en claves"

    dups = df.duplicated(subset=["userId", "movieId", "timestamp"]).sum()
    assert dups == 0, f"Duplicados en ratings: {dups}"


def validate_movies() -> None:
    df = pd.read_csv(BASE / "movies.csv")
    assert df["movieId"].notna().all(), "movieId nulo"
    assert df["title"].notna().all(), "title nulo"


if __name__ == "__main__":
    validate_ratings()
    validate_movies()
    print("[DQ] OK")
