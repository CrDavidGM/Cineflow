"""Transforma datos crudos en Mongo y los carga a Postgres (modelo simple)."""
import pandas as pd
from sqlalchemy import text
from cineflow.storage.mongo_client import get_mongo
from cineflow.storage.postgres_client import get_engine, init_schema

def main() -> None:
    # 1) leer raw de Mongo
    _, db = get_mongo()
    ratings = pd.DataFrame(list(db.ratings_raw.find({}, {"_id": 0})))
    movies = pd.DataFrame(list(db.movies_raw.find({}, {"_id": 0})))

    # 2) validaciones mínimas
    assert {"userId","movieId","rating","timestamp"} <= set(ratings.columns), "ratings.csv columnas inválidas"
    assert {"movieId","title","genres"} <= set(movies.columns), "movies.csv columnas inválidas"

    # 3) transformar
    ratings["rating_date"] = pd.to_datetime(ratings["timestamp"], unit="s").dt.date

    # 4) cargar a Postgres
    init_schema()
    engine = get_engine()
    with engine.begin() as conn:
        # --- dim_movie ---
        for _, r in movies.iterrows():
            conn.execute(text("""
                INSERT INTO dim_movie(movie_id, title, genres)
                VALUES (:id, :title, :genres)
                ON CONFLICT (movie_id)
                DO UPDATE SET title = EXCLUDED.title, genres = EXCLUDED.genres;
            """), {"id": int(r.movieId), "title": r.title, "genres": r.genres})

        # --- dim_user ---
        for uid in ratings["userId"].unique():
            conn.execute(text("""
                INSERT INTO dim_user(user_id)
                VALUES (:uid)
                ON CONFLICT (user_id) DO NOTHING;
            """), {"uid": int(uid)})

        # --- fact_rating ---
        ratings.rename(columns={
            "userId": "user_id", "movieId": "movie_id", "timestamp": "rating_ts"
        }, inplace=True)
        for _, r in ratings.iterrows():
            conn.execute(text("""
                INSERT INTO fact_rating(user_id, movie_id, rating, rating_ts, rating_date)
                VALUES (:u, :m, :r, :ts, :d)
                ON CONFLICT (user_id, movie_id, rating_ts) DO NOTHING;
            """), {"u": int(r.user_id), "m": int(r.movie_id),
                "r": float(r.rating), "ts": int(r.rating_ts), "d": r.rating_date})
    print("Carga a Postgres completada.")

if __name__ == "__main__":
    main()
