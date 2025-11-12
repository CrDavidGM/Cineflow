from fastapi import FastAPI, Query
from sqlalchemy import text
from cineflow.storage.postgres_client import get_engine

app = FastAPI(title="CineFlow API")

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/movies/top")
def top_movies(limit: int = Query(10, ge=1, le=100)) -> dict[str, list[dict[str, object]]]:
    engine = get_engine()
    sql = text("""
        SELECT m.movie_id, m.title, m.genres,
               COUNT(f.user_id) AS ratings,
               ROUND(AVG(f.rating)::numeric,2) AS avg_rating
        FROM fact_rating f
        JOIN dim_movie m ON m.movie_id = f.movie_id
        GROUP BY m.movie_id, m.title, m.genres
        HAVING COUNT(f.user_id) >= 10
        ORDER BY avg_rating DESC, ratings DESC
        LIMIT :limit
    """)
    with engine.begin() as c:
        rows = c.execute(sql, {"limit": limit}).mappings().all()
    return {"items": [dict(r) for r in rows]}

@app.get("/metrics/daily")
def metrics_daily(since: str | None = None) -> dict[str, list[dict[str, object]]]:
    engine = get_engine()
    base = "SELECT * FROM mv_daily_metrics"
    if since:
        base += " WHERE rating_date >= :since"
    base += " ORDER BY rating_date"
    with engine.begin() as c:
        rows = c.execute(text(base), {"since": since} if since else {}).mappings().all()
    return {"items": [dict(r) for r in rows]}

@app.get("/genres/top")
def top_genres(limit: int = Query(10, ge=1, le=50)) -> dict[str, list[dict[str, object]]]:
    sql = text("""
        SELECT unnest(string_to_array(m.genres, '|')) AS genre,
               COUNT(f.rating) AS total_ratings,
               ROUND(AVG(f.rating)::numeric, 2) AS avg_rating
        FROM fact_rating f
        JOIN dim_movie m ON m.movie_id = f.movie_id
        GROUP BY genre
        HAVING COUNT(f.rating) >= 10
        ORDER BY avg_rating DESC, total_ratings DESC
        LIMIT :limit
    """)
    engine = get_engine()
    with engine.begin() as c:
        rows = c.execute(sql, {"limit": limit}).mappings().all()
    return {"items": [dict(r) for r in rows]}
