from sqlalchemy import text
from cineflow.storage.postgres_client import get_engine

def create_indexes_and_views() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_rating_date ON fact_rating(rating_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_rating_movie ON fact_rating(movie_id);"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_metrics AS
            SELECT
              rating_date,
              COUNT(*)::int AS ratings_cnt,
              COUNT(DISTINCT user_id)::int AS users_active,
              ROUND(AVG(rating)::numeric, 3) AS avg_rating
            FROM fact_rating
            GROUP BY rating_date
            ORDER BY rating_date;
        """))

if __name__ == "__main__":
    create_indexes_and_views()
    print("[PG] Índices y MV creados")
