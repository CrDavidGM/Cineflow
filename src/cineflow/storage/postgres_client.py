from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from cineflow.utils.config import settings


def get_engine() -> Engine:
    url = f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
    return create_engine(url, pool_pre_ping=True)


def init_schema() -> None:
    engine = get_engine()
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_movie (
        movie_id INTEGER PRIMARY KEY,
        title TEXT,
        genres TEXT
    );
    CREATE TABLE IF NOT EXISTS dim_user (
        user_id INTEGER PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS fact_rating (
        user_id INTEGER,
        movie_id INTEGER,
        rating REAL,
        rating_ts BIGINT,
        rating_date DATE,
        PRIMARY KEY (user_id, movie_id, rating_ts)
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
