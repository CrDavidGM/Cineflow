from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import date
from typing import Any, Dict, Optional, cast

from cineflow.utils.config import settings

st.set_page_config(page_title="CineFlow", layout="wide")
st.title("🎬 CineFlow Dashboard")

# Conexión a Postgres
url = (
    f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
    f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
)
engine: Engine = create_engine(url, pool_pre_ping=True)

# ---------- Helpers ----------

@st.cache_data(ttl=60)
def load_mv(since: Optional[date]) -> pd.DataFrame:
    sql = "SELECT * FROM mv_daily_metrics"
    params: Dict[str, Any] = {}
    if since is not None:
        sql += " WHERE rating_date >= :since"
        params["since"] = since 
    sql += " ORDER BY rating_date"
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
        return cast(pd.DataFrame, df)

@st.cache_data(ttl=60)
def load_top_movies(limit: int, min_votes: int, since: Optional[date]) -> pd.DataFrame:
    where = ""
    params: Dict[str, Any] = {"limit": limit, "min_votes": min_votes}
    if since is not None:
        where = "WHERE f.rating_date >= :since"
        params["since"] = since
    sql = f"""
    SELECT m.title,
           COUNT(f.user_id) AS ratings,
           ROUND(AVG(f.rating)::numeric,2) AS avg_rating
    FROM fact_rating f
    JOIN dim_movie m ON m.movie_id = f.movie_id
    {where}
    GROUP BY m.title
    HAVING COUNT(f.user_id) >= :min_votes
    ORDER BY avg_rating DESC, ratings DESC
    LIMIT :limit
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
        return cast(pd.DataFrame, df)

@st.cache_data(ttl=60)
def load_top_genres(limit: int, min_votes: int, since: Optional[date]) -> pd.DataFrame:
    where = ""
    params: Dict[str, Any] = {"limit": limit, "min_votes": min_votes}
    if since is not None:
        where = "WHERE f.rating_date >= :since"
        params["since"] = since
    sql = f"""
    SELECT unnest(string_to_array(m.genres, '|')) AS genre,
           COUNT(f.rating) AS total_ratings,
           ROUND(AVG(f.rating)::numeric, 2) AS avg_rating
    FROM fact_rating f
    JOIN dim_movie m ON m.movie_id = f.movie_id
    {where}
    GROUP BY genre
    HAVING COUNT(f.rating) >= :min_votes
    ORDER BY avg_rating DESC, total_ratings DESC
    LIMIT :limit
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
        return cast(pd.DataFrame, df)
