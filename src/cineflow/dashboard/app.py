from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional, cast

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from cineflow.utils.config import settings

st.set_page_config(page_title="CineFlow", layout="wide")
st.title("🎬 CineFlow Dashboard")

# ---------- Conexión a Postgres ----------
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


# ---------- Controles globales ----------
with st.sidebar:
    st.header("Filtros")
    since_enabled = st.checkbox("Filtrar por fecha mínima", value=False)
    since = st.date_input(
        "Desde",
        value=date(1997, 1, 1),
        format="YYYY-MM-DD",
        disabled=not since_enabled,
    )
    since_val: Optional[date] = since if since_enabled else None

    min_votes = st.slider("Mínimo de votos", 1, 200, 10)
    st.caption("Se ignoran películas/géneros con menos votos que este umbral.")

tab1, tab2, tab3 = st.tabs(["📈 Métricas diarias", "🎬 Top Películas", "🎭 Top Géneros"])

# ---------- Tab 1: Métricas diarias ----------
with tab1:
    st.subheader("Tendencia diaria")
    mv = load_mv(since_val)
    if mv.empty:
        st.info("No hay datos para el rango seleccionado.")
    else:
        st.line_chart(
            mv.set_index("rating_date")[["ratings_cnt", "users_active", "avg_rating"]]
        )
        st.dataframe(mv.tail(20), use_container_width=True)

# ---------- Tab 2: Top Películas ----------
with tab2:
    colA, colB = st.columns([3, 1], gap="large")
    with colB:
        limit_movies = st.slider("Límite", 5, 50, 15)
    df_movies = load_top_movies(limit_movies, min_votes, since_val)
    if df_movies.empty:
        st.warning("Sin resultados con los filtros actuales.")
    else:
        st.dataframe(df_movies, use_container_width=True)
        st.bar_chart(df_movies.set_index("title")["avg_rating"])

# ---------- Tab 3: Top Géneros ----------
with tab3:
    colC, colD = st.columns([3, 1], gap="large")
    with colD:
        limit_genres = st.slider("Límite géneros", 5, 30, 10, key="g")
    df_genres = load_top_genres(limit_genres, min_votes, since_val)
    if df_genres.empty:
        st.warning("Sin resultados con los filtros actuales.")
    else:
        st.dataframe(df_genres, use_container_width=True)
        st.bar_chart(df_genres.set_index("genre")["avg_rating"])
