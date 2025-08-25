"""
Streamlit dashboard – Books & SpaceX Analytics
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# ---------- LOGGING ----------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "streamlit.log")

logger = logging.getLogger("streamlit_app")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    ch = logging.StreamHandler()
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s - %(message)s")
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)

# ---------- CONFIG ----------
DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
st.set_page_config(page_title="Data Analytics — Demo", layout="wide")

# ---------- DB HELPERS ----------
@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)

# BOOKS
@st.cache_data(show_spinner=False, ttl=60)
def load_books(limit=5000):
    logger.info("Carregando BOOKS do DB...")
    engine = get_engine()
    query = """
        select title, price_gbp, rating, load_date
        from analytics.dim_books
        order by load_date desc, title asc
        limit %(lim)s
    """
    df = pd.read_sql(query, engine, params={"lim": int(limit)})
    df["price_gbp"] = pd.to_numeric(df["price_gbp"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype("Int64")
    logger.info("BOOKS linhas: %d", len(df))
    return df

# SPACEX
@st.cache_data(show_spinner=False, ttl=300)
def load_spacex(limit=1000):
    logger.info("Carregando SPACEX do DB...")
    engine = get_engine()
    query = """
        select year, launches, successes, failures, success_rate_pct
        from analytics.fct_spacex_launches_by_year
        order by year asc
        limit %(lim)s
    """
    df = pd.read_sql(query, engine, params={"lim": int(limit)})
    logger.info("SPACEX anos: %d", len(df))
    return df

# ---------- UI ROOT ----------
dataset = st.sidebar.radio("Dataset", ["Books", "SpaceX"], index=0)

if dataset == "Books":
    st.title("📚 Books Analytics — Demo")

    try:
        df = load_books()
        if df.empty:
            st.warning("Sem dados em `analytics.dim_books`. Rode o DAG e depois `dbt run`.")
            st.stop()

        ratings_present = sorted([int(x) for x in df["rating"].dropna().unique().tolist()])
        col1, col2 = st.columns(2)
        with col1:
            rating_sel = st.multiselect("Filtrar por rating", options=[1,2,3,4,5], default=ratings_present or [1,2,3,4,5])
        with col2:
            valid_prices = df["price_gbp"].dropna()
            if valid_prices.empty:
                st.warning("Não há preços válidos na base ainda (todos NULL).")
                price_range = (0.0, 100.0)
            else:
                vmin, vmax = float(valid_prices.min()), float(valid_prices.max())
                if vmin == vmax: vmin = 0.0
                price_range = st.slider("Faixa de preço (GBP)", min_value=vmin, max_value=vmax, value=(vmin, vmax))

        mask = df["rating"].isin(rating_sel) & df["price_gbp"].between(price_range[0], price_range[1], inclusive="both")
        filtered = df[mask].copy()

        st.subheader("Tabela")
        if filtered.empty:
            st.info("Sem dados para esses filtros. Ajuste rating e/ou faixa de preço.")
            st.dataframe(df, use_container_width=True)
            st.stop()
        st.dataframe(filtered, use_container_width=True)

        st.subheader("Preço médio por rating")
        mean_df = (filtered.dropna(subset=["price_gbp","rating"])
                          .groupby("rating", dropna=False)["price_gbp"]
                          .mean().reset_index().sort_values("rating"))
        if not mean_df.empty:
            st.bar_chart(mean_df, x="rating", y="price_gbp")
        else:
            st.info("Não há valores de preço para calcular o gráfico.")
        st.caption("Dica: atualize o DAG diariamente para novos dados. Logs em `logs/streamlit.log`.")
    except Exception as e:
        st.error("Falha ao carregar/exibir dados (Books).")
        st.exception(e)

else:
    st.title("🚀 SpaceX — Launches by Year")

    try:
        df = load_spacex()
        if df.empty:
            st.warning("Sem dados em `analytics.fct_spacex_launches_by_year`. Rode a ingestão e `dbt run`.")
            st.stop()

        # KPIs
        tot_launches = int(df["launches"].sum())
        tot_success = int(df["successes"].sum())
        rate = round(100.0 * tot_success / tot_launches, 2) if tot_launches else 0.0
        col1, col2, col3 = st.columns(3)
        col1.metric("Total launches", f"{tot_launches}")
        col2.metric("Total successes", f"{tot_success}")
        col3.metric("Success rate", f"{rate}%")

        st.subheader("Tabela")
        st.dataframe(df.sort_values("year", ascending=False), use_container_width=True)

        st.subheader("Lançamentos por ano")
        st.line_chart(df, x="year", y=["launches", "successes"])

        st.subheader("Taxa de sucesso (%) por ano")
        st.bar_chart(df, x="year", y="success_rate_pct")

        st.caption("Fonte: SpaceX v4 API → raw.spacex_launches → dbt → analytics.fct_spacex_launches_by_year.")
    except Exception as e:
        st.error("Falha ao carregar/exibir dados (SpaceX).")
        st.exception(e)
