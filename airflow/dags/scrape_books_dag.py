"""
DAG: scrape_books
Scraping do site de teste 'Books to Scrape' -> Postgres (schema raw).
Salva o preço CRU (ex.: '£51.77') para limpeza posterior no dbt (stg_books).
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- LOGGING ----------
logger = logging.getLogger("scrape_books")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"))
    logger.addHandler(_h)

# ---------- CONFIG ----------
DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
TARGET_SCHEMA = "raw"
TARGET_TABLE  = "books"

BASE_URL   = "https://books.toscrape.com/"
START_PATH = "catalogue/page-1.html"          # começa no catálogo
MAX_PAGES  = int(os.getenv("BOOKS_PAGES", "5"))  # quantas páginas raspar (p/ demo)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; dps-scraper/1.0; +https://example.local)"
}
TIMEOUT = 30

# ---------- HELPERS ----------
def _ensure_schema_and_table(engine):
    """Garante schema e tabela com colunas esperadas (price como TEXT cru)."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};
    CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
        title       TEXT,
        price_gbp   TEXT,           -- preço CRU (ex.: '£51.77')
        rating_txt  TEXT,           -- 'One'..'Five'
        load_ts     TIMESTAMPTZ DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Schema/tabela verificados: %s.%s", TARGET_SCHEMA, TARGET_TABLE)

def _parse_card(card):
    """Extrai campos de um card de produto."""
    # título
    a = card.select_one("h3 a")
    title = (a.get("title") or a.get_text()).strip() if a else None

    # preço CRU (mantém '£' etc.)
    price_tag = card.select_one("p.price_color")
    price_raw = price_tag.get_text(strip=True) if price_tag else None  # ex.: '£51.77'

    # rating: classe 'star-rating Three' -> 'Three'
    rating_tag = card.select_one("p.star-rating")
    rating_txt = None
    if rating_tag:
        classes = rating_tag.get("class", [])
        rating_txt = next((c for c in classes if c != "star-rating"), None)

    return {"title": title, "price_gbp": price_raw, "rating_txt": rating_txt}

def _scrape_pages():
    """Percorre páginas do catálogo e retorna lista de dicts com os campos extraídos."""
    url = urljoin(BASE_URL, START_PATH)
    all_rows = []

    for page in range(1, MAX_PAGES + 1):
        logger.info("Baixando página %s: %s", page, url)
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("article.product_pod")
        page_rows = [_parse_card(c) for c in cards]
        all_rows.extend(page_rows)

        # próximo link (relativo ao catálogo)
        next_a = soup.select_one("li.next a")
        if not next_a:
            break
        url = urljoin(url, next_a.get("href"))

    logger.info("Total de itens extraídos: %d", len(all_rows))
    return all_rows

def extract_and_load(**_):
    """Extrai dados e carrega no Postgres (append)."""
    start = datetime.now(timezone.utc)
    logger.info("Início da extração: %s", start.isoformat())

    rows = _scrape_pages()
    if not rows:
        logger.warning("Nenhum item extraído. Nada a carregar.")
        return

    # acrescenta load_ts
    now = datetime.now(timezone.utc)
    for r in rows:
        r["load_ts"] = now

    df = pd.DataFrame(rows, columns=["title", "price_gbp", "rating_txt", "load_ts"])

    engine = create_engine(DB_URL, pool_pre_ping=True)
    _ensure_schema_and_table(engine)

    with engine.begin() as conn:
        # append sem índices
        df.to_sql(
            TARGET_TABLE, con=conn, schema=TARGET_SCHEMA,
            if_exists="append", index=False
        )
    logger.info("Carga concluída em %s.%s (%d linhas).", TARGET_SCHEMA, TARGET_TABLE, len(df))

    end = datetime.now(timezone.utc)
    logger.info("Fim da tarefa. Duração (s): %.2f", (end - start).total_seconds())

# ---------- DAG ----------
default_args = {
    "owner": "data-pipeline-scraping",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="scrape_books",
    description="Scrape Books to Scrape -> Postgres raw (preço CRU) com logging.",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["scraping", "demo", "logging"],
) as dag:
    extract = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )
