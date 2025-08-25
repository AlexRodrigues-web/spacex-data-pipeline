"""
DAG: spacex_api
Fetch SpaceX public API -> Postgres (schema raw).
"""
import os, logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("spacex_api")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"))
    logger.addHandler(h)

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
TARGET_SCHEMA = "raw"
TARGET_TABLE  = "spacex_launches"
API_URL = "https://api.spacexdata.com/v4/launches"

def _ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {TARGET_SCHEMA}"))

def extract_and_load(**_):
    start = datetime.utcnow()
    logger.info("Fetching: %s", API_URL)
    r = requests.get(API_URL, timeout=30, headers={"User-Agent": "dps-spacex/1.0"})
    r.raise_for_status()
    data = r.json()

    df = pd.json_normalize(data)[[
        "id","name","date_utc","success","flight_number","upcoming","details"
    ]].copy()

    df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce", utc=True).dt.tz_convert(None)
    df["success"] = df["success"].astype("boolean")
    df["upcoming"] = df["upcoming"].astype("boolean")
    df["flight_number"] = pd.to_numeric(df["flight_number"], errors="coerce").astype("Int64")
    df["load_ts"] = datetime.utcnow()

    engine = create_engine(DB_URL, pool_pre_ping=True)
    _ensure_schema(engine)
    with engine.begin() as conn:
        df.to_sql(TARGET_TABLE, conn, schema=TARGET_SCHEMA, if_exists="append", index=False)
    logger.info("Inserted %d rows into %s.%s", len(df), TARGET_SCHEMA, TARGET_TABLE)

default_args = {"owner":"data-pipeline-scraping","retries":1,"retry_delay":timedelta(minutes=2)}

with DAG(
    dag_id="spacex_api",
    description="SpaceX API -> Postgres raw",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["api","spacex","demo"]
) as dag:
    extract = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        provide_context=True,
    )