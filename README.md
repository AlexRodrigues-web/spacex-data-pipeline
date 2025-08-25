Set-Location "C:\xampp\htdocs\spacex-data-pipeline"

@'
# SpaceX Launches — Data Pipeline (API → Postgres → dbt → Streamlit)

End-to-end pipeline that ingests **SpaceX API v4** into **Postgres** (`raw`), transforms with **dbt** (`analytics`), and optionally visualizes in **Streamlit**.

**Flow:** SpaceX API → `raw.spacex_launches` → `analytics.stg_spacex_launches` (VIEW) → `analytics.fct_spacex_launches_by_year` (TABLE)

---

## Stack
- Docker & Docker Compose
- Postgres 15
- dbt-postgres 1.7
- Python 3.11
- (Optional) Airflow 2.9 for orchestration
- (Optional) Streamlit dashboard

---

## Structure
spacex-data-pipeline/
├─ dbt/
│ ├─ profiles.yml
│ └─ models/
│ ├─ staging/
│ │ └─ stg_spacex_launches.sql
│ └─ marts/
│ └─ fct_spacex_launches_by_year.sql
├─ streamlit/ # optional
│ └─ app_spacex.py
├─ docker-compose.yml # optional if running standalone
├─ .env.sample # optional
└─ README.md

> If you reuse infra from another repo, keep only the dbt models here and point to the same database.


## Environment
Create `.env` from `.env.sample` (do not commit real secrets):
```env
DATABASE_URL=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
DBT_HOST=postgres
DBT_USER=airflow
DBT_PASSWORD=airflow
DBT_DBNAME=airflow
DBT_SCHEMA=analytics
How to Run
1 Load raw data (one-shot without Airflow)
Run this in one line on Windows:


docker compose exec streamlit bash -lc "python - << 'PY'
import os, requests, pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
DB_URL = os.getenv('DATABASE_URL','postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
engine = create_engine(DB_URL, pool_pre_ping=True)
resp = requests.get('https://api.spacexdata.com/v4/launches', timeout=60); resp.raise_for_status()
rows = []
for r in resp.json():
    rows.append({
        'launch_id': r.get('id'),
        'name': r.get('name'),
        'date_utc': r.get('date_utc'),
        'success': r.get('success'),
        'rocket': r.get('rocket'),
        'details': r.get('details'),
        'load_ts': datetime.utcnow()
    })
df = pd.DataFrame(rows)
with engine.begin() as conn:
    conn.execute(text('create schema if not exists raw'))
    df.to_sql('spacex_launches', conn, schema='raw', if_exists='replace', index=False)
print('Inserted rows:', len(df))
PY"
Check:


docker compose exec postgres psql -U airflow -d airflow -c "select count(*) from raw.spacex_launches;"
2 Run dbt models
bash

docker compose run --rm dbt run --full-refresh --select stg_spacex_launches+ --profiles-dir /usr/app --project-dir /usr/app
3 (Optional) Streamlit
If you added streamlit/app_spacex.py, open http://localhost:8501

Quick Checks

docker compose exec postgres psql -U airflow -d airflow -c "select * from analytics.fct_spacex_launches_by_year order by year desc limit 10;"
docker compose exec postgres psql -U airflow -d airflow -c "\dt analytics.*"
Data Schema
raw.spacex_launches

column	type	example
launch_id	text	5eb87d46ffd86e000604b388
name	text	Starlink-15 (v1.0)
date_utc	text (ISO)	2020-10-24T15:31:00.000Z
success	boolean	true/false/null
rocket	text	5e9d0d95eda69973a809d1ec
details	text	…
load_ts	timestamptz	2025-08-25 19:35:00+00

analytics.stg_spacex_launches (VIEW)

Cast date_utc → timestamp / date

Derive year

Normalize success if needed

analytics.fct_spacex_launches_by_year (TABLE)

column	type	description
year	int	launch year
launches	int	total launches per year
successes	int	success count
failures	int	failure count
success_rate_pct	numeric	successes / launches * 100

Troubleshooting
relation "raw.spacex_launches" does not exist → ingest raw first.

Windows line continuation → use a single line (don’t use \).

Odd success rate → very old launches may have success = null; adjust model logic if desired.

License
MIT — see LICENSE.
'@ | Set-Content -Encoding UTF8 README.md


---

## 2 (Opcional) Adicione um Streamlit básico

New-Item -ItemType Directory -Force -Path ".\streamlit" | Out-Null

@'
import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(page_title="SpaceX Launches", layout="wide")
st.title("🚀 SpaceX Launches — by Year")

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
engine = create_engine(DB_URL, pool_pre_ping=True)

@st.cache_data(ttl=60)
def load_fct():
    q = "select * from analytics.fct_spacex_launches_by_year order by year"
    return pd.read_sql(q, engine)

df = load_fct()
if df.empty:
    st.warning("No data yet. Run raw ingestion and dbt.")
    st.stop()

st.dataframe(df, use_container_width=True)

st.subheader("Launches per Year")
st.bar_chart(df, x="year", y="launches")

st.subheader("Success Rate (%)")
st.line_chart(df.set_index("year")["success_rate_pct"])
'@ | Set-Content -Encoding UTF8 .\streamlit\app_spacex.py