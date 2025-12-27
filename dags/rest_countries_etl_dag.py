from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator 

# Import your ETL functions (from /opt/airflow/src inside the container)
from src.extract import extract_payload
from src.transform import transform_to_dim_country
from src.load import load_raw_payload, upsert_dim_country, get_pg_engine
from src.quality_checks import run_quality_checks


def create_tables():
    """
    Executes sql/create_tables.sql inside the container.
    """
    sql_path = Path("/opt/airflow/sql/create_tables.sql")
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    engine = get_pg_engine()
    sql = sql_path.read_text(encoding="utf-8")

    # Run multiple statements
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(stmt + ";")


def extract_task(ti):
    payload = extract_payload()
    # push to XCom (keep it small-ish: store only data, not too big metadata)
    ti.xcom_push(key="countries_payload", value=payload)


def transform_task(ti):
    payload = ti.xcom_pull(task_ids="extract", key="countries_payload")
    raw_data = payload["data"]
    df = transform_to_dim_country(raw_data)

    # Convert DataFrame to records for XCom (safe for this dataset size)
    ti.xcom_push(key="dim_records", value=df.to_dict(orient="records"))
    ti.xcom_push(key="raw_payload", value=payload)


def load_task(ti):
    payload = ti.xcom_pull(task_ids="transform", key="raw_payload")
    dim_records = ti.xcom_pull(task_ids="transform", key="dim_records")

    # Load raw JSON payload
    load_raw_payload(payload)

    # Load curated table
    import pandas as pd
    df = pd.DataFrame(dim_records)
    upsert_dim_country(df)


default_args = {
    "owner": "seyfe",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="rest_countries_etl",
    description="ETL pipeline: REST Countries API -> Postgres (staging + curated) with quality checks",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    tags=["portfolio", "data-engineering", "etl", "restcountries"],
) as dag:

    t_create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    t_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    t_create_tables >> t_extract >> t_transform >> t_load >> t_quality
