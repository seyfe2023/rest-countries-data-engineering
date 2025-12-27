import os
import json
from typing import Dict
from sqlalchemy import text
import pandas as pd
from sqlalchemy import create_engine, text


def get_pg_engine():
    """
    Create SQLAlchemy engine using env var AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    or fallback to standard Postgres settings.
    """
    conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not conn:
        user = os.getenv("POSTGRES_USER", "airflow")
        pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
        db = os.getenv("POSTGRES_DB", "airflow")
        host = os.getenv("POSTGRES_HOST", "postgres")
        conn = f"postgresql+psycopg2://{user}:{pwd}@{host}/{db}"
    return create_engine(conn)


def load_raw_payload(payload: dict) -> None:
    """
    Load raw payload into stg_countries_raw as JSONB.
    """
    engine = get_pg_engine()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO stg_countries_raw (source, payload) VALUES (:source, CAST(:payload AS jsonb))"),
            {
                "source": payload.get("source", "restcountries"),
                "payload": json.dumps(payload),
            },
        )


def upsert_dim_country(df: pd.DataFrame) -> None:
    """
    Upsert curated dimension table dim_country.
    """
    if df is None or df.empty:
        raise ValueError("Transformed dataframe is empty, nothing to load.")

    engine = get_pg_engine()

    # Load into a temp table then upsert
    temp_table = "tmp_dim_country"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    upsert_sql = f"""
    INSERT INTO dim_country (
      country_code, name_common, name_official, region, subregion, capital,
      population, area, independent, landlocked, flag_png, updated_at
    )
    SELECT
      country_code, name_common, name_official, region, subregion, capital,
      population::bigint, area::double precision, independent::boolean, landlocked::boolean, flag_png, NOW()
    FROM {temp_table}
    ON CONFLICT (country_code) DO UPDATE SET
      name_common   = EXCLUDED.name_common,
      name_official = EXCLUDED.name_official,
      region        = EXCLUDED.region,
      subregion     = EXCLUDED.subregion,
      capital       = EXCLUDED.capital,
      population    = EXCLUDED.population,
      area          = EXCLUDED.area,
      independent   = EXCLUDED.independent,
      landlocked    = EXCLUDED.landlocked,
      flag_png      = EXCLUDED.flag_png,
      updated_at    = NOW();
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
