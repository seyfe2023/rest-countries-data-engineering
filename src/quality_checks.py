import os
from sqlalchemy import create_engine, text


def get_pg_engine():
    conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not conn:
        user = os.getenv("POSTGRES_USER", "airflow")
        pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
        db = os.getenv("POSTGRES_DB", "airflow")
        host = os.getenv("POSTGRES_HOST", "postgres")
        conn = f"postgresql+psycopg2://{user}:{pwd}@{host}/{db}"
    return create_engine(conn)


def run_quality_checks(min_rows: int = 200) -> None:
    """
    Simple, meaningful checks for a reference dataset like countries.
    """
    engine = get_pg_engine()
    with engine.begin() as conn:
        # Row count
        row_count = conn.execute(text("SELECT COUNT(*) FROM dim_country;")).scalar_one()
        if row_count < min_rows:
            raise ValueError(f"dim_country row_count too low: {row_count} < {min_rows}")

        # Primary key uniqueness
        dupes = conn.execute(
            text(
                """
                SELECT country_code, COUNT(*)
                FROM dim_country
                GROUP BY country_code
                HAVING COUNT(*) > 1;
                """
            )
        ).fetchall()
        if dupes:
            raise ValueError(f"Duplicate country_code found: {dupes[:5]}")

        # Not-null key check
        null_keys = conn.execute(
            text("SELECT COUNT(*) FROM dim_country WHERE country_code IS NULL OR country_code = '';")
        ).scalar_one()
        if null_keys > 0:
            raise ValueError(f"Null/empty country_code rows found: {null_keys}")
