from __future__ import annotations

import sqlite3
import pandas as pd
from .utils import ensure_dir
import os

def load_staging_to_sqlite(staging_clean: pd.DataFrame, sqlite_db: str) -> None:
    """
    Loads the staging_clean DataFrame into a SQLite database table named 'patient_visits'.
    If the database file does not exist, it will be created.
    """
    ensure_dir(os.path.dirname(sqlite_db) or ".")

    with sqlite3.connect(sqlite_db) as conn:
        staging_clean.to_sql(
            name="stg_patient_visits",
            con=conn,
            if_exists="replace",
            index=False
        )
    print(f"[load] Loaded {len(staging_clean)} rows into SQLite database at {sqlite_db}")

def build_analytics_tables(sqlite_db: str) -> None:
     """
        Builds analytics tables in the SQLite database from the staging table.
        Example: creates a summary table of total visit costs per patient.
        """
     with sqlite3.connect(sqlite_db) as conn:
        conn.execute("DROP TABLE IF EXISTS fact_visits;")
        conn.execute("""
                CREATE TABLE fact_visits AS
                SELECT
                visit_id,
                patient_id,
                provider_id,
                diagnosis_code,
                visit_date,
                visit_cost,
                last_updated
                FROM stg_patient_visits;
            """)
        conn.execute("DROP TABLE IF EXISTS daily_visit_metrics;")
        conn.execute("""
                CREATE TABLE daily_visit_metrics AS
                SELECT
                visit_date,
                COUNT(*) AS visit_count,
                AVG(visit_cost) AS avg_visit_cost
                FROM fact_visits
                GROUP BY visit_date;
            """)

print("[load] built analytics tables: fact_visits, daily_visit_metrics")

       