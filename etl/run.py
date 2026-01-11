from __future__ import annotations

from .utils import load_config, ensure_dir
from .extract import extract_to_raw
from .transform import transform_raw_to_staging
from .load import load_staging_to_sqlite, build_analytics_tables

def main() -> None:
    cfg = load_config()

    input_csv = cfg["paths"]["input_csv"]
    raw_dir = cfg["paths"]["raw_dir"]
    staging_dir = cfg["paths"]["staging_dir"]
    sqlite_db = cfg["paths"]["sqlite_db"]

    ensure_dir(staging_dir)
    required_cols = cfg["etl"]["required_columns"]
    dedupe_key = cfg["etl"]["dedupe_key"]
    dedupe_order_by = cfg["etl"]["dedupe_order_by"]
    raw_path = extract_to_raw(input_csv, raw_dir)

    tranformed = transform_raw_to_staging(
        raw_path=raw_path,
        required_cols=required_cols,
        dedupe_key=dedupe_key,
        dedupe_order_by=dedupe_order_by
    )

    # Write staging outputs as CSV artifacts
    staging_clean_path = f"{staging_dir}/staging_clean.csv"
    staging_rejects_path = f"{staging_dir}/staging_rejects.csv"
    tranformed["staging_clean"].to_csv(staging_clean_path, index=False)
    tranformed["staging_rejects"].to_csv(staging_rejects_path, index=False)
    print(f"[run] Wrote staging_clean to {staging_clean_path}")
    print(f"[run] Wrote staging_rejects to {staging_rejects_path}") 
    load_staging_to_sqlite(
        staging_clean=tranformed["staging_clean"],
        sqlite_db=sqlite_db
    )
    build_analytics_tables(sqlite_db=sqlite_db)
    print("[run] ETL process completed.")

if __name__ == "__main__":
    main()
    