from __future__ import annotations

from .utils import load_config, ensure_dir
from .extract import extract_to_raw
from .transform import transform_raw_to_staging
from .load import load_staging_to_sqlite, build_analytics_tables
from .quality import run_quality_checks, write_dq_report
from .utils import read_json, write_json

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

    # Load previous state
    state_path = cfg["paths"]["state_json"]
    previous_state = read_json(state_path) or {}
    previous_row_count = previous_state.get("staging_clean_row_count")
    # Run data quality checks
    dq_cfg = cfg["etl"]["dq"]
    dq_result = run_quality_checks(
        df=tranformed["staging_clean"],
        required_cols=required_cols,
        dq_cfg=dq_cfg,
        previous_row_count=previous_row_count,
    )
    # Write DQ report
    dq_report_path = f"{staging_dir}/data_quality_report.json"
    write_dq_report(dq_result, dq_report_path)
    print(f"[run] Data quality report written to {dq_report_path}")
    # Update and save state
    new_state = {
        "staging_clean_row_count": int(len(tranformed["staging_clean"])),
        "dq_report_path": dq_report_path 
              }
    write_json(state_path, new_state)
    print(f"[run] Pipeline state updated at {state_path}")

    if not dq_result.passed:
        print("[run] Data quality checks failed. Aborting load step.")
        return

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
