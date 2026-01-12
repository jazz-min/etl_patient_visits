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

   
    # Load previous state
    state_path = cfg["paths"]["state_json"]
    previous_state = read_json(state_path) or {}
    previous_row_count = previous_state.get("last_staging_row_count")
    #Read watermark value
    watermark_value = previous_state.get("watermark_last_updated")
    #Read incremental config
    incremental_cfg = cfg["etl"].get("incremental", {})
    incremental_enabled = incremental_cfg.get("enabled", False)
    watermark_column = incremental_cfg.get("watermark_column", "last_updated")
    lookback_hours = incremental_cfg.get("lookback_hours", 24)
    if incremental_enabled:
        print(f"[run] Incremental mode enabled with watermark_column={watermark_column} watermark_value={watermark_value} lookback_hours={lookback_hours}")
    else:
        watermark_value = None
        print(f"[run] Incremental mode disabled; processing full dataset.")


    transformed = transform_raw_to_staging(
        raw_path=raw_path,
        required_cols=required_cols,
        dedupe_key=dedupe_key,
        dedupe_order_by=dedupe_order_by,
        watermark_column=watermark_column,
        watermark_value=watermark_value if incremental_enabled else None,
        lookback_hours=lookback_hours,
    )
    # Run data quality checks
    dq_cfg = cfg["etl"]["dq"]
    dq_result = run_quality_checks(
        df=transformed["staging_clean"],
        required_cols=required_cols,
        dq_cfg=dq_cfg,
        previous_row_count=previous_row_count,
    )
    # Write DQ report
    dq_report_path = f"{staging_dir}/data_quality_report.json"
    write_dq_report(dq_result, dq_report_path)
    print(f"[run] Data quality report written to {dq_report_path}")

    if not dq_result.passed:
        print("[run] Data quality checks failed. Aborting load step.")
        return

    # Write staging outputs as CSV artifacts
    staging_clean_path = f"{staging_dir}/staging_clean.csv"
    staging_rejects_path = f"{staging_dir}/staging_rejects.csv"
    transformed["staging_clean"].to_csv(staging_clean_path, index=False)
    transformed["staging_rejects"].to_csv(staging_rejects_path, index=False)
    print(f"[run] Wrote staging_clean to {staging_clean_path}")
    print(f"[run] Wrote staging_rejects to {staging_rejects_path}") 
    load_staging_to_sqlite(
        staging_clean=transformed["staging_clean"],
        sqlite_db=sqlite_db
    )
    build_analytics_tables(sqlite_db=sqlite_db)
    # Update and write state
    new_wm = transformed.get("new_watermark")
    state_update = {"last_staging_row_count": int(len(transformed["staging_clean"]))}
    if new_wm:
        state_update["watermark_last_updated"] = str(new_wm)
    write_json(state_path, state_update)
    print(f"[state] updated {state_path} -> {state_update}")


    print("[run] ETL process completed.")

if __name__ == "__main__":
    main()
