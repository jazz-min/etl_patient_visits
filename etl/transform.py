from __future__ import annotations

import pandas as pd

def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
        # Normalize visit_date to ISO date; allow multiple input formats
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce", utc=True)
    return df

def _type_cast(df: pd.DataFrame) -> pd.DataFrame:
    df["visit_cost"] = pd.to_numeric(df["visit_cost"], errors="coerce")
    return df

def _validate_required(df: pd.DataFrame, required_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (good_rows, bad_rows) based on required column presence (non-null).
    """
    missing_required = df[required_cols].isnull().any(axis=1)
    bad = df[missing_required].copy()
    good = df[~missing_required].copy()
    return good, bad

def _dedupe_latest(df: pd.DataFrame, key: str, order_by: str) -> pd.DataFrame:
    """
    Keep the latest record per key using order_by timestamp.
    """
    df = df.sort_values(by=[key, order_by], ascending=[True, False])
    deduped = df.drop_duplicates(subset=[key], keep="first")
    return deduped

def transform_raw_to_staging(raw_path: str, required_cols: list[str], dedupe_key: str, dedupe_order_by: str) -> dict:
    """
    Reads raw snapshot and produces:
      - staging_clean (validated + deduped)
      - staging_rejects (failed validation)
    Returns dict with dataframes.
    """
    df = pd.read_csv(raw_path)

     # Normalize & cast
    df = _normalize_dates(df)
    df = _type_cast(df)

    # Validation: required columns non-null AFTER normalization/casting
    good, bad = _validate_required(df, required_cols)

    # Deduplication: keep latest per visit_id
    good = _dedupe_latest(good, key=dedupe_key, order_by=dedupe_order_by)
    print(f"[transform] raw_rows={len(df)} good_rows={len(good)} bad_rows={len(bad)}")

    return {
        "staging_clean": good,
        "staging_rejects": bad
    }
