from __future__ import annotations

import pandas as pd
from .utils import ensure_dir, utc_now_compact

def extract_to_raw(input_csv: str, raw_dir: str) -> str:
    """ Reads input CSV and writes an immutable raw snapshot into raw_dir.
    Args:
        input_csv (str): Path to the input CSV file.
        raw_dir (str): Directory to save the raw data.

    Returns:
        str: Path to the saved raw data file.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv)

    # Ensure the raw data directory exists
    ensure_dir(raw_dir)

    # Create a timestamped filename for the raw data
    snapshot_name = f"patient_visits_raw_{utc_now_compact()}.csv"
    raw_file_path = f"{raw_dir}/{snapshot_name}"

    # Save the DataFrame to the raw data directory
    df.to_csv(raw_file_path, index=False)

    print(f"[extract] read_rows={len(df)} raw_written={raw_file_path}")

    return raw_file_path