from __future__ import annotations
import os
import yaml
from datetime import datetime, timezone

def load_config(path: str = "config.yaml") -> dict:
    """Load configuration from a YAML file."""
    with open(path, "r",encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return config

def ensure_dir(path: str) -> None:
    """Ensure that a directory exists; create it if it does not."""
    os.makedirs(path, exist_ok=True)

def utc_now_compact() -> str:
    """Get the current UTC time in a compact string format."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

