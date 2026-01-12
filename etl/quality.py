from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import json
import os

import pandas as pd


@dataclass
class DQResult:
    passed: bool
    checks: list[dict[str, Any]]
    metrics: dict[str, Any]

def _null_rate(df: pd.DataFrame, col: str) -> float:
    if len(df) == 0:
        return 0.0
    return float(df[col].isna().mean())

def _parseable_date_rate(df: pd.DataFrame, col: str) -> float:
    if len(df) == 0:
        return 1.0
    parsed = pd.to_datetime(df[col], errors="coerce")
    return float(parsed.notna().mean())

def run_quality_checks(
    df: pd.DataFrame,
    required_cols: list[str],
    dq_cfg: dict[str, Any],
    previous_row_count: int | None,
) -> DQResult:
    checks: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {"row_count": int(len(df))}

    # 1) Row count minimum (critical)
    min_rows = dq_cfg["fail_on"]["row_count_lt"]
    row_count_ok = len(df) >= min_rows
    checks.append({
        "name": "row_count_min",
        "severity": "FAIL",
        "passed": row_count_ok,
        "details": {"row_count": int(len(df)), "min_rows": min_rows},
    })

    # 2) Required columns null rate (critical)
    req_null_rates = {c: _null_rate(df, c) for c in required_cols}
    metrics["required_null_rates"] = req_null_rates
    req_ok = all(rate <= dq_cfg["fail_on"]["required_null_rate_gt"] for rate in req_null_rates.values())
    checks.append({
        "name": "required_null_rate",
        "severity": "FAIL",
        "passed": req_ok,
        "details": {"threshold": dq_cfg["fail_on"]["required_null_rate_gt"], "rates": req_null_rates},
    })

    # 3) visit_date parseable rate (critical)
    parse_rate = _parseable_date_rate(df, "visit_date")
    invalid_rate = 1.0 - parse_rate
    metrics["visit_date_parse_rate"] = parse_rate
    checks.append({
        "name": "visit_date_invalid_rate",
        "severity": "FAIL",
        "passed": invalid_rate <= dq_cfg["fail_on"]["invalid_visit_date_rate_gt"],
        "details": {"invalid_rate": invalid_rate, "threshold": dq_cfg["fail_on"]["invalid_visit_date_rate_gt"]},
    })

    # 4) visit_cost null rate (warn)
    cost_null_rate = _null_rate(df, "visit_cost")
    metrics["visit_cost_null_rate"] = cost_null_rate
    checks.append({
        "name": "visit_cost_null_rate",
        "severity": "WARN",
        "passed": cost_null_rate <= dq_cfg["warn_on"]["visit_cost_null_rate_gt"],
        "details": {"null_rate": cost_null_rate, "threshold": dq_cfg["warn_on"]["visit_cost_null_rate_gt"]},
    })

    # 5) Row-count drop compared to previous run (warn)
    if previous_row_count is not None and previous_row_count > 0:
        drop_pct = (previous_row_count - len(df)) / previous_row_count
        metrics["row_count_drop_pct"] = drop_pct
        checks.append({
            "name": "row_count_drop_pct",
            "severity": "WARN",
            "passed": drop_pct <= dq_cfg["row_count_drop_warn_pct"],
            "details": {"drop_pct": drop_pct, "threshold": dq_cfg["row_count_drop_warn_pct"]},
    })
        
    # Determine overall pass: all FAIL checks must pass
    fail_checks_ok = all(c["passed"] for c in checks if c["severity"] == "FAIL")
    return DQResult(passed=fail_checks_ok, checks=checks, metrics=metrics)

def write_dq_report(report: DQResult, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "passed": report.passed,
        "metrics": report.metrics,
        "checks": report.checks,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)