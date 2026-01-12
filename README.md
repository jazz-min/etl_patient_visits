# Patient Visits ELT Pipeline (Python)

## Overview

This project implements a **modern ELT-style data pipeline** using Python and SQL concepts.
It ingests raw patient visit data, enforces data quality rules, supports **incremental loading with watermarks**, and produces **analytics-ready tables**.

The pipeline is intentionally simple but designed with **production-grade patterns**:

- Immutable raw data
- Layered architecture
- Idempotent re-runs
- Data quality gates
- Incremental processing with late-arriving data handling

This repository is intended as a **demonstration project**.

---

## Architecture

```
Source
  |
  v
RAW Layer (immutable snapshots)
  |
  v
STAGING Layer (clean, validate, dedupe)
  |
  v
ANALYTICS Layer (facts & aggregates)
```

### Design Principles

- **ELT over ETL**: raw data is loaded first, transformations happen downstream
- **Separation of concerns**: each layer has a single responsibility
- **Idempotency**: safe re-runs and retries
- **Observability**: row counts, quality reports, and state tracking

---

## Data Layers

### Raw Layer

- Immutable, append-only snapshots
- Preserves source data exactly as received
- Used for auditing, debugging, and reprocessing

### Staging Layer

- Type normalization
- Required-field validation
- Deduplication by business key
- Quarantines invalid records instead of silently dropping them

### Analytics Layer

- Fact table: `fact_visits`
- Aggregate table: `daily_visit_metrics`
- Designed for BI and downstream ML use

---

## Incremental Loading with Watermarks

The pipeline supports **incremental processing** using a timestamp watermark (`last_updated`).

### How It Works

1. The pipeline stores the latest processed `last_updated` value in a state file
2. On the next run, only records newer than:

```
watermark - lookback_window
```
will be processed.

3. Deduplication ensures idempotency

### Why a Lookback Window?

Late-arriving or out-of-order data is common in real systems.
A configurable lookback window (default: 24 hours) trades minimal reprocessing for correctness.

### State File Example

```json
{
  "last_staging_row_count": 120,
  "watermark_last_updated": "2025-12-06T12:00:00+00:00"
}
```

---

## Data Quality Checks

Data quality is enforced **before loading analytics tables**.

### Critical (Fail Pipeline)

- Required fields missing
- Invalid `visit_date` above threshold
- Zero-row loads

### Non-Critical (Warn)

- Excessive nulls in `visit_cost`
- Large row-count drops compared to previous run
- Stale data beyond freshness threshold

### Output

Each run generates a machine-readable report:

```
data/staging/dq_report.json
```

---

## Project Structure

```
etl_project/
├── data/
│   ├── input/
│   ├── raw/
│   ├── staging/
│   ├── state/
│   └── warehouse.db
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── quality.py
│   ├── utils.py
│   └── run.py
├── config.yaml
├── requirements.txt
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.9+

### Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
python -m etl.run
```

### Outputs

- Raw snapshot CSVs
- Clean & rejected staging datasets
- Data quality report (`dq_report.json`)
- SQLite warehouse with analytics tables

---

## Example Analytics Query

```sql
SELECT *
FROM daily_visit_metrics
ORDER BY visit_date;
```

---

## Design Tradeoffs

- SQLite is used as a lightweight warehouse stand-in for demonstration purposes
- CSV input simulates upstream systems (APIs or operational databases)
- No orchestration tool is used to keep focus on core pipeline logic


---

## How This Maps to Cloud Warehouses

In a production environment:

- Raw and staging layers would live in cloud object storage
- Transformations would run in BigQuery or Snowflake using SQL
- Incremental loads would use `MERGE` statements
- Data quality checks would integrate with monitoring and alerting systems

---

## Key Concepts Demonstrated

- ELT pipeline design
- Incremental loading with watermarks
- Handling late-arriving data
- Idempotent processing
- Data quality enforcement
- Analytics-ready modeling

---

## Author Notes

This project is intentionally scoped to demonstrate **fundamental data engineering judgment**
rather than framework-specific complexity.
