# Cloud FinOps Data Lakehouse & Infrastructure Cost Optimizer

An end-to-end cloud data pipeline designed to ingest, transform, and analyze multi-cloud billing logs. Solves untagged cloud resource leaks, detects spend anomalies, and attributes infrastructure costs across engineering teams and environments.

## Tech Stack
- **Language:** Python
- **Data Processing:** Pandas, DuckDB, dbt
- **Cloud Storage & Warehouse:** Object Storage (S3 / GCS) & Warehouse (Snowflake / BigQuery)
- **BI & Analytics:** Looker Studio / Power BI

## Project Layout
```text
├── data/               <- Raw and processed local test artifacts (gitignored)
├── src/
│   ├── ingestion/      <- Log generator and cloud upload scripts
│   └── transforms/     <- Transformation scripts
├── sql/                <- Analytical queries & schemas
├── tests/              <- Data validation tests
└── README.md
