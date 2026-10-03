# Bank Batch Pipeline

Enterprise-grade Batch ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for banking data using Apache Spark, MinIO, and Apache Airflow. The pipeline extracts data from a live PostgreSQL (OLTP) database, processes it through Data Lake layers, and serves it for visualization via Jupyter and Streamlit.

## Architecture

```text
PostgreSQL (OLTP) → Bronze (Raw Parquet) → Silver (Cleansed) → Gold (Star Schema)
      │                                                                  │
  (JDBC Extract)                                          Jupyter / Streamlit (Analytics)
```

Data in the Bronze, Silver, and Gold layers is stored in **MinIO** (S3-compatible object storage) in Parquet format.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.10.5 |
| Processing | Apache Spark 3.5.2 (PySpark) |
| Storage | MinIO (Data Lake) |
| Source OLTP DB | PostgreSQL 16 (`bank-db`) |
| Metadata DB | PostgreSQL 16 (Airflow internal) |
| Analytics | Jupyter (PySpark), Streamlit |
| Runtime | Docker Compose |

## Pipeline Stages

### 1. Bronze — Raw Ingestion
Connects to the live `bank-db` (PostgreSQL) operational database via **JDBC**. Extracts all 9 tables (transactions, customers, accounts, etc.) and writes them directly to `s3a://bronze/` in Parquet format. The `transactions` table is partitioned by `date_key`.

### 2. Silver — Data Quality
Applies schema enforcement and resolves 5 deliberate data defects in `transactions`:

| Defect | Fix |
|--------|-----|
| Duplicate `transaction_id` | `dropDuplicates` |
| NULL amount | `dropna` |
| Negative amount | `abs()` |
| Invalid currency (`XXX`) | Filtered out |
| Orphan `account_id` (999) | Inner join with `accounts` |

### 3. Gold — Star Schema
Builds a Kimball Star Schema with surrogate keys:

- **`dim_customer`** — SCD Type 2 (joins `customers` + `customer_history`)
- **`dim_account`**, **`dim_branch`**, **`dim_date`** — Conformed dimensions
- **`fact_transaction`** — Time-aware join to historically correct customer record

## Data Exploration

- **Streamlit** (`frontend/app.py`) — A custom web UI to visually browse and filter any table across the Bronze, Silver, and Gold layers directly from S3.
- **Jupyter** (`notebooks/analytics.ipynb`) — Pre-configured PySpark environment for running analytical SQL queries and aggregations on the Gold layer.

## Quick Start

```bash
# 1. Start all services (MinIO buckets and Postgres tables are auto-initialized)
docker compose up -d

# 2. Open Airflow UI (localhost:8080) and trigger the "bank_batch_pipeline" DAG
```

*Note: The `data/` folder contains CSV seed files that are automatically loaded into the PostgreSQL OLTP database on first boot via the `init_db.sql` script.*

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Spark Master | http://localhost:8081 | — |
| Streamlit UI | http://localhost:8501 | — |
| Jupyter | http://localhost:8888 | token: `admin` |
| PostgreSQL (OLTP) | `localhost:5433` | bank_user / bank_pass |
