# Bank Batch Pipeline

Batch ETL pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for simulated banking data using Apache Spark, MinIO, and Apache Airflow. Processed data can be explored through a Jupyter notebook and a Streamlit web interface.

## Architecture

```
CSV (OLTP Simulation) → Bronze (Raw Parquet) → Silver (Cleansed) → Gold (Star Schema)
                                                                         │
                                                          Jupyter / Streamlit (Analytics)
```

All layers are stored as Parquet in MinIO (S3-compatible object storage).

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.10.5 |
| Processing | Apache Spark 3.5.2 (PySpark) |
| Storage | MinIO |
| Metadata DB | PostgreSQL 16 (Airflow metadata) |
| Analytics | Jupyter (PySpark), Streamlit |
| Runtime | Docker Compose |

## Pipeline Stages

### Bronze — Raw Ingestion
Reads 9 CSV tables and writes them as Parquet to `s3a://bronze/`. The `transactions` table is partitioned by `date_key`.

### Silver — Data Quality
Applies schema enforcement and resolves 5 deliberate data defects in `transactions`:

| Defect | Fix |
|--------|-----|
| Duplicate `transaction_id` | `dropDuplicates` |
| NULL amount | `dropna` |
| Negative amount | `abs()` |
| Invalid currency (`XXX`) | Filtered out |
| Orphan `account_id` (999) | Inner join with `accounts` |

### Gold — Star Schema
Builds a Kimball Star Schema with surrogate keys:

- **`dim_customer`** — SCD Type 2 (joins `customers` + `customer_history`)
- **`dim_account`**, **`dim_branch`**, **`dim_date`** — Conformed dimensions
- **`fact_transaction`** — Time-aware join to historically correct customer record

## Data Exploration

- **Jupyter** (`notebooks/analytics.ipynb`) — reads Gold tables from MinIO with PySpark and runs analytical SQL queries.
- **Streamlit** (`frontend/app.py`) — web interface to browse any table in the Bronze, Silver, or Gold layer.

## Project Structure

```
bank_batch_pipeline/
├── dags/
│   └── etl_pipeline_dag.py
├── jobs/
│   ├── bronze_ingestion.py
│   ├── silver_ingestion.py
│   └── gold_star_schema.py
├── notebooks/
│   └── analytics.ipynb
├── frontend/
│   └── app.py
├── data/                        # 9 source CSV files
├── jars/                        # hadoop-aws & aws-sdk JARs (gitignored)
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Quick Start

```bash
# 1. Place hadoop-aws-3.3.4.jar and aws-java-sdk-bundle-1.12.262.jar in jars/

# 2. Start services
docker compose up -d

# 3. Create buckets in MinIO UI — bronze, silver, gold

# 4. Trigger the DAG "bank_batch_pipeline" in Airflow UI
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Spark Master | http://localhost:8081 | — |
| Jupyter | http://localhost:8888 | token: admin |
| Streamlit | http://localhost:8501 | — |
