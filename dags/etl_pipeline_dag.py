from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

with DAG(
    "bank_batch_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    jars = "/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/jars/postgresql-42.7.3.jar"

    bronze_task = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"spark-submit --master 'local[*]' --jars {jars} /opt/spark/jobs/bronze_ingestion.py",
    )

    silver_task = BashOperator(
        task_id="cleanse_to_silver",
        bash_command=f"spark-submit --master 'local[*]' --jars {jars} /opt/spark/jobs/silver_ingestion.py",
    )

    gold_task = BashOperator(
        task_id="build_star_schema_gold",
        bash_command=f"spark-submit --master 'local[*]' --jars {jars} /opt/spark/jobs/gold_star_schema.py",
    )

    bronze_task >> silver_task >> gold_task
