from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = Path("/opt/airflow")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="chicago_crimes_pipeline",
    default_args=default_args,
    description="Raw ingest and transform pipeline for Chicago crimes data",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-engineering", "pyspark", "chicago-crimes"],
) as dag:

    raw_ingest = BashOperator(
        task_id="raw_ingest",
        bash_command="cd /opt/airflow && python jobs/raw_ingest.py",
    )

    transform = BashOperator(
        task_id="transform_crimes",
        bash_command="cd /opt/airflow && python jobs/transform_crimes.py",
    )

    raw_ingest >> transform
