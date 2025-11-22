# airflow/dags/ingest_weather_dag.py
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.append(str(scripts_path))

try:
    from transform_to_silver import run_transformation
except ImportError as e:
    raise ImportError(
        f"Could not import run_transformation from transform_to_silver: {e}"
    )

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "transform",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    description="A DAG to transform raw weather data from the Bronze layer into the Silver layer with cleaned and feature-engineered records.",
    tags=["silver", "transformation", "child"],
) as dag:
    transformation_task = PythonOperator(
        task_id="run_bronze_to_silver_transformation",
        python_callable=run_transformation,
    )
