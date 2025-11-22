from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.append(str(scripts_path))

try:
    from ingest_weather import run_ingestion
except ImportError as e:
    raise ImportError(f"Could not import run_ingestion from ingest_weather.py: {e}")

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ingest",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    description="A DAG to ingest current weather data from Weatherbit API to the Bronze layer.",
    tags=["bronze", "ingestion", "child"],
) as dag:
    ingestion_task = PythonOperator(
        task_id="fetch_transform_load_to_bronze", python_callable=run_ingestion
    )
