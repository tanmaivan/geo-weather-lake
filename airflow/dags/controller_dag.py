from airflow.sdk import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "controller",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@hourly",
    description="The main controller DAG that orchestrates the entire weather data pipeline.",
    tags=["controller", "orchestrator"],
) as dag:
    # task 1: trigger the bronze ingestion dag
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="ingest",
        wait_for_completion=True,
        poke_interval=30,
        failed_states=["failed"],
    )

    # task 2: trigger the silver transformation dag
    trigger_silver_transformation = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="transform",
        wait_for_completion=True,
        poke_interval=30,
        failed_states=["failed"],
    )

    # task 3: trigger the gold transformation dag
    trigger_gold_transformation = TriggerDagRunOperator(
        task_id="trigger_gold_transformation",
        trigger_dag_id="build_star_schema",
        wait_for_completion=True,
        poke_interval=30,
        failed_states=["failed"],
    )

    trigger_ingestion >> trigger_silver_transformation >> trigger_gold_transformation
