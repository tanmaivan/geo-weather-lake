from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/weather_analytics"

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "build_star_schema",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    description="A DAG to build the Star Schema from Silver-layer weather data using dbt, creating dimension and fact tables for the Gold layer.",
    tags=["gold", "transformation", "child"],
) as dag:
    dbt_run_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run && \
            dbt test && \
            mv dbt_tmp.duckdb dbt.duckdb
        """,
    )
