"""
Insurance Analytics Pipeline DAG
==================================
Orchestrates the full pipeline: data generation -> DuckDB load -> dbt run -> dbt test.
Schedule: daily at 06:00 UTC.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"

default_args = {
    "owner": "analytics-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def generate_data():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.generate_synthetic_data import main
    main()


def load_to_duckdb():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.loaders import load_all_raw_data
    results = load_all_raw_data()
    for table, count in results.items():
        print(f"Loaded {table}: {count:,} rows")


def notify_success(context):
    print(f"Pipeline completed successfully at {datetime.now()}")


def notify_failure(context):
    task = context.get("task_instance")
    print(f"Pipeline FAILED: task={task.task_id}, execution_date={context['ds']}")


with DAG(
    dag_id="insurance_pipeline",
    default_args=default_args,
    description="End-to-end insurance analytics pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["insurance", "analytics", "dbt"],
    on_failure_callback=notify_failure,
) as dag:

    task_generate = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    task_load = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )

    task_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    )

    task_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate marts",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    (
        task_generate
        >> task_load
        >> task_dbt_staging
        >> task_dbt_marts
        >> task_dbt_test
        >> task_notify
    )
