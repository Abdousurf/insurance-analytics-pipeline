"""Insurance Analytics Pipeline DAG.

Orchestrates the full pipeline: data generation -> DuckDB load -> dbt run -> dbt test.
Schedule: daily at 06:00 UTC.

Tasks:
    1. generate_data: Create synthetic policy, claim, and contract datasets.
    2. load_to_duckdb: Ingest raw Parquet files into the DuckDB warehouse.
    3. dbt_run_staging: Run dbt staging models.
    4. dbt_run_marts: Run dbt intermediate and mart models.
    5. dbt_test: Execute dbt tests.
    6. notify_success: Log pipeline completion.
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
    """Run the synthetic data generation pipeline.

    Imports and executes the main entry point from the ingestion module
    to produce raw Parquet datasets.
    """
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.generate_synthetic_data import main
    main()


def load_to_duckdb():
    """Load raw Parquet files into the DuckDB warehouse.

    Imports the loader module and ingests all available raw datasets
    into the ``raw`` schema, logging row counts per table.
    """
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.loaders import load_all_raw_data
    results = load_all_raw_data()
    for table, count in results.items():
        print(f"Loaded {table}: {count:,} rows")


def notify_success(context):
    """Log a success message when the pipeline completes.

    Args:
        context: Airflow task instance context dictionary.
    """
    print(f"Pipeline completed successfully at {datetime.now()}")


def notify_failure(context):
    """Log an error message when a pipeline task fails.

    Args:
        context: Airflow task instance context dictionary.
    """
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
