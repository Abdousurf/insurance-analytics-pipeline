"""Insurance Analytics Pipeline DAG.

Controls the daily automated workflow that keeps our data fresh.
Every morning at 6:00 AM (UTC), it runs these steps in order:
    1. generate_data: Create fresh fake policy, claim, and contract data.
    2. load_to_duckdb: Put that raw data into our local database.
    3. dbt_run_staging: Clean and standardize the raw data.
    4. dbt_run_marts: Build the summary tables used by the dashboard.
    5. dbt_test: Check that the data looks correct.
    6. notify_success: Log that everything finished successfully.
"""

# ───────────────────────────────────────────────────────
# WHAT THIS FILE DOES (in plain English):
#
# This file defines an automated daily workflow (called a "DAG")
# using Apache Airflow — a tool for scheduling and running tasks.
#
# Think of it like a recipe with steps that must happen in order:
#   1. Create fresh test data
#   2. Load it into the database
#   3. Clean and organize the data (staging)
#   4. Build summary reports (marts)
#   5. Run quality checks
#   6. Log that everything went well
#
# If any step fails, it retries up to 3 times and sends an alert.
# This runs automatically every day so the dashboard always
# shows up-to-date information.
# ───────────────────────────────────────────────────────

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Folder paths on the Airflow server where our project code lives
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"

# Default settings that apply to every task in this workflow
default_args = {
    "owner": "analytics-engineering",
    "depends_on_past": False,  # Each run is independent — don't wait for yesterday's
    "email_on_failure": True,  # Send an email if something goes wrong
    "email_on_retry": False,  # Don't send emails for routine retries
    "retries": 3,  # Try each step up to 3 times if it fails
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes before retrying
    "retry_exponential_backoff": True,  # Wait longer between each retry attempt
}


def generate_data():
    """Create fresh fake insurance data.

    Runs the data generator script, which produces fake (but realistic)
    policy, claims, and contract files for testing.
    """
    # Add the project folder to Python's search path so we can find our code
    import sys

    sys.path.insert(0, PROJECT_DIR)
    from ingestion.generate_synthetic_data import main

    main()


def load_to_duckdb():
    """Move the raw data files into our database.

    Reads all the data files that were just created and loads them
    into the database so the next steps can work with them.
    """
    # Add the project folder to Python's search path so we can find our code
    import sys

    sys.path.insert(0, PROJECT_DIR)
    from ingestion.loaders import load_all_raw_data

    results = load_all_raw_data()
    # Print a summary of what was loaded
    for table, count in results.items():
        print(f"Loaded {table}: {count:,} rows")


def notify_success(context):
    """Log a message when the entire pipeline finishes without errors.

    Args:
        context: Information about the current task run, provided by Airflow.
    """
    print(f"Pipeline completed successfully at {datetime.now()}")


def notify_failure(context):
    """Log a warning message when any step in the pipeline fails.

    Args:
        context: Information about the current task run, provided by Airflow.
    """
    task = context.get("task_instance")
    print(f"Pipeline FAILED: task={task.task_id}, execution_date={context['ds']}")


# ── Define the workflow (DAG) ─────────────────────────────────────────────
# This block sets up the automated daily pipeline and all its steps
with DAG(
    dag_id="insurance_pipeline",
    default_args=default_args,
    description="End-to-end insurance analytics pipeline",
    schedule_interval="0 6 * * *",  # Run every day at 6:00 AM UTC
    start_date=datetime(2024, 1, 1),  # Start scheduling from this date
    catchup=False,  # Don't try to run for past dates we missed
    tags=["insurance", "analytics", "dbt"],
    on_failure_callback=notify_failure,
) as dag:

    # Step 1: Create fresh test data using our Python generator
    task_generate = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    # Step 2: Load the raw data files into the database
    task_load = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )

    # Step 3: Run dbt to clean and standardize the raw data (staging layer)
    task_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    )

    # Step 4: Run dbt to build the summary tables used by the dashboard
    task_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate marts",
    )

    # Step 5: Run dbt tests to make sure the data looks correct
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    # Step 6: Log that everything finished successfully
    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Define the order: each step must finish before the next one starts
    (
        task_generate
        >> task_load
        >> task_dbt_staging
        >> task_dbt_marts
        >> task_dbt_test
        >> task_notify
    )
