from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("/opt/airflow/.env")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
JOB_ID = os.environ.get("DATABRICKS_JOB_ID")

sys.path.append("/opt/airflow")

from fetch_stock_data import main as fetch_stock_data

default_args = {
    "owner": "ernest",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "stock_pipeline_dag",
    default_args=default_args,
    description="A simple DAG to fetch stock data",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 19),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=fetch_stock_data,
        op_args=[["AAPL", "GOOGL"]]  # List of stock symbols to fetch,
    )

    process_data = BashOperator(
        task_id="transform_stock_data",
        bash_command=(
            "curl -X POST 'https://dbc-118d1597-8b59.cloud.databricks.com/api/2.0/jobs/run-now' "
            f"-H 'Authorization: Bearer {DATABRICKS_TOKEN}' "
            f"-d '{{\"job_id\": {JOB_ID}}}'"
        ),
    )

    fetch_data >> process_data
