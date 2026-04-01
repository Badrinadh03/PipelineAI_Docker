from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data from source...")
    print("Connected to database successfully")

def transform():
    print("Starting transformation...")
    # This will intentionally fail
    raise ValueError("Column 'user_id' not found in dataset. Schema mismatch detected.")

def load():
    print("Loading data to warehouse...")

with DAG(
    dag_id="test_failing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task