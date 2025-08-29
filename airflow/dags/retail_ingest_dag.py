from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import ingestion_script

def load_data():
    ingestion_script.run_ingestion()

# Define the DAG
with DAG(
    dag_id="retail_csv_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="load_csvs_to_postgres",
        python_callable=load_data
    )