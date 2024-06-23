from datetime import datetime
import pandas as pd
from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def order_funnel():

    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    pd.read_parquet("data/order.parquet").to_sql("order", engine, if_exists="replace", index=False)

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "ingest_order",
    default_args=default_args,
    description="Order Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

task_load_order = PythonOperator(
     task_id="ingest_order",
     python_callable=order_funnel,
     dag=dag,
)

task_load_order
