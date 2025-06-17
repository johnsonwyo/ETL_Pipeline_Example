from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="create_table_dag",
    start_date=datetime(2025, 6, 15),
    schedule=None,  # Or set to '@once' for a single run
    catchup=False,
) as dag:
    create_table_task = SQLExecuteQueryOperator(
        task_id="estimated_annual_consumption_interval",
        conn_id="duckdb",  # Replace with your connection ID
        sql="CREATE OR REPLACE TABLE estimated_annual_consumption_interval AS SELECT * FROM read_parquet('/home/johnsonwyo/Analytics_Test_JJ/data/estimated_annual_consumption_interval.parquet')",
    )