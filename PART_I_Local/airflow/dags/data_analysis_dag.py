from airflow.decorators import dag, task
from datetime import datetime
import duckdb
import os
from dotenv import load_dotenv
import json

# Get enviornmental variables, set paths
load_dotenv()
data_path = os.getenv("RAW_DATA_PATH")
tables_path = os.getenv("TABLE_INFO_PATH")
db_path = os.getenv("DB_PATH")
db_conn = duckdb.connect(database=db_path)

@dag(
    dag_id="energy_market_analysis",
    start_date=datetime(2025, 6, 15),
    schedule="@weekly",
    catchup=False,
    max_active_tasks=1,
)
def energy_analysis_dag():
    
    @task(task_id="create_seed_tables")
    def create_tables():
        # Get table names and columns from table_info.json
        with open(tables_path, 'r') as tables_import_file:
            tables = json.load(tables_import_file)

        for table in tables:
            name = table.get("name")
            columns = str(table.get("columns"))
            columns = columns.replace("[", "").replace("]", "").replace("'", "")
            db_conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT {columns} FROM read_parquet('{data_path}{name}.parquet')")

    create_tables()

energy_analysis_dag()