from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
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
dbt_path = os.getenv("DBT_PATH")
my_dbt_executable_path = os.getenv("DBT_EXECUTABLE_PATH")

my_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping = DuckDBUserPasswordProfileMapping(
        conn_id=db_conn,
        profile_args={"path" : f"{os.environ['AIRFLOW_HOME']}/{db_path}"}
    )
)

my_execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}{my_dbt_executable_path}"
)

@dag(
    dag_id="energy_market_analysis",
    start_date=datetime(2025, 7, 29),
    schedule="@weekly",
    catchup=False,
    max_active_tasks=1,
    default_args={"retries": 2},
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

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(f"{os.environ['AIRFLOW_HOME']}{dbt_path}"),
        execution_config=my_execution_config,
        profile_config=my_profile_config
    )

    create_tables() >> transform_data

energy_analysis_dag()