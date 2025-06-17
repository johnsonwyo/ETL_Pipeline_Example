from airflow.decorators import dag, task
from datetime import datetime
import os
import duckdb

con = duckdb.connect(database='analytics_test_db.ddb')
con.execute("CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_parquet('/home/johnsonwyo/Analytics_Test_JJ/data/estimated_annual_consumption_interval.parquet')")
