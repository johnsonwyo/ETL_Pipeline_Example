<a id="readme-top"></a>

# Data Pipeline: Airflow, DBT, DuckDB
Jacoby Johnson

## PROJECT SCOPE



## PART I: ALL LOCAL, NO DOCKER

The tech stack for Part I was limited to Python and SQL, though this is interpreted loosly and any library that I could be installed with "pip install" was fair game. 
Open source was the name of the game, so implementation was on Linux (WSL specifically). Goal was to implement similar tech stack to that given in the job description.
The following technologies were used:

    - airflow: used for orchestration and creating the initial pipeline. Installed for python3.12 via:
        
        pip install 'apache-airflow==3.0.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"

    - dbt-core: used for data transformations. Lightweight, open-source version of dbt. Also need adapter for duckdb.
        
        python -m pip install dbt-core dbt-duckdb

    - duckdb: open-source in-process DBMS. Columnar data storage to conform with provided source data. Python client installed via:
        
        pip install duckdb

      duckdb also has a neat little ui containing a workspace/notebooks that can be accessed by installing duckdb v1.2.1+. This needs to
        be a non-python client due to limited versioning. Installed via:
        
        curl https://install.duckdb.org | sh
    
    - tableua: used for visualizations.

Tech stack notes:
    - Airflow use was limited due to complexity of airflow to dbt-core. Only a single dag was created that just seeded the data from the parquet
        files to the duckdb database (.ddb). Optimally, the dag would have also orchestrated the dbt; however, I ran into dependency issues between
        airflow and dbt-core/dbt-duckdb. I didn't really  Normally, I would have just spun up some docker containers to abstract away the dependencies, but that seemed
        a bit like overkill here and definitely violated the "only python" rule.
    - There were limited options for creating visualizations directly from a .ddb database, so tableua was chosen for ease of use. 
        SQL was still used to query master data table and generate analyses via data pool.

## PART II: ALL LOCAL, DOCKER, ASTRO, ONE-THREAD

## PART III: AWS DEPLOYMENT, MULTITHREADED

Coming soon...



2. ENGINEERING

Like mentioned above, airflow was initally used to seed the data from the parquet files into a duckdb database. In this case, this pretty much encompasses
all of the "E" and "L" from ETL/ELT, as all further data diving was really just transformations. A single dag was created that dynamically created .ddb tables
that were mapped in a json file in:

./data/table_info.json

This dag was ran by spinning up a standalone dev app from the command line via: airflow standalone. Formatting was consistant to include scheduling, though in this case 
it was just manually triggered. Code can be found in: 

./airflow/dags/seed_data_dag.py

dbt was used for all the remaining transformations. Like previously mentioned, aiflow and dbt had dependency issues (so much so that a second venv had to be created), 
so dbt was manually run on the CLI. The main model for the engineering section can be found at:

./dbt_analysis/models/demand_price_elasticity.sql

While creating all the ctes of the advanced query to calculate the demand_price_elasticity, a lucid chart was used to document the query substeps.
The first and final draft of the flow charts can be found at:

./show_your_work/pipeline/Transformation_FlowCharts.pdf

3. ANALYSIS

For the analysis section, dbt models were once again used, which are located in:

./dbt_analysis/models/

Visualizations that were created in tableua were screenshot and stored in:

./show_your_work/analysis/Energy_Markets_Test_Visualizations.pdf

In the real world, I would typically go hunt down the stakeholders and pick their brains for final KPIs, but in this case,
here are a few ideas for potential categories and visualizations (any actually created will be annoted):

TRENDS IN SEASONAILTY:
Idea here would be to see the larger trends that occur over the course of the entire year at different granularities:
    - estimated v actual consumption averages on daily basis (created)
    - estimated v actual consumption averages on weekly basis (created)
    - demand price elasticity average on daily basis (created)
    - demand price elasticity average on weekly basis (created)

TRENDS BY LOCATION:
Should investigate elasticity based on region (gsp_group_id). Would need more knowledge about the gsp_group_ids to actually implement:
    - elasticity daily average city v rural
    - elasticity daily average northern v southern
    - elasticity daily average coastal communities v NOT costal

TRENDS BY AGREEMENT TYPE:
I drug all those boolean variables along to the end for a reason:
    - elasticity using an enumeration between is_export, is_variable, is_charged_half_hourly (created)
    - elasticity using product_category
    - actual consumption for smart_import and NOT smart_import

TRENDS BY TIME OF DAY:
Daytime v nighttime consumption should be investigated:
    - estimated v actual consumption grouped by nighttime (11pm and 7am) and daytime (7am and 11pm) designations
    - actual consumption grouped by nighttime (11pm and 7am) and daytime (7am and 11pm) designations w/ smart_import v NOT smart_import
    - demand price elasticity grouped by nighttime (11pm and 7am) and daytime (7am and 11pm) designations