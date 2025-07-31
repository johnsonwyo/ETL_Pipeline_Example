# Data Pipeline & Analysis: Airflow, DBT, DuckDB
Jacoby Johnson

## PROJECT SCOPE

### The Problem Domain
The domain for this data engineering / analytics project was to create a data pipeline for an energy company that would take raw data from energy meters outside of customer houses/businesses 
and compare that data against predictive energy models. There were eight sources of input (time-based parquet files dumped into an S3 bucket) that were periodically updated and the overall 
project goal was to seed them, create a master table to run analytics against, and then run said analyses. As I'm not the owner of the data for this project, I won't spend too much time discussing
the analytics piece of the puzzle and won't be sharing the data; however, the technologies used and created pipeline will be extensively discussed and everything should be easy enough to follow for implementing any data in this pipeline.

### Core Tech Stack
* Airflow: Open source. Used for orchestration and scheduling the pipeline to run at set intervals.
* DBT-Core: Open source. Used for all transformations and analytics. Break SQL statements into sub tables that can be referenced with Jinja templating. 
* DuckDB: Open source. Columnar database that is in-memory and uses vectorized processing. Excellent at analytics for time-based data. Limited by single thread access for writes.

### The Data Engineering Piece
The aim of this demo is to demonstrate three separate ways how to create a data pipeline using Airflow for orchestration, DBT for transformations, and DuckDB for the database.
Each sequential method for creating this pipeline increases in complexity and tools used, though this will also result in an increase in usefulness. The ways the pipeline was created are as follows:

* PART I: Local. All local installation of airflow, dbt, and duckdb without the use of VMs or containerization with docker. Here be dragons.
* PART II: Local Containerization. Astronomer Cosmos was used to spin up a local Docker container that housed DBT in a venv, had a mounted DuckDB database, but was limited to one thread.
* PART III: Fully Deployed. Tech stack from Part II was used again, but this time the container was deployed in AWS, input/outputs are from S3 buckets, and multi-threading was used for efficiency.

## PIPELINE PART I: ALL LOCAL, NO DOCKER

This is not the way. There is no "simple" way to get airflow, dbt, and duckdb working together without the use of VMs/containers. Not to say that it can't be done, just saying that
there are a few complex issues to overcome that make this not the best approach. Issues to consider are as follows:

* Airflow and dbt-core have dependency issues. These can be overcome by using two separate venvs and using either a bash script, PythonVirtualenvOperator, or an ExternalPythonOperator
to use dbt within an airflow task; however, this leads to the next issue.
* DuckDB can only be accessed by one write thread at a time, leading to concurrency control issues. When calling the external dbt venv from within an airflow task, a "Cannot access
lock on ddb" error will occur. This cannot be overcome with a simple MutEx lock.
* If using a bash script, PythonVirtualenvOperator, or an ExternalPythonOperator to access dbt, the individual models in dbt will not be shown in the airflow DAG.

In the end, I didn't think the juice was worth the squeeze in getting all three technologies to work together without using containers, so PART I just implements airflow and dbt
separately. Installation details for this part are below; however, if you want to see an easier approach, just continue to PART II.

### Installation and Setup

The tech stack for Part I was limited to Python and SQL, though this is interpreted loosely and any library that could be installed with "pip install" in python was fair game. 
Open source was the name of the game, so implementation was on Linux (WSL specifically). Installation instructions will be slightly different on Mac or Windows. The following technologies were used:

* Venv - create two venvs called .venv_airflow and .venv_dbt. Using .venv_dbt as an example below:

    ```sh
    python3 -m venv .venv_dbt
    ```

    Activate venvs as needed using:
    ```sh
    . .venv_dbt/bin/activate
    ```

* Airflow - used for orchestration. Installed for python3.12 via:
        
    ```sh
    pip install 'apache-airflow==3.0.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
    ```

    Need to update the environmental variable for the airflow home directory:
    ```sh
    export AIRFLOW_HOME={your_airflow_project_path_here}/airflow
    ```

    Might need to migrate the airflow database if AIRFLOW_HOME was changed with:
    ```sh
    airflow db migrate
    ```

    Change the airflow.cfg file to exclude example dags:
    ```sh
    load_examples = False
    ```

    Create a dag in the dags folder then spin up a local instance of airflow with:
    ```sh
    airflow standalone
    ```

    Will be spun up at localhost on port 8080 and you will have to input the password found in the auto-generated simple_auth_manager_passwords.json.generated file
    that was created in the airflow directory.

* Dotenv - used for local environmental variables referenced from a .env file. Installed via:
    ```sh
    pip install dotenv
    ```

* DuckDB - columnar database. Installed with:
    ```sh
    pip install duckdb
    ```

* DBT - transformations and analytics. Installed both core version and dbt adapter with via:
    ```sh
    pip install dbt-core dbt-duckdb
    ```

    To only run dbt models, just activate the .venv_dbt, navigate to the dbt project directory, and run:
    ```sh
    dbt run
    ```

    To run the dbt models and any tests, activate the .venv_dbt, navigate to the dbt project directory, and run:
    ```sh
    dbt build
    ```

* Tableau - used for visualizations. Third party.

### Project Work

Like mentioned above, airflow was initially used to seed the data from the parquet files into a duckdb database. In this case, this pretty much encompasses
all of the "E" and "L" from ETL/ELT, as all further data diving was really just transformations. In Part I, a single dag was created that dynamically created .ddb tables
that were mapped in a json file in:

```sh
PART_I_Local/data/table_info.json
```

The duckdb database was just created as a .ddb file in the project directory at:

```sh
PART_I_Local/database/database.ddb
```

This dag was ran by spinning up a standalone airflow app from the command line. Code for the dag can be found in: 

```sh
PART_I_Local/airflow/dags/data_analysis_dag.py
```

Dbt was used for all the remaining transformations. Like previously mentioned, airflow and dbt had dependency issues (so much so that a second venv had to be created), 
so dbt was manually run on the CLI. The model for the creating the master data set can be found at:

```sh
PART_I_Local/dbt_analysis/models/master_table/demand_price_elasticity.sql
```

While creating all the CTEs of the advanced query to calculate the demand_price_elasticity, a lucid chart was used to document the query sub steps.
The first and final draft of the flow charts can be found at:

```sh
PART_I_Local/show_your_work/pipeline/Transformation_FlowCharts.pdf
```

All other analytical queries and the resulting visuals can respectively be found at:
```sh
PART_I_Local/dbt_analysis/models/analytics_tables/
PART_I_Local/show_your_work/analysis/Visualizations.pdf
```

## PART II: ALL LOCAL, DOCKER, ASTRO, ONE-THREAD

This is the way. Mostly. All the major problems that were experienced in PART I were solved using containers and astronomer cosmos. There are no longer dependency issues, the database locking
errors are resolved (with some DAG settings modifications), and DBT tasks are now properly displayed within the airflow DAG.

This would be a go-to solution for any data pipeline + analytics work to be accomplished locally/on-prem. After the initial setup, all deployment and Ops are handled by Cosmos CLI commands,
which means that Docker knowledge is not a barrier to entry for this method. With the right setup and using a single CLI command, DBT is installed in the image on compile and the container is spun-up with the five necessary processes for Airflow.

### Installation and Setup

For Part II, all the same technologies are used that were used in Part I, but now additionally Docker will be used to house and run a local airflow container and Astronomer Cosmos will be used
as a framework for running all applications and interfacing with Docker:

* Docker Desktop - used to run containers locally. WSL was also downloaded since I used Windows. Find download documentation at the links at the bottom of this webpage:
```sh
https://docs.docker.com/desktop/
```

* Astronomer Cosmos - used to abstract away dependency issues between airflow, dbt, duckdb and manage the Docker container. Installed with:
```sh
pip install astronomer-cosmos
```

Once cosmos is installed, this also means that you have installed airflow. Much Rejoicing! Now you need to initialize your project with:
```sh
astro dev init
```

This should create the necessary folder structure and files needed to run a cosmos airflow project. Since we are using dbt-core, and open-source anything requires extra steps, we will need to
create a venv inside of our Docker image that houses our dbt and duckdb connector. Navigate to the Dockerfile and copy the following:

```sh
RUN python -m venv .venv_dbt && \
    source .venv_dbt/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && \
    deactivate
```

DBT should be good to go now. The final step to get this project off the ground is to add some libraries to be installed inside of the requirements file. Navigate to the requirements.txt
file and copy the following:
```sh
astronomer-cosmos
dbt-duckdb
airflow-provider-duckdb
```

Optionally, you can also edit ports for the airflow app if you are experiencing port conflicts in:
```sh
PART_II_Docker-Astro-One-Thread/.astro/config.yaml
```

* Tableau - used for visualizations. Third party.

### Project Work

The duckdb database needs to be housed in a specific directory in order to communicate outside of the Docker container. This can automatically be accomplished by housing the database.ddb file in the
/include default directory. Cosmos auto-mounts anything within the /include directory, so anything that happens to the database within the Docker container is reciprocated in the same folder
in the local directory. Place the DuckDB file (or just set the db path since the database.ddb file will be created automatically if it doesn't already exist) here:
```sh
PART_II_Docker-Astro-One-Thread/include/database.ddb
```

In Part II, a single dag was once again created that dynamically created .ddb tables that were mapped in a json file in:
```sh
PART_II_Docker-Astro-One-Thread/data/table_info.json
```

While that was where the functionality of the DAG stopped in Part I, in Part II, the dag is also used to create all dbt models using a DBT Task Group. Code for the dag can be found in: 
```sh
PART_II_Docker-Astro-One-Thread/dags/data_analysis_dag.py
```

Here is where it is necessary to tackle the problem of having only a single write access thread at a time for DuckDB. This obviously isn't efficient for running multiple analytical queries at once,
but if the desire is to persist tables in the duckdb database using DBT, then it is necessary to add the following within the dag properties when you are creating the dag in the data_analysis_dag.py file:
```sh
max_active_tasks=1,
```

Dbt was still used for the data transformations. The model for the creating the master data set can be found at:
```sh
PART_II_Docker-Astro-One-Thread/dags/dbt_analysis/models/master_table/demand_price_elasticity.sql
```

While creating all the CTEs of the advanced query to calculate the demand_price_elasticity, a lucid chart was used to document the query sub steps.
The first and final draft of the flow charts can be found at:
```sh
PART_II_Docker-Astro-One-Thread/show_your_work/pipeline/Transformation_FlowCharts.pdf
```
All other analytical queries created and the resulting visuals can respectively be found at:
```sh
PART_II_Docker-Astro-One-Thread/dags/dbt_analysis/models/analytics_tables/
PART_II_Docker-Astro-One-Thread/show_your_work/analysis/Visualizations.pdf
```

Time to spin up the container. This can be simply accomplished with:
```sh
astro dev start
```

Congrats! You now have airflow, dbt-core, and duckdb all working together. 

A few more useful commands are:
* Stop the container with:
```sh
astro dev stop
```

* Stop and delete the container with:
```sh
astro dev kill
```

## PART III: AWS DEPLOYMENT, MULTITHREADED

Coming soon...

Features to be added include:

* S3 buckets used to house input and output files
* Container spun up on AWS ECS
* Analytics models modified to include multithreading