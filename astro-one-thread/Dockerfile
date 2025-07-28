FROM astrocrpublic.azurecr.io/runtime:3.0-5

RUN python -m venv .venv_dbt && \
    source .venv_dbt/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && \
    deactivate