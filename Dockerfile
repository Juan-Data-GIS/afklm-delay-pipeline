FROM apache/airflow:3.0.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Environnement isolé pour dbt 
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres

# 2. Environnement isolé ( DLT, Pandas, ML...)
RUN python -m venv /home/airflow/pipeline_venv && \
    /home/airflow/pipeline_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
RUN /home/airflow/pipeline_venv/bin/pip install dlt[postgres] requests apache-airflow==3.0.0

# 3. Installation des providers sur le cœur d'Airflow via fichier spécifique
COPY requirements.airflow.txt /requirements.airflow.txt
RUN pip install --no-cache-dir -r /requirements.airflow.txt