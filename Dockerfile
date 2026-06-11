# Sincronisation immédiate de l'image de base sur le runtime de ton orchestrateur
FROM apache/airflow:3.1.8-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Environnement isolé pour dbt (Verrouillé en 1.8.2 stable pour Postgres)
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.2 dbt-postgres==1.8.2

# 2. Environnement isolé pour l'ingestion DLT (Version d'Airflow parfaitement alignée)
RUN python -m venv /home/airflow/pipeline_venv && \
    /home/airflow/pipeline_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel

RUN /home/airflow/pipeline_venv/bin/pip install dlt[postgres]==1.23.0 requests==2.32.5 apache-airflow==3.1.8

# 3. Installation de la suite Data Science / ML native
COPY requirements.airflow.txt /requirements.airflow.txt
RUN pip install --no-cache-dir -r /requirements.airflow.txt \
    pandas==2.2.1 \
    scikit-learn==1.4.1.post1 \
    xgboost==2.0.3 \
    sqlalchemy==2.0.27