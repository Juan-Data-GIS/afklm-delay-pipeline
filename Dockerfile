FROM apache/airflow:3.0.0-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Environnement isolé pour dbt (Laissé inchangé pour vos transformations)
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.8.2

# 2. Environnement isolé pour le pipeline d'ingestion DLT
RUN python -m venv /home/airflow/pipeline_venv && \
    /home/airflow/pipeline_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
# MODIFICATION DE SÉCURITÉ : Alignement sur la version 3.1.8 dictée par le runtime 
# pour supprimer définitivement le warning de disparité de l'ExternalPythonOperator.
RUN /home/airflow/pipeline_venv/bin/pip install dlt[postgres]==1.23.0 requests==2.32.5 apache-airflow==3.1.8

# 3. Installation des providers et de la suite ML harmonisée sur le cœur d'Airflow
# RESOLUTION : Versions modernes compatibles Python 3.12 (Wheels pré-compilées).
COPY requirements.airflow.txt /requirements.airflow.txt
RUN pip install --no-cache-dir -r /requirements.airflow.txt \
    pandas==2.2.1 \
    scikit-learn==1.4.1.post1 \
    xgboost==2.0.3 \
    sqlalchemy==2.0.27