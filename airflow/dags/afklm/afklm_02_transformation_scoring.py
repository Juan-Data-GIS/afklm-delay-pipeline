"""
afklm_02_transformation_scoring.py — Orchestration Transformations dbt & Scoring ML.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator
import sys
import os

sys.path.insert(0, '/opt/airflow/dags')
from utils.monitoring_utils import operator_failure_callbacks, log_operator_success

default_args = {
    'owner': 'afklm_analytics_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_dbt_transformation():
    import sys
    import os
    from dbt.cli.main import dbtRunner
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    cli_args = ["run", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt", "--target", os.getenv("DBT_TARGET", "prod")]
    dbt = dbtRunner()
    result = dbt.invoke(cli_args)
    if not result.success: raise RuntimeError("Echec de la commande dbt run.")

def run_dbt_validation():
    import sys
    import os
    from dbt.cli.main import dbtRunner
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    cli_args = ["test", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt", "--target", os.getenv("DBT_TARGET", "prod")]
    dbt = dbtRunner()
    result = dbt.invoke(cli_args)
    if not result.success: raise RuntimeError("Echec de la commande dbt test.")

def _trigger_fastapi():
    import requests
    response = requests.post("http://afklm-fastapi:8000/v1/scoring/reload", timeout=30)
    response.raise_for_status()


with DAG(
    'afklm_02_transformation_scoring',
    default_args=default_args,
    description='Pipeline ELT AF/KLM - Etape 2 : Transformations DBT & Scoring API',
    schedule=None,
    catchup=False,
    tags=['afklm', 'transformation', 'dbt', 'ml'],
) as dag:

    run_dbt_models = ExternalPythonOperator(
        task_id='afklm_t_dbt_run',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_transformation,
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_run_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="TRUSTED", event_type="dbt_run_success")
    )

    run_dbt_tests = ExternalPythonOperator(
        task_id='afklm_t_dbt_test',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_validation,
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_test_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="TRUSTED", event_type="dbt_test_success")
    )

    trigger_fastapi_scoring = ExternalPythonOperator(
        task_id='afklm_ml_trigger_fastapi',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=_trigger_fastapi,
        on_failure_callback=operator_failure_callbacks(layer="REFINED", event_type="fastapi_reload_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="REFINED", event_type="fastapi_reload_success")
    )

    run_dbt_models >> run_dbt_tests >> trigger_fastapi_scoring