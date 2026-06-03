"""
afklm_02_transformation_scoring.py — Orchestration Transformations dbt & Scoring ML.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator
import sys
import os

from monitoring_utils import operator_failure_callbacks, log_operator_success

default_args = {
    'owner': 'afklm_analytics_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_dbt_transformation(env_target: str):
    import sys
    import os
    import json
    from dbt.cli.main import dbtRunner
    
    # Configuration des répertoires de travail dbt
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    
    # Alignement forcé de la variable d'environnement lue par profiles.yml
    os.environ["DBT_TARGET"] = env_target.strip().lower()
    
    cli_args = [
        "run", 
        "--profiles-dir", "/opt/airflow/dbt", 
        "--project-dir", "/opt/airflow/dbt", 
        "--target", os.environ["DBT_TARGET"]
    ]
    
    dbt = dbtRunner()
    result = dbt.invoke(cli_args)
    
    if not result.success: 
        raise RuntimeError("Echec de la commande dbt run.")
        
    # ---- EXTRACT METRICS FROM dbt RUN ----
    try:
        results_path = "/opt/airflow/dbt/target/run_results.json"
        if os.path.exists(results_path):
            with open(results_path, "r") as f:
                run_results = json.load(f)
            
            total_rows = 0
            for res in run_results.get("results", []):
                rows_affected = res.get("adapter_response", {}).get("rows_affected", 0)
                if isinstance(rows_affected, int):
                    total_rows += rows_affected
                elif isinstance(rows_affected, float):
                    total_rows += int(rows_affected)

            # Communication du volume traité au Listener Airflow via XCom
            from airflow.sdk import get_current_context
            ctx = get_current_context()
            ti = ctx["task_instance"]
            
            ti.xcom_push(key='data_metrics', value={
                "records_processed": total_rows,
                "pipeline_engine": "dbt",
                "data_layer": "trusted"
            })
            print(f"[DATAOPS METRICS] Total dbt rows affected: {total_rows}")
    except Exception as e:
        print(f"[METRICS WARNING] Impossible d'extraire les volumes de run_results.json: {e}")


def run_dbt_validation(env_target: str):
    import sys
    import os
    from dbt.cli.main import dbtRunner
    
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_TARGET"] = env_target.strip().lower()
    
    cli_args = [
        "test", 
        "--profiles-dir", "/opt/airflow/dbt", 
        "--project-dir", "/opt/airflow/dbt", 
        "--target", os.environ["DBT_TARGET"]
    ]
    
    dbt = dbtRunner()
    result = dbt.invoke(cli_args)
    if not result.success: 
        raise RuntimeError("Echec de la commande dbt test.")


def run_ml_scoring_pipeline(env_target: str):
    """Exécute de manière isolée le pipeline d'inférence prédictive XGBoost."""
    import sys
    import os
    
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    
    # Résolution des imports via l'injection du chemin absolu du volume ml monté
    ml_path = "/opt/airflow/ml"
    if ml_path not in sys.path:
        sys.path.insert(0, ml_path)
            
    print(f"[DEBUG PATH] Ingestion sys.path reussie pour environnement natif : {sys.path}")
    
    from ml_run import main as run_prediction_script
    run_prediction_script()


def _trigger_fastapi(env_target: str):
    import requests
    import os
    
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    
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
        op_kwargs={
            "env_target": "{{ dag_run.conf.get('env_target', 'local') }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_run_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="TRUSTED", event_type="dbt_run_success")
    )

    run_dbt_tests = ExternalPythonOperator(
        task_id='afklm_t_dbt_test',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_validation,
        op_kwargs={
            "env_target": "{{ dag_run.conf.get('env_target', 'local') }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_test_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="TRUSTED", event_type="dbt_test_success")
    )

    compute_ml_predictions = PythonOperator(
        task_id='afklm_ml_compute_predictions',
        python_callable=run_ml_scoring_pipeline,
        op_kwargs={
            "env_target": "{{ dag_run.conf.get('env_target', 'local') }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="REFINED", event_type="ml_scoring_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="REFINED", event_type="ml_scoring_success")
    )

    trigger_fastapi_scoring = ExternalPythonOperator(
        task_id='afklm_ml_trigger_fastapi',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=_trigger_fastapi,
        op_kwargs={
            "env_target": "{{ dag_run.conf.get('env_target', 'local') }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="REFINED", event_type="fastapi_reload_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="REFINED", event_type="fastapi_reload_success")
    )

    run_dbt_models >> run_dbt_tests >> compute_ml_predictions >> trigger_fastapi_scoring