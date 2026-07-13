from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator

from monitoring_utils import log_event, operator_failure_callbacks

default_args = {
    'owner': 'afklm_analytics_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- TRACKING FUNCTIONS ---
def start_trans_tracking():
    from airflow.sdk import get_current_context
    from datetime import datetime
    ctx = get_current_context()
    
    # Récupération de la date métier transmise par le DAG 01 dans la conf
    dag_run = ctx.get("dag_run")
    business_date = None
    if dag_run and dag_run.conf and dag_run.conf.get("start_date"):
        try:
            # On parse la date transmise ("YYYY-MM-DD")
            business_date = datetime.strptime(dag_run.conf["start_date"].strip()[:10], "%Y-%m-%d")
        except Exception:
            business_date = ctx.get("logical_date")
    else:
        business_date = ctx.get("logical_date")

    log_event(
        level="INFO", layer="ORCHESTRATION", message="Demarrage du pipeline de transformation et scoring dbt",
        dag_id=ctx["task_instance"].dag_id, task_id=ctx["task_instance"].task_id,
        event_type="dag_started", run_id=str(ctx["dag_run"].run_id), 
        explicit_timestamp=business_date  # <--- Utilise la date métier harmonisée
    )

def success_trans_tracking():
    from airflow.sdk import get_current_context
    from datetime import datetime
    ctx = get_current_context()
    
    # Récupération de la volumétrie dbt via XCom
    dbt_metrics = ctx["task_instance"].xcom_pull(task_ids="afklm_t_dbt_run") or {}
    records = dbt_metrics.get("records_processed", 0)
    
    # Même logique de récupération de la date métier pour le log de succès
    dag_run = ctx.get("dag_run")
    business_date = None
    if dag_run and dag_run.conf and dag_run.conf.get("start_date"):
        try:
            business_date = datetime.strptime(dag_run.conf["start_date"].strip()[:10], "%Y-%m-%d")
        except Exception:
            business_date = ctx.get("logical_date")
    else:
        business_date = ctx.get("logical_date")
        
    log_event(
        level="INFO", layer="TRUSTED", message=f"Pipeline Transformation REUSSI : {records} lignes traitees.",
        dag_id=ctx["task_instance"].dag_id, task_id=ctx["task_instance"].task_id,
        event_type="dbt_run_success", run_id=str(ctx["dag_run"].run_id), 
        explicit_timestamp=business_date,  # <--- Utilise la date métier harmonisée
        vols_ingested=0,          
        rows_inserted=records,     
        pipeline_engine="dbt"
    )

# --- BUSINESS TASKS ---
def run_dbt_transformation(env_target: str):
    import os
    import json
    from dbt.cli.main import dbtRunner
    
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_TARGET"] = env_target.strip().lower()
    
    dbt = dbtRunner()
    result = dbt.invoke(["run", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt", "--target", os.environ["DBT_TARGET"]])
    
    if not result.success: 
        raise RuntimeError("Echec de la commande dbt run.")
        
    total_rows = 0
    try:
        results_path = "/opt/airflow/dbt/target/run_results.json"
        if os.path.exists(results_path):
            with open(results_path, "r") as f:
                run_results = json.load(f)
            for res in run_results.get("results", []):
                rows_affected = res.get("adapter_response", {}).get("rows_affected", 0)
                total_rows += int(rows_affected) if isinstance(rows_affected, (int, float)) else 0
    except Exception as e:
        print(f"[METRICS WARNING] Erreur run_results.json: {e}")

    return {"records_processed": total_rows, "pipeline_engine": "dbt"}

def run_dbt_validation(env_target: str):
    import os
    from dbt.cli.main import dbtRunner
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_TARGET"] = env_target.strip().lower()
    dbt = dbtRunner()
    result = dbt.invoke(["test", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt", "--target", os.environ["DBT_TARGET"]])
    if not result.success: 
        raise RuntimeError("Echec de la commande dbt test.")

def run_ml_scoring_pipeline(env_target: str):
    import sys
    import os
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    ml_path = "/opt/airflow/ml"
    if ml_path not in sys.path:
        sys.path.insert(0, ml_path)
    from ml_run import main
    main()

def _trigger_fastapi(env_target: str):
    import requests
    import os
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    response = requests.post("http://afklm-formation-fastapi:8000/v1/scoring/reload", timeout=30)
    response.raise_for_status()


# Docs Markdown Loader
docs_path = os.path.join('/opt/airflow/docs', 'afklm_02_transformation_scoring.md')
dag_doc_md = ""
if os.path.exists(docs_path):
    with open(docs_path, 'r', encoding='utf-8') as f:
        dag_doc_md = f.read()

with DAG(
    'afklm_02_transformation_scoring',
    default_args=default_args,
    doc_md=dag_doc_md,
    description='Pipeline ELT AF/KLM - Etape 2 : Transformations DBT & Scoring API',
    schedule=None,
    catchup=False,
    tags=['afklm', 'transformation', 'dbt', 'ml'],
) as dag:

    start_tracking = PythonOperator(
        task_id="log_start_transformation",
        python_callable=start_trans_tracking
    )

    run_dbt_models = ExternalPythonOperator(
        task_id='afklm_t_dbt_run',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_transformation,
        op_kwargs={"env_target": "{{ dag_run.conf.get('env_target', 'local') }}"},
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_run_failure")
    )

    run_dbt_tests = ExternalPythonOperator(
        task_id='afklm_t_dbt_test',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_validation,
        op_kwargs={"env_target": "{{ dag_run.conf.get('env_target', 'local') }}"},
        on_failure_callback=operator_failure_callbacks(layer="TRUSTED", event_type="dbt_test_failure")
    )

    compute_ml_predictions = PythonOperator(
        task_id='afklm_ml_compute_predictions',
        python_callable=run_ml_scoring_pipeline,
        op_kwargs={"env_target": "{{ dag_run.conf.get('env_target', 'local') }}"},
        on_failure_callback=operator_failure_callbacks(layer="REFINED", event_type="ml_scoring_failure")
    )

    trigger_fastapi_scoring = ExternalPythonOperator(
        task_id='afklm_ml_trigger_fastapi',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=_trigger_fastapi,
        op_kwargs={"env_target": "{{ dag_run.conf.get('env_target', 'local') }}"},
        on_failure_callback=operator_failure_callbacks(layer="REFINED", event_type="fastapi_reload_failure")
    )

    end_tracking = PythonOperator(
        task_id="log_success_transformation",
        python_callable=success_trans_tracking
    )

    start_tracking >> run_dbt_models >> run_dbt_tests >> compute_ml_predictions >> trigger_fastapi_scoring >> end_tracking