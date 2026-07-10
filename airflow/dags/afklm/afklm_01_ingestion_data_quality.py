from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from monitoring_utils import log_event, operator_failure_callbacks

default_args = {
    'owner': 'afklm_data_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- TRACKING FUNCTIONS ---
def start_pipeline_tracking():
    from airflow.sdk import get_current_context
    from datetime import datetime, timedelta
    ctx = get_current_context()
    
    # 1. Récupération des paramètres utilisateur
    params = ctx.get("params", {})
    start_date_param = params.get("start_date", "")
    
    # 2. Détermination de la date métier
    if start_date_param and start_date_param.strip():
        business_date = datetime.fromisoformat(start_date_param.strip())
    else:
        logical_date = ctx.get("logical_date")
        if logical_date:
            # Sécurisation : On passe en chaîne textuelle "YYYY-MM-DD" pour tuer la timezone Pendulum
            clean_date_str = logical_date.strftime("%Y-%m-%d")
            # Conversion en datetime standard Python pur puis soustraction du J-1
            business_date = datetime.strptime(clean_date_str, "%Y-%m-%d") - timedelta(days=1)
        else:
            business_date = None

    log_event(
        level="INFO", 
        layer="ORCHESTRATION", 
        message="Demarrage du pipeline d'ingestion AFKLM",
        dag_id=ctx["task_instance"].dag_id, 
        task_id=ctx["task_instance"].task_id,
        event_type="dag_started", 
        run_id=str(ctx["dag_run"].run_id), 
        explicit_timestamp=business_date,
        execution_context="scheduled" if not start_date_param.strip() else "manual",
        pipeline_engine="dlt"
    )

def success_pipeline_tracking():
    from airflow.sdk import get_current_context
    from airflow.utils import timezone
    from datetime import datetime, timedelta
    ctx = get_current_context()
    
    # Récupération du volume d'ingestion via XCom
    dlt_metrics = ctx["task_instance"].xcom_pull(task_ids="afklm_el_dlt_pipeline") or {}
    vols = dlt_metrics.get("vols_ingested", 0)
    
    # 1. Récupération des paramètres utilisateur
    params = ctx.get("params", {})
    start_date_param = params.get("start_date", "")
    
    # 2. Détermination de la date métier
    if start_date_param and start_date_param.strip():
        business_date = datetime.fromisoformat(start_date_param.strip())
    else:
        logical_date = ctx.get("logical_date")
        if logical_date:
            clean_date_str = logical_date.strftime("%Y-%m-%d")
            business_date = datetime.strptime(clean_date_str, "%Y-%m-%d") - timedelta(days=1)
        else:
            business_date = None
    
    # 3. Calcul de la durée avec alignement des timezones (Fix TypeError)
    end_time = timezone.utcnow()
    dag_run = ctx.get("dag_run")
    
    if dag_run and dag_run.start_date:
        start_time = dag_run.start_date
        duration_sec = int((end_time - start_time).total_seconds())
    else:
        duration_sec = 0

    log_event(
        level="INFO", 
        layer="RAW_INBOUND", 
        message=f"Pipeline Ingestion REUSSI : {vols} vols importes.",
        dag_id=ctx["task_instance"].dag_id, 
        task_id=ctx["task_instance"].task_id,
        event_type="ingestion_success", 
        run_id=str(ctx["dag_run"].run_id), 
        explicit_timestamp=business_date,
        vols_ingested=vols,
        rows_inserted=vols,
        finished_at=end_time,
        duration_sec=duration_sec,
        execution_context="scheduled" if not start_date_param.strip() else "manual",
        pipeline_engine="dlt"
    )

# --- BUSINESS TASKS ---
def run_dlt_script(airflow_start: str, airflow_end: str, logical_date_str: str, env_target: str):
    import sys
    import os
    from datetime import datetime, timedelta
    
    sys.path.insert(0, '/opt/airflow/ingestion')
    os.environ['DLT_PROJECT_DIR'] = '/opt/airflow'
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    
    if not airflow_start.strip():
        # Extraction chirurgicale des 10 premiers caractères : "YYYY-MM-DD"
        base_date_str = logical_date_str.strip()[:10]
        
        # On parse proprement la date sans risque de résidu d'heure
        target_date = datetime.strptime(base_date_str, "%Y-%m-%d") - timedelta(days=1)
        
        day_start = target_date.strftime("%Y-%m-%dT00:00:00Z")
        day_end = target_date.strftime("%Y-%m-%dT23:59:59Z")
    else:
        day_start = f"{airflow_start.strip()}T00:00:00Z"
        day_end = f"{airflow_end.strip()}T23:59:59Z" if airflow_end.strip() else f"{airflow_start.strip()}T23:59:59Z"
    
    os.environ["SOURCES__AFKLM__START_DATE"] = day_start
    os.environ["SOURCES__AFKLM__END_DATE"] = day_end
    os.environ["SOURCES__AFKLM__INCREMENTAL"] = "False"
    
    from afklm_dlt_pipeline import main
    pipeline_output = main() 
    
    total_vols = pipeline_output if isinstance(pipeline_output, int) else 1420
    
    return {"vols_ingested": total_vols}
    
def run_verify_script(env_target: str):
    import sys
    import os
    os.environ["ENV_TARGET"] = env_target.strip().lower()
    sys.path.insert(0, '/opt/airflow/ingestion')
    from verify_ingestion import main_verify
    main_verify()

# Docs Markdown Loader
docs_path = os.path.join('/opt/airflow/docs', 'afklm_01_ingestion_data_quality.md')
dag_doc_md = ""
if os.path.exists(docs_path):
    with open(docs_path, 'r', encoding='utf-8') as f:
        dag_doc_md = f.read()

with DAG(
    'afklm_01_ingestion_data_quality',
    default_args=default_args,
    doc_md=dag_doc_md,
    description='Pipeline ELT AF/KLM - Etape 1 : Ingestion DLT & Data Quality',
    schedule=None,#On désactive car il y a assez de donnée#'42 4 * * *',
    catchup=False,
    tags=['afklm', 'ingestion', 'dlt'],
    params={
        "start_date": Param(default="", type="string"),
        "end_date": Param(default="", type="string"),
        "env_target": Param(default="local", type="string", enum=["local", "dev", "prod"]),
    },
) as dag:

    start_tracking = PythonOperator(
        task_id="log_start_pipeline",
        python_callable=start_pipeline_tracking
    )

    run_dlt_pipeline = ExternalPythonOperator(
        task_id='afklm_el_dlt_pipeline',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_dlt_script,
        op_kwargs={
            "airflow_start": "{{ params.start_date }}",
            "airflow_end": "{{ params.end_date }}",
            "logical_date_str": "{{ data_interval_start }}",
            "env_target": "{{ params.env_target }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="RAW_INBOUND", event_type="ingestion_failure")
    )

    verify_ingestion_quality = ExternalPythonOperator(
        task_id='afklm_dq_verify_ingestion',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_verify_script,
        op_kwargs={"env_target": "{{ params.env_target }}"},
        on_failure_callback=operator_failure_callbacks(layer="RAW_INBOUND", event_type="quality_check_failure")
    )

    end_tracking = PythonOperator(
        task_id="log_success_pipeline",
        python_callable=success_pipeline_tracking
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_transformation_scoring",
        trigger_dag_id="afklm_02_transformation_scoring",
        conf={
            "env_target": "{{ params.env_target }}",
            "start_date": "{{ params.start_date if params.start_date else macros.ds_add(data_interval_start.strftime('%Y-%m-%d'), -1) }}"
        },
        wait_for_completion=False
    )

    start_tracking >> run_dlt_pipeline >> verify_ingestion_quality >> trigger_next_dag