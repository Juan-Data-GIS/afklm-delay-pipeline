"""
afklm_01_ingestion_data_quality.py — Orchestration Ingestion & Qualité de Production.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import sys
import os

sys.path.insert(0, '/opt/airflow/dags')
from utils.monitoring_utils import operator_failure_callbacks, log_operator_success

default_args = {
    'owner': 'afklm_data_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_dlt_script(airflow_start: str, airflow_end: str, logical_date_str: str):
    import sys
    import os
    from datetime import datetime
    
    sys.path.insert(0, '/opt/airflow/ingestion')
    os.environ['DLT_PROJECT_DIR'] = '/opt/airflow'
    
    airflow_start = airflow_start.strip()
    airflow_end = airflow_end.strip()
    
    if not airflow_start:
        target_date = datetime.fromisoformat(logical_date_str)
        day_start = target_date.strftime("%Y-%m-%dT00:00:00Z")
        day_end = target_date.strftime("%Y-%m-%dT23:59:59Z")
    else:
        day_start = f"{airflow_start}T00:00:00Z"
        day_end = f"{airflow_end}T23:59:59Z" if airflow_end else f"{airflow_start}T23:59:59Z"
    
    os.environ["SOURCES__AFKLM__START_DATE"] = day_start
    os.environ["SOURCES__AFKLM__END_DATE"] = day_end
    os.environ["SOURCES__AFKLM__INCREMENTAL"] = "False"
    
    from afklm_dlt_pipeline import main
    main()

def run_verify_script():
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from verify_ingestion import main_verify
    main_verify()


with DAG(
    'afklm_01_ingestion_data_quality',
    default_args=default_args,
    description='Pipeline ELT AF/KLM - Etape 1 : Ingestion DLT & Data Quality',
    schedule='@daily',
    catchup=False,
    tags=['afklm', 'ingestion', 'dlt'],
    params={
        "start_date": Param(default="", type="string"),
        "end_date": Param(default="", type="string"),  
    },
) as dag:

    run_dlt_pipeline = ExternalPythonOperator(
        task_id='afklm_el_dlt_pipeline',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_dlt_script,
        op_kwargs={
            "airflow_start": "{{ params.start_date }}",
            "airflow_end": "{{ params.end_date }}",
            "logical_date_str": "{{ data_interval_start }}",
        },
        on_failure_callback=operator_failure_callbacks(layer="RAW_INBOUND", event_type="ingestion_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="RAW_INBOUND", event_type="ingestion_success")
    )

    verify_ingestion_quality = ExternalPythonOperator(
        task_id='afklm_dq_verify_ingestion',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_verify_script,
        on_failure_callback=operator_failure_callbacks(layer="RAW_INBOUND", event_type="quality_check_failure"),
        on_success_callback=lambda context: log_operator_success(context, layer="RAW_INBOUND", event_type="quality_check_success")
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_transformation_scoring",
        trigger_dag_id="afklm_02_transformation_scoring",
        wait_for_completion=False,
        on_success_callback=lambda context: log_operator_success(context, layer="ORCHESTRATION", event_type="dag_trigger_success")
    )

    run_dlt_pipeline >> verify_ingestion_quality >> trigger_next_dag