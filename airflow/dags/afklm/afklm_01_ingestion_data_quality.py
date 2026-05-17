"""
afklm_01_ingestion_data_quality.py — Orchestration Ingestion & Qualité de Production.

Ce DAG s'exécute automatiquement tous les jours pour ingérer les données de J-1.
Il permet également un déclenchement manuel (Backfill) depuis l'interface Airflow
en spécifiant une date ou un intervalle de dates, traité une journée à la fois.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import os

default_args = {
    'owner': 'afklm_data_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─────────────────────────────────────────────────────────────────────────────
# Logique d'encapsulation pour l'environnement virtuel isolé (Airflow 3)
# ─────────────────────────────────────────────────────────────────────────────

def run_dlt_script(airflow_start: str, airflow_end: str, logical_date_str: str):
    """Fonction exécutée dans le venv pour lancer DLT avec gestion des dates."""
    import sys
    import os
    from datetime import datetime
    
    # Ajout du chemin pour localiser les scripts
    sys.path.insert(0, '/opt/airflow/ingestion')
    os.environ['DLT_PROJECT_DIR'] = '/opt/airflow'
    
    # Nettoyage des chaînes reçues par Airflow
    airflow_start = airflow_start.strip()
    airflow_end = airflow_end.strip()
    
    # Logique de calcul des dates déterministe
    if not airflow_start:
        # Mode automatique : On utilise la date logique d'Airflow (Date d'exécution théorique du run)
        # exemple de logical_date_str : "2026-05-17T00:00:00+00:00"
        target_date = datetime.fromisoformat(logical_date_str)
        day_start = target_date.strftime("%Y-%m-%dT00:00:00Z")
        day_end = target_date.strftime("%Y-%m-%dT23:59:59Z")
        print(f"[MODE AUTOMATIQUE] Traitement de la date logique du run : {target_date.date()}")
    else:
        # Mode manuel / Backfill
        day_start = f"{airflow_start}T00:00:00Z"
        day_end = f"{airflow_end}T23:59:59Z" if airflow_end else f"{airflow_start}T23:59:59Z"
        print(f"[MODE BACKFILL] Traitement manuel de la periode : {day_start} -> {day_end}")
    
    # Surcharge des variables d'environnement lues nativement par DLT
    os.environ["SOURCES__AFKLM__START_DATE"] = day_start
    os.environ["SOURCES__AFKLM__END_DATE"] = day_end
    os.environ["SOURCES__AFKLM__INCREMENTAL"] = "False"  # Force le respect strict de la fenêtre
    
    # Exécution explicite de la fonction main du pipeline
    from afklm_dlt_pipeline import main
    print("[DLT START] Execution du pipeline d'ingestion...")
    main()
    print("Ingestion incrementale DLT Air France-KLM terminee avec succes.")


def run_verify_script():
    """Fonction exécutée dans le venv pour lancer les vérifications Data Quality."""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    
    from verify_ingestion import main_verify
    print("[DQ START] Lancement des tests de validation sur Supabase...")
    main_verify()
    print("Validation de la qualite des donnees validee.")


# ─────────────────────────────────────────────────────────────────────────────
# Définition de la topologie du DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    'afklm_01_ingestion_data_quality',
    default_args=default_args,
    description='Pipeline ELT AF/KLM - Etape 1 : Ingestion DLT & Data Quality (J-1 / Backfill)',
    schedule='@daily',
    catchup=False,
    tags=['afklm', 'ingestion', 'dlt'],
    params={
        "start_date": Param(
            default="",
            type="string",
            description="Optionnel (Format: YYYY-MM-DD). Si laisse vide, le pipeline traitera automatiquement Hier (J-1)."
        ),
        "end_date": Param(
            default="",
            type="string",
            description="Optionnel (Format: YYYY-MM-DD). Si laisse vide, seule la journee de start_date sera traitee."
        ),  
    },
) as dag:
    docs_path = "/opt/airflow/docs/afklm_01_ingestion_data_quality.md"
    
    if os.path.exists(docs_path):
        with open(docs_path, "r", encoding="utf-8") as f:
            dag.doc_md = f.read()
    else:
        dag.doc_md = "### Attention : Fichier afklm_01_ingestion_data_quality.md introuvable dans le volume docs."

    # Tâche 1 : Extraction & Chargement (EL) via DLT
    run_dlt_pipeline = ExternalPythonOperator(
        task_id='afklm_el_dlt_pipeline',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_dlt_script,
        op_kwargs={
            "airflow_start": "{{ params.start_date }}",
            "airflow_end": "{{ params.end_date }}",
            "logical_date_str": "{{ data_interval_start }}", # Injection de la borne de début de l'intervalle Airflow
        },
    )

    # Tâche 2 : Validation Data Quality de Production
    verify_ingestion_quality = ExternalPythonOperator(
        task_id='afklm_dq_verify_ingestion',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=run_verify_script,
    )

    # Tâche 3 : Déclenchement automatique du DAG de transformation et de scoring
    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_transformation_scoring",
        trigger_dag_id="afklm_02_transformation_scoring",
        wait_for_completion=False,
    )

    # Flux de dépendance linéaire et robuste
    run_dlt_pipeline >> verify_ingestion_quality >> trigger_next_dag