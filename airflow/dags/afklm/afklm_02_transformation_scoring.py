"""
afklm_02_transformation_scoring.py — Orchestration Transformations dbt & Scoring ML.

Ce DAG est déclenché automatiquement par le DAG 1 dès que l'ingestion et les tests 
de qualité de données (Data Quality) de production sont validés avec succès.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator
import os

default_args = {
    'owner': 'afklm_analytics_engineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─────────────────────────────────────────────────────────────────────────────
# Fonctions exécutées de manière étanche dans les Venvs (Airflow 3)
# ─────────────────────────────────────────────────────────────────────────────

def run_dbt_transformation():
    """Exécute dbt run via l'API native dbtRunner pour éviter les conflits CLI sous WSL."""
    import sys
    import os
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    print("--- Demarrage de la transformation dbt en mode natif Python ---")
    
    # Configuration explicite des dossiers dbt
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"

    cli_args = [
        "run",
        "--profiles-dir", "/opt/airflow/dbt",
        "--project-dir", "/opt/airflow/dbt",
        "--target", os.getenv("DBT_TARGET", "prod")
    ]

    dbt = dbtRunner()
    result: dbtRunnerResult = dbt.invoke(cli_args)

    if not result.success:
        print("[DBT RUN ERROR] La transformation a echoue.", file=sys.stderr)
        if result.exception:
            print(f"Exception: {result.exception}", file=sys.stderr)
        raise RuntimeError("Echec de la commande dbt run.")
        
    print("--- Transformation dbt executee avec succes ---")


def run_dbt_validation():
    """Exécute dbt test via l'API native dbtRunner."""
    import sys
    import os
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    print("--- Demarrage des tests de modelisation dbt ---")
    
    os.environ["DBT_PROFILES_DIR"] = "/opt/airflow/dbt"
    os.environ["DBT_PROJECT_DIR"] = "/opt/airflow/dbt"

    cli_args = [
        "test",
        "--profiles-dir", "/opt/airflow/dbt",
        "--project-dir", "/opt/airflow/dbt",
        "--target", os.getenv("DBT_TARGET", "prod")
    ]

    dbt = dbtRunner()
    result: dbtRunnerResult = dbt.invoke(cli_args)

    if not result.success:
        print("[DBT TEST ERROR] Un ou plusieurs tests dbt sont en echec.", file=sys.stderr)
        raise RuntimeError("Echec de la commande dbt test.")
        
    print("--- Validation des tests dbt terminee avec succes ---")


def _trigger_fastapi():
    """Appelle directement le conteneur FastAPI sur le réseau Docker pour actualiser le modèle."""
    import requests
    
    url = "http://afklm-fastapi:8000/v1/scoring/reload"
    print(f"[FASTAPI] Envoi de la requete de rechargement du modele sur : {url}")
    
    response = requests.post(url, timeout=30)
    response.raise_for_status()
    print("Scoring recharge avec succes sur l'instance FastAPI.")


# ─────────────────────────────────────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    'afklm_02_transformation_scoring',
    default_args=default_args,
    description='Pipeline ELT AF/KLM - Etape 2 : Transformations DBT & Scoring API',
    schedule=None,
    catchup=False,
    tags=['afklm', 'transformation', 'dbt', 'ml'],
) as dag:
    docs_path = "/opt/airflow/docs/afklm_02_transformation_scoring.md"
    
    if os.path.exists(docs_path):
        with open(docs_path, "r", encoding="utf-8") as f:
            dag.doc_md = f.read()
    else:
        # Message de secours explicite si le fichier n'est pas trouvé lors du parsing
        dag.doc_md = """
    ### Système de Support indisponible
    Le fichier de documentation opérationnelle `afklm_02_transformation_scoring.md` est introuvable dans le dossier cible.
    * **Chemin attendu** : `/opt/airflow/docs/`
    * **Action requise** : Vérifier le montage du volume ou la présence du fichier dans le répertoire source.
    """

    # Tâche 1 : Exécution des modèles (1_raw -> 2_int -> 3_mart)
    run_dbt_models = ExternalPythonOperator(
        task_id='afklm_t_dbt_run',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_transformation,
    )

    # Tâche 2 : Exécution des tests dbt (clés primaires, non null, etc.)
    run_dbt_tests = ExternalPythonOperator(
        task_id='afklm_t_dbt_test',
        python='/home/airflow/dbt_venv/bin/python',
        python_callable=run_dbt_validation,
    )

    # Tâche 3 : Inférence / Rechargement de l'API de scoring ML
    trigger_fastapi_scoring = ExternalPythonOperator(
        task_id='afklm_ml_trigger_fastapi',
        python='/home/airflow/pipeline_venv/bin/python',
        python_callable=_trigger_fastapi,
    )

    # Définition du flux de dépendance linéaire
    run_dbt_models >> run_dbt_tests >> trigger_fastapi_scoring