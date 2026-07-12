"""
afklm_dlt_pipeline.py — Point d'entrée du pipeline dlt AF/KLM.

Usage :
    cd ~/Documents/APPRENTISSAGE/Projet_prod/afklm-delay-pipeline
    source venv/bin/activate
    python ingestion/afklm_dlt_pipeline.py

Ce script est minimal par design : toute la logique métier (appels API,
pagination, normalisation) est dans afklm_source.py.
Les credentials et la configuration sont dans .dlt/secrets.toml et .dlt/config.toml.
"""

import sys
import os
import logging
from pathlib import Path
import dlt

# Ajoute le répertoire courant (ingestion/) au PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from afklm_source import afklm_source

def configure_dlt_destination():
    """Détermine dynamiquement la base de données cible selon le choix fait dans Airflow.
    
    Injecte la variable d'environnement magique attendue par dlt pour surcharger les secrets.
    """
    env_target = os.getenv("ENV_TARGET", "local").strip().lower()
    
    print(f"[DLT ROUTING] Cible détectée : {env_target.upper()}")
    
    if env_target == "local":
        # Surcharge dlt pour pointer sur le conteneur Postgres local défini dans Docker Compose
        connection_url = "postgresql://data_engineer:FormationData2026@postgres_local:5432/data_hub"
        print("[DLT ROUTING] Écriture configurée sur : Postgres Docker Local (data_hub)")
    else:
        # Reconstruction dynamique de l'URL Supabase Cloud à partir des variables globales
        host = os.getenv('AFKLM_DB_HOST')
        port = os.getenv('AFKLM_DB_PORT', '5432')
        user = os.getenv('AFKLM_DB_USER')
        password = os.getenv('AFKLM_DB_PASSWORD')
        dbname = os.getenv('AFKLM_DB_NAME', 'postgres')
        
        connection_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
        print(f"[DLT ROUTING] Écriture configurée sur : Supabase Cloud ({host})")
    
    # Injection dans la clé de configuration standard de dlt
    os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = connection_url


def main():
    # 1. Configuration de l'aiguillage réseau avant d'initialiser le pipeline
    configure_dlt_destination()
    
    # 2. Crée l'objet pipeline dlt.
    # dlt utilisera l'en-tête DESTINATION__POSTGRES__CREDENTIALS injecté juste au-dessus.
    pipeline = dlt.pipeline(
        pipeline_name="afklm",
        destination="postgres",
        dataset_name="public",
    )

    # Récupération dynamique des variables de fenêtrage injectées par le DAG Airflow
    start_date = os.getenv("SOURCES__AFKLM__START_DATE")
    end_date = os.getenv("SOURCES__AFKLM__END_DATE")
    incremental_mode = os.getenv("SOURCES__AFKLM__INCREMENTAL", "True") == "True"

    # Instanciation de la source avec les paramètres calculés par Airflow
    source_instance = afklm_source(
        start_date=start_date,
        end_date=end_date,
        incremental=incremental_mode
    )

    try:
        print(f"[DLT RUN] Début de l'exécution pour la période : {start_date} -> {end_date}")
        # Lance les 3 phases : Extract → Normalize → Load
        load_info = pipeline.run(source_instance)

        # Affiche le résumé du run (tables, lignes chargées, statuts)
        print(load_info)
        
    except Exception as e:
        print(f"[DLT ERROR] Échec critique lors du run : {e}", file=sys.stderr)
        raise e
        
    finally:
        print("[DLT AUDIT] Fin de la séquence DLT — Traçabilité opérationnelle gérée par Airflow.")

# Permet de conserver l'usage du script en exécution manuelle directe
if __name__ == "__main__":
    main()