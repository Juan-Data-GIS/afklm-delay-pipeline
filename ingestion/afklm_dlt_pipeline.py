"""
afklm_dlt_pipeline.py — Point d'entrée du pipeline dlt AF/KLM.

Usage :
    cd ~/Documents/APPRENTISSAGE/dst_airlines
    source venv/bin/activate
    python 1_ingestion/afklm_dlt_pipeline.py

Ce script est minimal par design : toute la logique métier (appels API,
pagination, normalisation) est dans afklm_source.py.
Les credentials et la configuration sont dans .dlt/secrets.toml et .dlt/config.toml.
"""

import sys
import os
import logging
from pathlib import Path
import dlt

# Ajoute le répertoire courant (1_ingestion/) au PYTHONPATH.
# Sans cela, "from afklm_source import ..." échouerait si le script est lancé
# depuis la racine du projet (dst_airlines/) plutôt que depuis 1_ingestion/.
sys.path.insert(0, str(Path(__file__).resolve().parent))

# CORRECTION : Suppression des imports obsolètes liés à logs.job_runs
from afklm_source import afklm_source

def main():
    # Force DLT à afficher ses logs dans la console pour Airflow
    # dlt.init_logging(log_level="INFO") -- obsolète car utilisation print(load_info)
    
    # Crée l'objet pipeline dlt.
    # pipeline_name : identifiant local — le state incrémental est stocké dans
    #   ~/.dlt/pipelines/afklm/ (à supprimer pour réinitialiser).
    # destination   : connecteur "postgres" — les credentials sont lus
    #   automatiquement depuis .dlt/secrets.toml [destination.postgres.credentials].
    # dataset_name  : schéma PostgreSQL cible ("public" = schéma par défaut Supabase).
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
        #   - afklm_source() lit api_key depuis .dlt/secrets.toml
        #   - En mode de production Airflow, remplace la lecture de .dlt/config.toml par les variables d'environnement
        #   - En mode incrémental (par défaut), reprend depuis la dernière end_date mémorisée
        load_info = pipeline.run(source_instance)

        # Affiche le résumé du run :
        #   - Nombre de lignes chargées par table
        #   - Durée totale
        #   - Statut des jobs (LOADED / failed)
        print(load_info)
        
    except Exception as e:
        print(f"[DLT ERROR] Échec critique lors du run : {e}", file=sys.stderr)
        raise e
        
    finally:
        # CORRECTION : Nettoyage de l'appel obsolète. 
        # C'est maintenant Airflow qui gère l'écriture centralisée dans logs.airflow_events via callbacks.
        print("[DLT AUDIT] Fin de la séquence DLT — Traçabilité opérationnelle gérée par Airflow.")

# Permet de conserver l'usage du script en exécution manuelle directe par ton collègue
if __name__ == "__main__":
    main()