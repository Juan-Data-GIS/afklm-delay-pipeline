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
from pathlib import Path

# Ajoute le répertoire courant (1_ingestion/) au PYTHONPATH.
# Sans cela, "from afklm_source import ..." échouerait si le script est lancé
# depuis la racine du projet (dst_airlines/) plutôt que depuis 1_ingestion/.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import dlt
from afklm_source import afklm_source

if __name__ == "__main__":
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

    # Lance les 3 phases : Extract → Normalize → Load
    #   - afklm_source() lit api_key depuis .dlt/secrets.toml
    #   - afklm_source() lit start_date / end_date depuis .dlt/config.toml
    #   - En mode incrémental (par défaut), reprend depuis la dernière end_date mémorisée
    load_info = pipeline.run(afklm_source())

    # Affiche le résumé du run :
    #   - Nombre de lignes chargées par table
    #   - Durée totale
    #   - Statut des jobs (LOADED / failed)
    print(load_info)
