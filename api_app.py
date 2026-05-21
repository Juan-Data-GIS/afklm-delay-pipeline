import os
import io
import pickle
import traceback
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from typing import Optional

# 1. Chargement des variables d'environnement
load_dotenv()

# --- CONFIGURATION BASE DE DONNÉES ---
DB_USER = os.getenv("DB_USER", "postgres.amtxaysrmhlznfwqemdu")
DB_PASSWORD = os.getenv("DB_PASSWORD", "FormationData2026")
DB_HOST = os.getenv("DB_HOST", "aws-1-eu-west-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- INITIALISATION FASTAPI & SQLALCHEMY ---
app = FastAPI(title="API AFKLM - Monitoring & Analytics ML")
engine = create_engine(DATABASE_URL)

# --- CACHE LOCAL POUR LES MODÈLES ML (MLOPS GLOBAL) ---
ml_models = {
    "means": None,
    "scaler": None,
    "xgb": None
}

def load_ml_models_from_storage():
    """Télécharge les artefacts Pickle (.pkl) depuis le Storage Supabase."""
    print(" Chargement des modèles prédictifs depuis Supabase Storage...")
    urls = {
        "means": os.getenv("MODEL_MEANS_URL"),
        "scaler": os.getenv("MODEL_SCALER_URL"),
        "xgb": os.getenv("MODEL_XGB_URL")
    }
    
    for model_name, url in urls.items():
        if not url:
            print(f" Variable d'environnement MODEL_{model_name.upper()}_URL manquante.")
            continue
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            ml_models[model_name] = pickle.load(io.BytesIO(response.content))
            print(f" Modèle ML [{model_name}] chargé avec succès.")
        except Exception as e:
            print(f" Échec du chargement du modèle [{model_name}]: {e}")

# Chargement automatique au démarrage de l'API
@app.on_event("startup")
def startup_event():
    load_ml_models_from_storage()


# --- ENDPOINTS API ---

@app.get("/")
def read_root():
    """Check de santé de l'API et statut des modèles ML pour le jury."""
    models_status = {k: ("OK" if v is not None else "Manquant") for k, v in ml_models.items()}
    return {
        "status": "L'API est en ligne !",
        "ml_models_loaded": models_status
    }

@app.post("/v1/scoring/reload")
def reload_scoring():
    """Endpoint déclenché par Airflow pour forcer le rechargement des artefacts ML."""
    try:
        load_ml_models_from_storage()
        return {"status": "success", "message": "Artefacts de scoring ML mis à jour avec succès."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du rafraîchissement ML: {str(e)}")


# --- NEW ENDPOINT 1 : MONITORING PIPELINE (LOGS D'ORCHESTRATION) ---
@app.get("/v1/monitoring/pipeline-logs")
def get_pipeline_logs():
    """Récupère l'historique complet de l'orchestration depuis logs.airflow_events."""
    try:
        with engine.connect() as conn:
            # Correction des colonnes selon le DDL : event_at, "level"
            query = text("""
                SELECT id, event_at, level, layer, dag_id, task_id, run_id, event_type, message 
                FROM logs.airflow_events 
                ORDER BY event_at DESC 
                LIMIT 50;
            """)
            res = conn.execute(query).mappings().all()
            return [dict(row) for row in res]
    except Exception as e:
        print(f" ERREUR REQUÊTE LOGS : {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- NEW ENDPOINT 2 : ANALYTICS ML PAR PARAMÈTRE (DYNAMIQUE) ---
@app.get("/v1/analytics/ml-metrics")
def get_ml_metrics(dimension: str = Query(..., description="Choix de l'axe : airport, city, date, airline")):
    """Calcule le taux de retard prédit par le ML selon l'axe d'analyse choisi par l'utilisateur."""
    
    # Mapping des colonnes SQL selon la dimension sélectionnée sur Streamlit
    dimension_map = {
        "airport": ("departure_airport_name", "LEFT JOIN public_mart.dim_airports a ON dd.departure_airport_key = a.airport_key"),
        "city": ("city_name", "LEFT JOIN public_mart.dim_airports a ON dd.departure_airport_key = a.airport_key"),
        "date": ("date_key", ""),
        "airline": ("airline_name", "")
    }
    
    if dimension not in dimension_map:
        raise HTTPException(status_code=400, detail="Dimension invalide. Choisissez parmi: airport, city, date, airline")
    
    col_name, extra_join = dimension_map[dimension]
    
    # On extrait uniquement les clés de faits dans la première CTE, puis on récupère les libellés 
    # textuels (ville, aéroport) depuis la table de dimension 'dim_airports' (a) lors de la jointure.
    sql_query = f"""
        WITH delays_and_data AS (
            SELECT d.delay_predicted, l.departure_airport_key, l.date_key, l.airline_name 
            FROM public.ml_delays d
            LEFT JOIN public_mart.fct_flight_legs l ON CAST(d.leg_id AS varchar(36)) = l.leg_id
        ),
        delays_data_and_airports AS (
            SELECT dd.* {', a.departure_airport_name, a.city_name' if extra_join else ''} 
            FROM delays_and_data dd 
            {extra_join}
        ) 
        SELECT {col_name} AS label, 
               ROUND(SUM(delay_predicted) * 100.0 / NULLIF(COUNT({col_name}), 0), 2) AS delayed_share
        FROM delays_data_and_airports
        WHERE {col_name} IS NOT NULL
        GROUP BY {col_name} 
        ORDER BY delayed_share DESC;
    """
    
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql_query)).mappings().all()
            return [dict(row) for row in res]
    except Exception as e:
        print(f" ERREUR REQUÊTE ANALYTICS ML ({dimension}) :")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# --- NEW ENDPOINT 3 : MATRICE DE CONFUSION / PERFORMANCE DES PRÉDICTIONS ---
@app.get("/v1/analytics/confusion-matrix")
def get_confusion_matrix():
    """Retourne la répartition VP, FP, VN, FN pour évaluer la pertinence du modèle ML."""
    sql_query = """
        WITH delays_and_data AS (
            SELECT
                CASE
                    WHEN delay_predicted = 1 AND is_delayed = true THEN 'Vrais Positifs (VP)'
                    WHEN delay_predicted = 1 AND is_delayed = false THEN 'Faux Positifs (FP)'
                    WHEN delay_predicted = 0 AND is_delayed = false THEN 'Vrais Négatifs (VN)'
                    WHEN delay_predicted = 0 AND is_delayed = true THEN 'Faux Négatifs (FN)'
                    ELSE 'Inconnu'
                END AS prediction_type
            FROM public.ml_delays d
            LEFT JOIN public_mart.fct_flight_legs l ON CAST(d.leg_id AS VARCHAR(36)) = l.leg_id
        )
        SELECT prediction_type, COUNT(*) AS total
        FROM delays_and_data
        GROUP BY prediction_type;
    """
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql_query)).mappings().all()
            # Transformation en dictionnaire simple pour faciliter la lecture côté Streamlit
            return {row["prediction_type"]: row["total"] for row in res}
    except Exception as e:
        print(" ERREUR REQUÊTE MATRICE DE CONFUSION :")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))