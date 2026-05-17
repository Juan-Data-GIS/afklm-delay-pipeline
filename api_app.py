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
app = FastAPI(title="API AFKLM Production & ML Scoring")
engine = create_engine(DATABASE_URL)

# --- CACHE LOCAL POUR LES MODÈLES ML (MLOPS GLOBAL) ---
ml_models = {
    "means": None,
    "scaler": None,
    "xgb": None
}

def load_ml_models_from_storage():
    """Télécharge les artefacts Pickle (.pkl) depuis le Storage Supabase."""
    print("🔄 Chargement des modèles prédictifs depuis Supabase Storage...")
    urls = {
        "means": os.getenv("MODEL_MEANS_URL"),
        "scaler": os.getenv("MODEL_SCALER_URL"),
        "xgb": os.getenv("MODEL_XGB_URL")
    }
    
    for model_name, url in urls.items():
        if not url:
            print(f"⚠️ Variable d'environnement MODEL_{model_name.upper()}_URL manquante.")
            continue
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            # Utilisation de io.BytesIO pour charger le flux binaire pickle
            ml_models[model_name] = pickle.load(io.BytesIO(response.content))
            print(f"✅ Modèle ML [{model_name}] chargé avec succès.")
        except Exception as e:
            print(f"❌ Échec du chargement du modèle [{model_name}]: {e}")

# Chargement asynchrone / automatique au démarrage de l'API
@app.on_event("startup")
def startup_event():
    load_ml_models_from_storage()


# --- ENDPOINTS API ---

@app.get("/")
def read_root():
    # Petit check pour confirmer au jury si l'API possède bien ses modèles ML
    models_status = {k: ("OK" if v is not None else "Manquant") for k, v in ml_models.items()}
    return {
        "status": "L'API est en ligne !",
        "ml_models_loaded": models_status
    }

@app.post("/v1/scoring/reload")
def reload_scoring():
    """Endpoint déclenché par le DAG Airflow 2 après les transformations dbt.
    
    Il force le rechargement des artefacts ML ou réexécute la logique de scoring.
    """
    try:
        load_ml_models_from_storage()
        return {"status": "success", "message": "Artefacts de scoring ML mis à jour avec succès."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du rafraîchissement ML: {str(e)}")

@app.get("/flight-stats")
def get_flight_stats(
    date: Optional[str] = Query(None),
    airline: Optional[str] = Query(None)
):
    try:
        with engine.connect() as conn:
            where_clauses = []
            params = {}
            
            # --- FILTRES ---
            if date:
                where_clauses.append("CAST(flight_date AS DATE) = CAST(:d AS DATE)")
                params["d"] = date
                
            if airline and airline != "ALL":
                where_clauses.append("airline_code = :a")
                params["a"] = airline
                
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            print(f"Exécution requête avec filtres : {where_sql} | Paramètres : {params}")
            
            # --- REQUÊTES (Format universel row[0], row[1]) ---
            total = conn.execute(text(f"SELECT COUNT(*) FROM silver.s_flights {where_sql}"), params).scalar()
            
            status_res = conn.execute(text(f"SELECT flight_status, COUNT(*) FROM silver.s_flights {where_sql} GROUP BY flight_status"), params)
            statuses = {row[0]: row[1] for row in status_res}
            
            routes_res = conn.execute(text(f"SELECT origin_iata || ' ➔ ' || destination_iata, COUNT(*) as count FROM silver.s_flights {where_sql} GROUP BY origin_iata, destination_iata ORDER BY count DESC LIMIT 5"), params)
            top_routes = [{"route": row[0], "count": row[1]} for row in routes_res]

            airlines_res = conn.execute(text(f"SELECT airline_code, COUNT(*) as count FROM silver.s_flights {where_sql} GROUP BY airline_code ORDER BY count DESC LIMIT 5"), params)
            top_airlines = [{"airline_code": row[0], "count": row[1]} for row in airlines_res]

        return {
            "total_flights": total or 0,
            "statuses": statuses,
            "top_routes": top_routes,
            "top_airlines": top_airlines
        }
    except Exception as e:
        print("ERREUR CRITIQUE DANS L'API :")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/monitoring-stats")
def get_monitoring_stats():
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    COUNT(DISTINCT execution_date) as days,
                    SUM(records_processed) as total_rows,
                    ROUND(CAST(SUM(records_error) AS NUMERIC) / NULLIF(SUM(records_processed) + SUM(records_error), 0) * 100, 2) as error_rate,
                    (SELECT status FROM logs.job_runs ORDER BY started_at DESC LIMIT 1) as last_status
                FROM logs.job_runs
                WHERE layer = 'BRONZE'
            """)
            res = conn.execute(query).mappings().first()
            
            return {
                "days": res["days"] or 0,
                "total_rows": res["total_rows"] or 0,
                "error_rate": float(res["error_rate"]) if res["error_rate"] else 0.0,
                "last_status": res["last_status"] or "N/A"
            }
    except Exception as e:
        print(f"ERREUR MONITORING : {e}")
        return {"days": 0, "total_rows": 0, "error_rate": 0, "last_status": "ERROR"}