import os
import io
import pickle
import traceback
import requests
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# 1. Chargement des variables d'environnement (AVANT toute initialisation)
load_dotenv()

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from typing import Optional
from prometheus_fastapi_instrumentator import Instrumentator

# --- ARCHITECTURE DATAOPS : IMPORT DU SYSTEME DE LOGS PARTAGE ---
# Ce module provient de ton dossier plugins projeté dans le conteneur
from utils.monitoring_utils import log_event

# --- CONFIGURATION DYNAMIQUE DE LA BASE DE DONNÉES ---
ENV_TARGET = os.getenv("ENV_TARGET", "local").strip().lower()
print(f"[FASTAPI STARTUP] Environnement detecte : {ENV_TARGET.upper()}")

if ENV_TARGET == "local":
    # Connexion vers le conteneur Docker local (Pas de SSL requis)
    DATABASE_URL = "postgresql://data_engineer:FormationData2026@postgres_local:5432/data_hub"
    print("[FASTAPI DATABASE] Engine connecte sur : Postgres Docker Local")
else:
    # Récupération dynamique des variables de production Supabase Cloud
    DB_USER = os.getenv("AFKLM_DB_USER") or os.getenv("DB_USER", "postgres.amtxaysrmhlznfwqemdu")
    DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD") or os.getenv("DB_PASSWORD", "FormationData2026")
    DB_HOST = os.getenv("AFKLM_DB_HOST") or os.getenv("DB_HOST", "aws-1-eu-west-1.pooler.supabase.com")
    DB_PORT = os.getenv("AFKLM_DB_PORT") or os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("AFKLM_DB_NAME") or os.getenv("DB_NAME", "postgres")
    
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    print(f"[FASTAPI DATABASE] Engine connecte sur : Supabase Cloud ({DB_HOST})")

engine = create_engine(DATABASE_URL)

# --- CACHE LOCAL POUR LES MODÈLES ML (MLOPS GLOBAL) ---
ml_models = {
    "means": None,
    "scaler": None,
    "xgb": None
}

def load_ml_models_from_storage():
    """Télécharge les artefacts Pickle (.pkl) depuis le Storage Supabase ou Mock en local."""
    print(" Chargement des modèles prédictifs depuis Supabase Storage...")
    urls = {
        "means": os.getenv("MODEL_MEANS_URL"),
        "scaler": os.getenv("MODEL_SCALER_URL"),
        "xgb": os.getenv("MODEL_XGB_URL")
    }
    
    for model_name, url in urls.items():
        if not url:
            print(f" Variable d'environnement MODEL_{model_name.upper()}_URL manquante.")
            if ENV_TARGET == "local":
                print(f" Mode local : Simulation d'artefact pour {model_name}")
                ml_models[model_name] = "MOCK_MODEL_LOCAL"
            continue
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            ml_models[model_name] = pickle.load(io.BytesIO(response.content))
            print(f" Modèle ML [{model_name}] chargé avec succès.")
        except Exception as e:
            print(f" Échec du chargement du modèle [{model_name}]: {e}")
            if ENV_TARGET == "local":
                ml_models[model_name] = "MOCK_MODEL_LOCAL"

# --- MANAGEMENT DU CYCLE DE VIE (LIFESPAN) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Enregistrement du démarrage nominal de l'API dans les logs centralisés
    try:
        load_ml_models_from_storage()
        log_event(
            level="INFO",
            layer="API",
            dag_id="system_startup",
            task_id="fastapi_lifespan",
            event_type="api_boot_success",
            message=f"Démarrage nominal du backend FastAPI sur l'environnement {ENV_TARGET.upper()}."
        )
    except Exception as boot_err:
        log_event(
            level="ERROR",
            layer="API",
            dag_id="system_startup",
            task_id="fastapi_lifespan",
            event_type="api_boot_failure",
            message=f"Incident lors du boot de l'API : {boot_err}"
        )
    yield


# --- INITIALISATION UNIQUE DE FASTAPI ---
app = FastAPI(title="API AFKLM - Monitoring & Analytics ML", lifespan=lifespan)

# --- CONFIGURATION PROMETHEUS ---
Instrumentator().instrument(app).expose(app)


# --- ENDPOINTS API ---

@app.get("/")
def read_root():
    """Check de santé de l'API et statut des modèles ML pour le jury."""
    models_status = {k: ("OK" if v is not None else "Manquant") for k, v in ml_models.items()}
    return {
        "status": f"L'API est en ligne sur l'environnement {ENV_TARGET.upper()} !",
        "ml_models_loaded": models_status
    }

@app.post("/v1/scoring/reload")
def reload_scoring():
    """Endpoint déclenché par Airflow pour forcer le rechargement des artefacts ML."""
    try:
        load_ml_models_from_storage()
        
        # LOGS CENTRALISÉS : Notification de succès de la synchronisation Airflow -> API
        log_event(
            level="INFO",
            layer="REFINED",
            dag_id="afklm_02_transformation_scoring",
            task_id="afklm_ml_trigger_fastapi",
            event_type="api_model_reload_success",
            message="Webhook exécuté : Les derniers artefacts du modèle XGBoost ont été rechargés en mémoire vive."
        )
        return {"status": "success", "message": f"Artefacts de scoring ML mis à jour avec succès sur {ENV_TARGET}."}
    except Exception as e:
        # LOGS CENTRALISÉS : Notification de l'incident critique
        log_event(
            level="ERROR",
            layer="REFINED",
            dag_id="afklm_02_transformation_scoring",
            task_id="afklm_ml_trigger_fastapi",
            event_type="api_model_reload_failure",
            message=f"Échec du rechargement à chaud du modèle ML via Webhook : {str(e)}"
        )
        raise HTTPException(status_code=500, detail=f"Erreur lors du rafraîchissement ML: {str(e)}")


# --- ENDPOINT 1 : MONITORING PIPELINE (LOGS D'ORCHESTRATION) ---
@app.get("/v1/monitoring/pipeline-logs")
def get_pipeline_logs():
    """Récupère les logs d'orchestration et calcule les métriques DataOps globales."""
    try:
        with engine.connect() as conn:
            query_logs = text("""
                SELECT id, event_at, level, layer, dag_id, task_id, event_type, message 
                FROM logs.airflow_events 
                ORDER BY event_at DESC 
                LIMIT 50;
            """)
            res_logs = conn.execute(query_logs).mappings().all()
            list_logs = [dict(row) for row in res_logs]

            query_metrics = text("""
                SELECT 
                    COUNT(*) as total_events,
                    ROUND(COUNT(CASE WHEN level = 'ERROR' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as error_rate,
                    EXTRACT(DAY FROM (MAX(event_at) - MIN(event_at))) + 1 as days_covered
                FROM logs.airflow_events;
            """)
            metrics_res = conn.execute(query_metrics).mappings().first()

            total_events = metrics_res["total_events"] if metrics_res and metrics_res["total_events"] is not None else 0
            error_rate = metrics_res["error_rate"] if metrics_res and metrics_res["error_rate"] is not None else 0.0
            days_covered = int(metrics_res["days_covered"]) if metrics_res and metrics_res["days_covered"] is not None else 1

            query_vols = text("""
                SELECT COUNT(*) as total_vols 
                FROM public_mart.fct_flight_legs;
            """)
            
            try:
                vols_res = conn.execute(query_vols).mappings().first()
                total_vols = vols_res["total_vols"] if vols_res and vols_res["total_vols"] is not None else 0
            except:
                total_vols = 0

            return {
                "metrics": {
                    "total_events": total_events,
                    "error_rate": error_rate,
                    "days_covered": days_covered,
                    "total_vols_mart": total_vols
                },
                "logs": list_logs
            }
            
    except Exception as e:
        print(f" ERREUR PIPELINE LOGS ENRICHIE : {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ENDPOINT 2 : ANALYTICS ML PAR PARAMÈTRE (DYNAMIQUE) ---
@app.get("/v1/analytics/ml-metrics")
def get_ml_metrics(dimension: str = Query(..., description="Choix de l'axe : airport, city, date, airline")):
    """Calcule le taux de retard prédit par le ML en exploitant directement les colonnes riches de fct_flight_legs."""
    
    dimension_map = {
        "airport": "departure_airport_name",
        "city": "departure_airport_name",  
        "date": "date_key",
        "airline": "airline_name"
    }
    
    if dimension not in dimension_map:
        raise HTTPException(status_code=400, detail="Dimension invalide.")
    
    col_name = dimension_map[dimension]
    
    sql_query = f"""
        WITH delays_and_data AS (
            SELECT d.delay_predicted, l.{col_name}
            FROM public.ml_delays d
            LEFT JOIN public_mart.fct_flight_legs l ON CAST(d.leg_id AS varchar(36)) = l.leg_id
        )
        SELECT {col_name} AS label, 
               ROUND(SUM(delay_predicted) * 100.0 / NULLIF(COUNT({col_name}), 0), 2) AS delayed_share
        FROM delays_and_data
        WHERE {col_name} IS NOT NULL
        GROUP BY {col_name} 
        ORDER BY delayed_share DESC;
    """
    
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql_query)).mappings().all()
            return [dict(row) for row in res]
    except Exception as e:
        print(f" ERREUR REQUÊTE ANALYTICS ML ({dimension}) : {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ENDPOINT 3 : MATRICE DE CONFUSION / PERFORMANCE DES PRÉDICTIONS ---
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
            return {row["prediction_type"]: row["total"] for row in res}
    except Exception as e:
        print(" ERREUR REQUÊTE MATRICE DE CONFUSION :")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))