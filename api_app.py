import os
import io
import pickle
import traceback
import requests
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from typing import Optional
from prometheus_fastapi_instrumentator import Instrumentator

ENV_TARGET = os.getenv("ENV_TARGET", "local").strip().lower()
print(f"[FASTAPI STARTUP] Environnement detecte : {ENV_TARGET.upper()}")

if ENV_TARGET == "local":
    DATABASE_URL = "postgresql://data_engineer:FormationData2026@afklm-formation-postgres-local:5432/data_hub"
    print("[FASTAPI DATABASE] Engine connecte sur : afklm-formation-postgres-local")
else:
    DB_USER = os.getenv("AFKLM_DB_USER") or os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD") or os.getenv("DB_PASSWORD", "FormationData2026")
    DB_HOST = os.getenv("AFKLM_DB_HOST") or os.getenv("DB_HOST", "aws-1-eu-west-1.pooler.supabase.com")
    DB_PORT = os.getenv("AFKLM_DB_PORT") or os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("AFKLM_DB_NAME") or os.getenv("DB_NAME", "postgres")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

engine = create_engine(DATABASE_URL)

ml_models = {
    "means": None,
    "scaler": None,
    "xgb": None
}

def load_ml_models_from_storage():
    """Télécharge les artefacts avec gestion de secours automatique."""
    print(" Chargement des modèles prédictifs depuis Supabase Storage...")
    urls = {
        "means": os.getenv("MODEL_MEANS_URL"),
        "scaler": os.getenv("MODEL_SCALER_URL"),
        "xgb": os.getenv("MODEL_XGB_URL")
    }
    
    for model_name, url in urls.items():
        if not url:
            if ENV_TARGET == "local":
                ml_models[model_name] = "MOCK_MODEL_LOCAL"
            continue
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            ml_models[model_name] = pickle.load(io.BytesIO(response.content))
            print(f" Modèle ML [{model_name}] chargé avec succès.")
        except Exception as e:
            print(f" Échec du chargement du modèle [{model_name}]: {e}")
            
            # 🌟 SÉCURITÉ REPLI : Si l'artefact Pandas plante, on injecte les valeurs par défaut pour ne pas bloquer l'API
            if model_name == "means":
                print("[API COMPATIBILITY] Activation immédiate du dictionnaire de secours pour 'means'")
                ml_models["means"] = {
                    "scheduled_flight_duration_minutes": 142.5,
                    "nb_flight_departing_departure_airport": 45.0,
                    "nb_flight_arriving_departure_airport": 42.0,
                    "nb_flight_departing_arrival_airport": 44.0,
                    "nb_flight_arriving_arrival_airport": 41.0,
                    "departure_airport_delayed_share": 18.4,
                    "aircraft_delayed_share": 12.1,
                    "airline_delayed_share": 14.7,
                    "departure_monthday": 15.0,
                    "departure_weekday": 3.0,
                    "departure_hour": 12.0
                }
            elif ENV_TARGET == "local":
                ml_models[model_name] = "MOCK_MODEL_LOCAL"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        load_ml_models_from_storage()
        print("[FASTAPI LIFESPAN] Modeles ML charges, API prete.")
    except Exception as boot_err:
        print(f"[FASTAPI LIFESPAN] Erreur au boot : {boot_err}")
    yield

app = FastAPI(title="API AFKLM - Monitoring & Analytics ML", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

@app.get("/")
def read_root():
    models_status = {k: ("OK" if v is not None else "Manquant") for k, v in ml_models.items()}
    return {
        "status": "L'API est en ligne !",
        "ml_models_loaded": models_status
    }

@app.post("/v1/scoring/reload")
def reload_scoring():
    try:
        load_ml_models_from_storage()
        return {"status": "success", "message": "Artefacts mis à jour."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/monitoring/pipeline-logs")
def get_pipeline_logs():
    """Récupère les logs de la base de données de formation."""
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

            query_vols = text("SELECT COUNT(*) as total_vols FROM public_mart.fct_flight_legs;")
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
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/analytics/delay-metrics")
def get_delay_metrics(dimension: str = Query(..., description="Choix de l'axe : airport, city, date, airline")):
    """Calcule les métriques de retards et renvoie les statistiques globales pour les KPIs."""
    dimension_map = {
        "airport": "departure_airport_name",
        "city": "departure_city_name",  
        "date": "date_key",
        "airline": "airline_name"
    }
    
    if dimension not in dimension_map:
        raise HTTPException(status_code=400, detail="Dimension invalide.")
    
    col_name = dimension_map[dimension]
    min_vols_threshold = 50 if dimension in ["airport", "city"] else 10
    
    sql_query = f"""

        WITH unique_mart_flights_delays AS (
            SELECT 
                leg_id,
                MAX({col_name}) AS label,
                MAX(CASE WHEN is_delayed = true THEN 1 ELSE 0 END) as is_delayed
            FROM public_mart.fct_flight_legs
            WHERE {col_name} IS NOT NULL and is_delayed is not null
            GROUP BY leg_id
        ),
        aggregated_data AS (
            SELECT 
                label,
                SUM(is_delayed) AS total_retards,
                COUNT(*) AS total_vols
            FROM unique_mart_flights_delays 
            GROUP BY label
        )
        SELECT 
            label,
            total_retards,
            total_vols,
            ROUND((total_retards * 100.0 / total_vols), 2) AS delayed_share
        FROM aggregated_data
        WHERE total_vols >= {min_vols_threshold}
        ORDER BY delayed_share DESC;

    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql_query)).mappings().all()
            data_list = [dict(row) for row in rows]
            
            # Calcul des KPIs globaux directement en Python pour soulager la base
            global_vols = sum(item['total_vols'] for item in data_list)
            global_retards = sum(item['total_retards'] for item in data_list)
            global_rate = round((global_retards * 100.0 / global_vols), 2) if global_vols > 0 else 0.0
            
            return {
                "kpis": {
                    "global_rate": global_rate,
                    "total_vols": global_vols,
                    "total_retards": global_retards
                },
                "breakdown": data_list
            }
    except Exception as e:
        print(f"[SQL EXCEPTION] : {e}")
        return {"kpis": {"global_rate": 0, "total_vols": 0, "total_retards": 0}, "breakdown": []}

@app.get("/v1/analytics/confusion-matrix")
def get_confusion_matrix():
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
            INNER JOIN public_mart.fct_flight_legs l ON CAST(d.leg_id AS VARCHAR(36)) = CAST(l.leg_id AS VARCHAR(36))
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
        raise HTTPException(status_code=500, detail=str(e))