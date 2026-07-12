from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_insert_weather(**kwargs):
    execution_date_str = kwargs.get('ds')
    started_at = datetime.now()
    job_name = "openmeteo_ingestion"
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    
    # Insertion initiale avec les colonnes garanties valides
    cursor.execute("""
        INSERT INTO logs.job_runs (job_name, layer, status, execution_date, started_at)
        VALUES (%s, 'bronze', 'RUNNING', %s, %s)
        RETURNING id;
    """, (job_name, execution_date_str, started_at))
    log_id = cursor.fetchone()[0]
    conn.commit()

    try:
        cursor.execute("SELECT airport_code, latitude, longitude FROM public.referentiel_airports;")
        airports = cursor.fetchall()
        
        if not airports:
            print("Aucun aéroport trouvé dans public.referentiel_airports.")
            raise ValueError("Referentiel airports vide")

        for code, lat, lon in airports:
            print(f"Requêtage Open-Meteo pour {code} à la date {execution_date_str}")
            
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(lat),
                "longitude": float(lon),
                "start_date": execution_date_str,
                "end_date": execution_date_str,
                "hourly": "temperature_2m,precipitation,wind_speed_10m",
                "timezone": "Europe/Paris"
            }
            
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Warning: Impossible de joindre l'API pour {code}")
                continue
                
            data = response.json()
            hourly = data.get("hourly", {})
            
            for idx, ts in enumerate(hourly.get("time", [])):
                cursor.execute("""
                    INSERT INTO bronze.b_openmeteo_weather 
                    (airport_code, weather_timestamp, temperature_2m, precipitation, wind_speed_10m)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (airport_code, weather_timestamp) 
                    DO UPDATE SET 
                        temperature_2m = EXCLUDED.temperature_2m,
                        precipitation = EXCLUDED.precipitation,
                        wind_speed_10m = EXCLUDED.wind_speed_10m,
                        fetched_at = CURRENT_TIMESTAMP;
                """, (
                    code,
                    ts,
                    hourly["temperature_2m"][idx],
                    hourly["precipitation"][idx],
                    hourly["wind_speed_10m"][idx]
                ))
                
        # Mise à jour de sécurité : uniquement sur status et ended_at pour éviter les colonnes manquantes
        ended_at = datetime.now()
        cursor.execute("""
            UPDATE logs.job_runs 
            SET status = 'SUCCESS', ended_at = %s
            WHERE id = %s;
        """, (ended_at, log_id))
        conn.commit()
        print("Pipeline achevé avec succès.")

    except Exception as e:
        conn.rollback()
        ended_at = datetime.now()
        cursor.execute("""
            UPDATE logs.job_runs 
            SET status = 'FAILED', ended_at = %s
            WHERE id = %s;
        """, (ended_at, log_id))
        conn.commit()
        print(f"Erreur durant l'exécution du pipeline : {str(e)}")
        raise e
        
    finally:
        cursor.close()
        conn.close()


with DAG(
    'openmeteo_01_ingestion_weather',
    default_args=default_args,
    description='Pipeline classique Airflow pour l ingestion meteo vers le schema bronze',
    schedule='0 3 * * *',
    catchup=True,
    is_paused_upon_creation=True,          
    max_active_runs=1  
) as dag:

    openmeteo_docs_path = "/opt/airflow/docs/openmeteo_01_ingestion_weather.md"
    if os.path.exists(openmeteo_docs_path):
        with open(openmeteo_docs_path, "r", encoding="utf-8") as f:
            dag.doc_md = f.read()

    process_weather = PythonOperator(
        task_id='openmeteo_sensor_and_insert',
        python_callable=fetch_and_insert_weather
    )