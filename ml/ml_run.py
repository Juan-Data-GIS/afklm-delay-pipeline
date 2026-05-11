"""
Script ML pour la prédiction des retards AF/KLM.
Lit silver_mart.fct_flight_legs, prépare les observations et applique le modèle,
écrit les prédictions dans ml_delays.
"""

#update libs
import os
import re
from pathlib import Path
import joblib
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sqlalchemy import create_engine, text
import urllib.request
import pickle

# Config DB depuis variables d'environnement 
DB_HOST = os.getenv("AFKLM_DB_HOST", "localhost")
DB_PORT = os.getenv("AFKLM_DB_PORT", "5432")
DB_USER = os.getenv("AFKLM_DB_USER", "postgres")
DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD", "")
DB_NAME = os.getenv("AFKLM_DB_NAME", "postgres")
DB_SSLMODE = os.getenv("AFKLM_DB_SSLMODE", "prefer")

DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"  

FEATURES = [  
    "scheduledFlightDuration",
    "nbFlightDepartingDepartureAirport",
    "nbFlightArrivingDepartureAirport",
    "nbFlightDepartingArrivalAirport",
    "nbFlightArrivingArrivalAirport",
    "departureairportdelayedshare",
    "aircraftdelayedshare",
    "airlinedelayedshare",
    "departureMonthDay",
    "departureWeekDay",
    "departureHour"
]
TARGET = "is_delayed"


def load_data(engine) -> pd.DataFrame:
    """Charge silver_mart.fct_flight_legs (vols non annulés)."""
    query = """
    SELECT * FROM silver_mart.fct_flight_legs
    WHERE cancelled = false
    """
    return pd.read_sql(query, engine)


def prepare_for_predicton(df: pd.DataFrame): 
    """Prépare X, y"""
    df[TARGET] = df[TARGET].astype(bool).astype(int)
    means_df = pickle.load(urllib.request.urlopen("https://amtxaysrmhlznfwqemdu.supabase.co/storage/v1/object/sign/ml_models/means_2026-04-01_11_33_01.pkl?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV80OThhOTAyZC0zYTJmLTRjM2EtOTFlOC05NGE0YTE2MTc0ZTgiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJtbF9tb2RlbHMvbWVhbnNfMjAyNi0wNC0wMV8xMV8zM18wMS5wa2wiLCJpYXQiOjE3Nzc4NzYzNTYsImV4cCI6MTgwOTQxMjM1Nn0.B-8vk1ot07LMpXv_1dkGoHSKZmpYFsYfuxzRBht_WFc"))
    scaler = pickle.load(urllib.request.urlopen("https://amtxaysrmhlznfwqemdu.supabase.co/storage/v1/object/sign/ml_models/scaler_2026-04-01_11_33_06.pkl?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV80OThhOTAyZC0zYTJmLTRjM2EtOTFlOC05NGE0YTE2MTc0ZTgiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJtbF9tb2RlbHMvc2NhbGVyXzIwMjYtMDQtMDFfMTFfMzNfMDYucGtsIiwiaWF0IjoxNzc3ODc2NDE5LCJleHAiOjE4MDk0MTI0MTl9.3fW6ZOSCZuAzxc-yUGKZ3iFkpVsVXB_5GBSyWapzhEY"))

    # rename for ml 
    df = df.rename(columns = {
            "scheduled_flight_duration_minutes":"scheduledFlightDuration",
            "departure_weekday":"departureWeekDay",
            "departure_hour":"departureHour",
            "departure_monthday":"departureMonthDay",
            "nb_flight_departing_departure_airport":"nbFlightDepartingDepartureAirport",
            "nb_flight_arriving_departure_airport":"nbFlightArrivingDepartureAirport",
            "nb_flight_departing_arrival_airport":"nbFlightDepartingArrivalAirport",
            "nb_flight_arriving_arrival_airport":"nbFlightArrivingArrivalAirport",
            "departure_airport_delayed_share":"departureairportdelayedshare",
            "aircraft_delayed_share":"aircraftdelayedshare",
            "airline_delayed_share":"airlinedelayedshare"}
            )
    

    # replace with mean value 
    for col in FEATURES:
        df[col] = df[col].fillna(means_df[col])
    X = df[FEATURES].copy()

    # Normalize 
    X_norm = scaler.transform(X)
    X_norm  = pd.DataFrame(X_norm)
    X_norm.columns = X.columns
    X_norm.index = X.index

    y = df[TARGET].values


    

    return X_norm, y



def main():
    engine = create_engine(DB_URI)
    df = load_data(engine)
    if df.empty:
        print("Aucune donnée dans mart.fct_flight_legs. Exécuter dbt run avant ml_run.py.")
        return

    X, y = prepare_for_predicton(df)

    model_ = pickle.load(urllib.request.urlopen("https://amtxaysrmhlznfwqemdu.supabase.co/storage/v1/object/sign/ml_models/xgb_2026-04-01_11_36_35.pkl?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV80OThhOTAyZC0zYTJmLTRjM2EtOTFlOC05NGE0YTE2MTc0ZTgiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJtbF9tb2RlbHMveGdiXzIwMjYtMDQtMDFfMTFfMzZfMzUucGtsIiwiaWF0IjoxNzc3ODgxOTY3LCJleHAiOjE4MDk0MTc5Njd9.dnvFSw1YEAYkNLpequlsQKpWSEng6Zo2p3FaDV0mK7M"))                    

    y_pred = model_.predict(X)

    df_w_pred = df.copy()
    df_w_pred["delay_predicted"] = y_pred

    create_sql = """
     TABLE IF NOT EXISTS public.ml_delays (
        leg_id UUID,
        flight_id VARCHAR(50),
        delay_predicted INTEGER,
        PRIMARY KEY (leg_id) 
    );
    """
    #with engine.begin() as conn:
        #conn.execute(text("DROP TABLE IF EXISTS public.ml_delays CASCADE"))
    #    conn.execute(text(create_sql))

    cols_out = ["leg_id", "flight_id", "delay_predicted"]
    #df_w_pred[cols_out].to_sql(
     #   "ml_delays",
     #   engine,
     #   schema="public",
     #   if_exists="append",
     #   index=False,
     #   method="multi",
    #    chunksize=1000,
    #)

    values = ", ".join([f"('{row['leg_id']}', '{row['flight_id']}', {row['delay_predicted']})" for _, row in df_w_pred.iterrows()])
    query = f"""
    INSERT INTO public.ml_delays (leg_id, flight_id, delay_predicted)
    VALUES {values}
    ON CONFLICT (leg_id)
    DO UPDATE SET
        flight_id = EXCLUDED.flight_id,
        delay_predicted = EXCLUDED.delay_predicted;
    """

    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

    print(f"Prédictions écrites dans public.ml_delays ({len(df_w_pred)} lignes).")


if __name__ == "__main__":
    main()
