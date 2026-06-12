"""
Script ML pour la prédiction des retards AF/KLM.
Lit public_mart.fct_flight_legs, prépare les observations et applique le modèle,
écrit les prédictions dans public.ml_delays.
"""

import os
import pickle
import urllib.request
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text

# Config DB depuis variables d'environnement
DB_HOST = os.getenv("AFKLM_DB_HOST", "localhost")
DB_PORT = os.getenv("AFKLM_DB_PORT", "5432")
DB_USER = os.getenv("AFKLM_DB_USER", "postgres")
DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD", "")
DB_NAME = os.getenv("AFKLM_DB_NAME", "postgres")
DB_SSLMODE = os.getenv("AFKLM_DB_SSLMODE", "prefer")

DB_URI = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?sslmode={DB_SSLMODE}"
)

MODEL_MEANS_URL = os.getenv("MODEL_MEANS_URL")
MODEL_SCALER_URL = os.getenv("MODEL_SCALER_URL")
MODEL_XGB_URL = os.getenv("MODEL_XGB_URL")
HTTP_TIMEOUT = int(os.getenv("MODEL_HTTP_TIMEOUT", "30"))

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
    "departureHour",
]
TARGET = "is_delayed"


def _load_pickle_from_url(url: str | None, *, label: str) -> object:
    if not url or not str(url).strip():
        raise RuntimeError(
            f"{label} est vide ou absent. Définir MODEL_MEANS_URL, MODEL_SCALER_URL, "
            "MODEL_XGB_URL dans l'environnement (voir .env.example)."
        )
    req = urllib.request.Request(str(url), method="GET")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return pickle.load(resp)


def load_data(engine) -> pd.DataFrame:
    """Charge public_mart.fct_flight_legs (vols non annulés)."""
    query = """
    SELECT * FROM public_mart.fct_flight_legs
    WHERE cancelled = false
    """
    return pd.read_sql(query, engine)


def prepare_for_prediction(df: pd.DataFrame):
    """Prépare X, y."""
    df[TARGET] = df[TARGET].astype(bool).astype(int)
    means_df = _load_pickle_from_url(MODEL_MEANS_URL, label="MODEL_MEANS_URL")
    scaler = _load_pickle_from_url(MODEL_SCALER_URL, label="MODEL_SCALER_URL")

    df = df.rename(
        columns={
            "scheduled_flight_duration_minutes": "scheduledFlightDuration",
            "departure_weekday": "departureWeekDay",
            "departure_hour": "departureHour",
            "departure_monthday": "departureMonthDay",
            "nb_flight_departing_departure_airport": "nbFlightDepartingDepartureAirport",
            "nb_flight_arriving_departure_airport": "nbFlightArrivingDepartureAirport",
            "nb_flight_departing_arrival_airport": "nbFlightDepartingArrivalAirport",
            "nb_flight_arriving_arrival_airport": "nbFlightArrivingArrivalAirport",
            "departure_airport_delayed_share": "departureairportdelayedshare",
            "aircraft_delayed_share": "aircraftdelayedshare",
            "airline_delayed_share": "airlinedelayedshare",
        }
    )

    for col in FEATURES:
        df[col] = df[col].fillna(means_df[col])
    X = df[FEATURES].copy()

    X_norm = scaler.transform(X)
    X_norm = pd.DataFrame(X_norm)
    X_norm.columns = X.columns
    X_norm.index = X.index

    y = df[TARGET].values

    return X_norm, y


def main():
    engine = create_engine(DB_URI)
    df = load_data(engine)
    if df.empty:
        print(
            "Aucune donnée dans public_mart.fct_flight_legs. Exécuter dbt run avant ml_run.py."
        )
        return

    X, y = prepare_for_prediction(df)

    model_ = _load_pickle_from_url(MODEL_XGB_URL, label="MODEL_XGB_URL")

    y_pred = model_.predict(X)

    df_w_pred = df.copy()
    df_w_pred["delay_predicted"] = y_pred

    # Add timestamps 
    df_w_pred["timestamp"] = pd.Series([pd.Timestamp.now()] * len(df_w_pred))

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.ml_delays (
        leg_id          UUID PRIMARY KEY,
        flight_id       VARCHAR(50),
        delay_predicted INTEGER,
        timestamp TIMESTAMP
    );
    """

    upsert_sql = text(
        """
        INSERT INTO public.ml_delays (leg_id, flight_id, delay_predicted, timestamp)
        VALUES (:leg_id, :flight_id, :delay_predicted, :timestamp)
        ON CONFLICT (leg_id) DO UPDATE SET
            flight_id       = EXCLUDED.flight_id,
            delay_predicted = EXCLUDED.delay_predicted
        """
    )

    out = df_w_pred[["leg_id", "flight_id", "delay_predicted", "timestamp"]].copy()
    out["delay_predicted"] = out["delay_predicted"].astype(int)
    records = out.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        for row in records:
            conn.execute(upsert_sql, row)

    print(f"Prédictions écrites dans public.ml_delays ({len(df_w_pred)} lignes).")


if __name__ == "__main__":
    main()
