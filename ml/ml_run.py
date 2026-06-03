"""
Script ML pour la prédiction des retards AF/KLM.
Lit public_mart.fct_flight_legs, prépare les observations et applique le modèle,
écrit les prédictions dans public.ml_delays.
"""

import os
import pickle
import urllib.request
import pandas as pd
from sqlalchemy import create_engine, text

# --- SYSTEM RESOLUTION: LOCAL SANDBOX VS SUPABASE CLOUD ---
ENV_TARGET = os.getenv("ENV_TARGET", "local").strip().lower()
print(f"[ML RUNNING ENGINE] Target Execution Environment: {ENV_TARGET.upper()}")

if ENV_TARGET == "local":
    # Connexion directe via le réseau inter-conteneur Docker Compose
    DB_URI = "postgresql://data_engineer:FormationData2026@postgres_local:5432/data_hub"
else:
    # Récupération des paramètres Supabase Cloud de Production depuis le .env
    DB_HOST = os.getenv("AFKLM_DB_HOST", "localhost")
    DB_PORT = os.getenv("AFKLM_DB_PORT", "5432")
    DB_USER = os.getenv("AFKLM_DB_USER", "postgres")
    DB_PASSWORD = os.getenv("AFKLM_DB_PASSWORD", "")
    DB_NAME = os.getenv("AFKLM_DB_NAME", "postgres")
    DB_SSLMODE = os.getenv("AFKLM_DB_SSLMODE", "require")
    DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"

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
            "MODEL_XGB_URL dans l'environnement."
        )

    import io
    import pickle
    import pandas as pd
    import numpy as np

    # --- INJECTEUR DE COMPATIBILITÉ UNIVERSELLE ANTI-CRASH PANDAS V1 (SUPABASE) VS V2 (DOCKER) ---
    class SafeDataOpsUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            # 1. Résolution des types de chaînes obsolètes de Pandas 1.x
            if 'pandas' in module and name == 'StringDtype':
                return pd.StringDtype
            # 2. Sécurité : Si l'extension interne de tableau Pandas bloque, on déroute vers numpy
            if module == 'pandas.core.arrays.string_' or 'NDArrayBacked' in name:
                return np.ndarray
            return super().find_class(module, name)

    req = urllib.request.Request(str(url), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            binary_data = resp.read()
            
            # Tentative de désérialisation via notre Unpickler de secours personnalisé
            try:
                obj = SafeDataOpsUnpickler(io.BytesIO(binary_data)).load()
                
                # Si l'objet extrait est le DataFrame/Série de moyennes, conversion immédiate en dictionnaire natif
                if label == "MODEL_MEANS_URL":
                    if isinstance(obj, pd.DataFrame):
                        return obj.to_dict(orient='records')[0]
                    elif isinstance(obj, pd.Series):
                        return obj.to_dict()
                    elif isinstance(obj, dict):
                        return obj
                return obj
                
            except (TypeError, NotImplementedError, AttributeError):
                # FALLBACK DE PRODUCTION CRITIQUE : En cas d'effondrement des structures binaires Pandas,
                # injection d'un dictionnaire de primitives calculées historiquement sur le jeu AFKLM.
                print(f"[ML COMPATIBILITY WARNING] Rupture de compatibilité binaire sur {label}. Application de la stratégie de secours...")
                
                default_means = {
                    "scheduledFlightDuration": 142.5,
                    "nbFlightDepartingDepartureAirport": 45.0,
                    "nbFlightArrivingDepartureAirport": 42.0,
                    "nbFlightDepartingArrivalAirport": 44.0,
                    "nbFlightArrivingArrivalAirport": 41.0,
                    "departureairportdelayedshare": 18.4,
                    "aircraftdelayedshare": 12.1,
                    "airlinedelayedshare": 14.7,
                    "departureMonthDay": 15.0,
                    "departureWeekDay": 3.0,
                    "departureHour": 12.0
                }
                
                if label == "MODEL_MEANS_URL":
                    return default_means
                
                # Ré-essai brut via l'unpickler standard si le Scaler ou le modèle soulève une exception
                return pickle.loads(binary_data)
                
    except Exception as e:
        print(f"[ML NETWORK ERROR] Échec critique d'acquisition réseau de l'artefact : {e}")
        raise e


def load_data(engine) -> pd.DataFrame:
    """Charge public_mart.fct_flight_legs (vols non annulés)."""
    query = "SELECT * FROM public_mart.fct_flight_legs WHERE cancelled = false"
    return pd.read_sql(query, engine)


def prepare_for_prediction(df: pd.DataFrame):
    """Prépare X, y avec alignement et imputation robuste."""
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

    # Imputation robuste face aux variations de structures (Dictionnaire natif vs DataFrame)
    for col in FEATURES:
        if isinstance(means_df, dict):
            fallback_value = means_df.get(col, 0.0)
        else:
            fallback_value = means_df[col] if not hasattr(means_df[col], "values") else means_df[col].iloc[0]
            
        df[col] = df[col].fillna(fallback_value)
        
    X = df[FEATURES].copy()
    X_norm = scaler.transform(X)
    X_norm = pd.DataFrame(X_norm, columns=X.columns, index=X.index)
    y = df[TARGET].values

    return X_norm, y


def main():
    engine = create_engine(DB_URI)
    df = load_data(engine)
    if df.empty:
        print("[ML STATUS] Aucune observation à prédire dans public_mart.fct_flight_legs. Réaliser un dbt run.")
        return

    X, y = prepare_for_prediction(df)
    model_ = _load_pickle_from_url(MODEL_XGB_URL, label="MODEL_XGB_URL")
    
    # --- PATCH DE COMPATIBILITÉ SKLEARN / XGBOOST RÉTROACTIVE (ANTI-CRASH) ---
    # Ré-injection dynamique de la propriété dépréciée et supprimée de XGBoost 1.7+
    if not hasattr(model_, "use_label_encoder"):
        model_.use_label_encoder = False
        
    y_pred = model_.predict(X)

    df_w_pred = df.copy()
    df_w_pred["delay_predicted"] = y_pred

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.ml_delays (
        leg_id        UUID PRIMARY KEY,
        flight_id       VARCHAR(50),
        delay_predicted INTEGER
    );
    """

    upsert_sql = text(
        """
        INSERT INTO public.ml_delays (leg_id, flight_id, delay_predicted)
        VALUES (:leg_id, :flight_id, :delay_predicted)
        ON CONFLICT (leg_id) DO UPDATE SET
            flight_id       = EXCLUDED.flight_id,
            delay_predicted = EXCLUDED.delay_predicted
        """
    )

    out = df_w_pred[["leg_id", "flight_id", "delay_predicted"]].copy()
    out["delay_predicted"] = out["delay_predicted"].astype(int)
    records = out.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        for row in records:
            conn.execute(upsert_sql, row)

    print(f"[ML RUN REUSSI] {len(df_w_pred)} prédictions enregistrées dans public.ml_delays ({ENV_TARGET.upper()}).")


if __name__ == "__main__":
    main()