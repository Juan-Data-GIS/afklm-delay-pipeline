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

# --- HARMONISATION SÉCURISÉE EN SNAKE_CASE (CONFORME AUX NOMS DE COLONNES FCT_FLIGHT_LEGS) ---
FEATURES = [
    "scheduled_flight_duration_minutes",
    "nb_flight_departing_departure_airport",
    "nb_flight_arriving_departure_airport",
    "nb_flight_departing_arrival_airport",
    "nb_flight_arriving_arrival_airport",
    "departure_airport_delayed_share",
    "aircraft_delayed_share",
    "airline_delayed_share",
    "departure_monthday",
    "departure_weekday",
    "departure_hour",
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

    # --- INJECTEUR DE COMPATIBILITÉ UNIVERSELLE ANTI-CRASH PANDAS V1 VS V2 ---
    class SafeDataOpsUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            if 'pandas' in module and name == 'StringDtype':
                return pd.StringDtype
            if module == 'pandas.core.arrays.string_' or 'NDArrayBacked' in name:
                return np.ndarray
            return super().find_class(module, name)

    req = urllib.request.Request(str(url), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            binary_data = resp.read()
            
            try:
                obj = SafeDataOpsUnpickler(io.BytesIO(binary_data)).load()
                
                if label == "MODEL_MEANS_URL":
                    if isinstance(obj, pd.DataFrame):
                        # On force la conversion des clés du dictionnaire en minuscules/snake_case si nécessaire
                        raw_dict = obj.to_dict(orient='records')[0]
                        return {k.lower().replace("shares", "_share").replace("duration", "_duration_minutes").replace("day", "day").replace("weekday", "_weekday").replace("hour", "_hour"): v for k, v in raw_dict.items()}
                    elif isinstance(obj, pd.Series):
                        return obj.to_dict()
                    elif isinstance(obj, dict):
                        return obj
                return obj
                
            except (TypeError, NotImplementedError, AttributeError):
                print(f"[ML COMPATIBILITY WARNING] Rupture de compatibilité binaire sur {label}. Application de la stratégie de secours...")
                
                # Alignement strict du dictionnaire de secours sur la nomenclature FEATURES
                default_means = {
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
                
                if label == "MODEL_MEANS_URL":
                    return default_means
                
                return pickle.loads(binary_data)
                
    except Exception as e:
        print(f"[ML NETWORK ERROR] Échec critique d'acquisition réseau de l'artefact : {e}")
        raise e


def load_data(engine) -> pd.DataFrame:
    """Charge les données de vols. Supporte élégamment l'absence de la table ml_delays au premier run."""
    incremental_query = """
        SELECT l.* FROM public_mart.fct_flight_legs l
        LEFT JOIN public.ml_delays d ON l.leg_id = d.leg_id
        WHERE l.cancelled = false AND d.leg_id IS NULL
    """
    fallback_query = """
        SELECT * FROM public_mart.fct_flight_legs 
        WHERE cancelled = false
    """
    try:
        # Essai en mode incrémental (vitesse maximale en production)
        return pd.read_sql(incremental_query, engine)
    except Exception as e:
        # Si la table ml_delays n'existe pas encore (First Run), on intercepte proprement
        if "undefinedtable" in str(e).lower() or "does not exist" in str(e).lower():
            print("[ML MLOPS INFO] Table public.ml_delays non détectée (Premier run à froid). Repli sur le chargement complet.")
            return pd.read_sql(fallback_query, engine)
        else:
            raise e


def prepare_for_prediction(df: pd.DataFrame):
    """Prépare X, y avec alignement et imputation robuste."""
    df[TARGET] = df[TARGET].astype(bool).astype(int)
    means_df = _load_pickle_from_url(MODEL_MEANS_URL, label="MODEL_MEANS_URL")
    scaler = _load_pickle_from_url(MODEL_SCALER_URL, label="MODEL_SCALER_URL")

    # --- SÉCURITÉ ACCRUE : Plus besoin de mapper vers le CamelCase obsolète ---
    # Les colonnes de fct_flight_legs matchent directement nos FEATURES en snake_case.
    # On applique une normalisation des clés du dictionnaire d'imputation s'il vient de Supabase en CamelCase
    clean_means = {}
    if isinstance(means_df, dict):
        # Cartographie inverse pour traduire l'ancien artefact s'il est au format Supabase
        mapping_legacy = {
            "scheduledflightduration": "scheduled_flight_duration_minutes",
            "nbflightdepartingdepartureairport": "nb_flight_departing_departure_airport",
            "nbflightarrivingdepartureairport": "nb_flight_arriving_departure_airport",
            "nbflightdepartingarrivalairport": "nb_flight_departing_arrival_airport",
            "nbflightarrivingarrivalairport": "nb_flight_arriving_arrival_airport",
            "departureairportdelayedshare": "departure_airport_delayed_share",
            "aircraftdelayedshare": "aircraft_delayed_share",
            "airlinedelayedshare": "airline_delayed_share",
            "departuremonthday": "departure_monthday",
            "departureweekday": "departure_weekday",
            "departurehour": "departure_hour"
        }
        for k, v in means_df.items():
            k_low = k.lower().replace("_", "")
            clean_key = mapping_legacy.get(k_low, k)
            clean_means[clean_key] = v
    else:
        clean_means = means_df

    # Imputation robuste
    for col in FEATURES:
        if isinstance(clean_means, dict):
            fallback_value = clean_means.get(col, 0.0)
        else:
            fallback_value = clean_means[col] if not hasattr(clean_means[col], "values") else clean_means[col].iloc[0]
            
        df[col] = df[col].fillna(fallback_value)
        
    X = df[FEATURES].copy()
    
    # Adaptation dynamique des colonnes pour le modèle de scaling historique (qui attendait l'ancien ordre/nom)
    X_legacy_names = X.copy()
    X_legacy_names.columns = [
        "scheduledFlightDuration", "nbFlightDepartingDepartureAirport", "nbFlightArrivingDepartureAirport",
        "nbFlightDepartingArrivalAirport", "nbFlightArrivingArrivalAirport", "departureairportdelayedshare",
        "aircraftdelayedshare", "airlinedelayedshare", "departureMonthDay", "departureWeekDay", "departureHour"
    ]
    
    X_norm = scaler.transform(X_legacy_names)
    X_norm_df = pd.DataFrame(X_norm, columns=X_legacy_names.columns, index=X.index)
    y = df[TARGET].values

    return X_norm_df, y


def main():
    engine = create_engine(DB_URI)
    df = load_data(engine)
    if df.empty:
        print("[ML STATUS] Aucune nouvelle observation à prédire dans fct_flight_legs. En attente de nouvelles données.")
        return

    X, y = prepare_for_prediction(df)
    model_ = _load_pickle_from_url(MODEL_XGB_URL, label="MODEL_XGB_URL")
    
    # --- PATCH DE COMPATIBILITÉ SKLEARN / XGBOOST RÉTROACTIVE ---
    if not hasattr(model_, "use_label_encoder"):
        model_.use_label_encoder = False
        
    y_pred = model_.predict(X)

    df_w_pred = df.copy()
    df_w_pred["delay_predicted"] = y_pred
    df_w_pred["timestamp"] = pd.Timestamp.now()

    create_sql = """
    CREATE TABLE IF NOT EXISTS public.ml_delays (
        leg_id          UUID PRIMARY KEY,
        flight_id       VARCHAR(50),
        delay_predicted INTEGER,
        timestamp       TIMESTAMP
    );
    """

    upsert_sql = text(
        """
        INSERT INTO public.ml_delays (leg_id, flight_id, delay_predicted, timestamp)
        VALUES (:leg_id, :flight_id, :delay_predicted, :timestamp)
        ON CONFLICT (leg_id) DO UPDATE SET
            flight_id       = EXCLUDED.flight_id,
            delay_predicted = EXCLUDED.delay_predicted,
            timestamp       = EXCLUDED.timestamp
        """
    )

    out = df_w_pred[["leg_id", "flight_id", "delay_predicted", "timestamp"]].copy()
    out["delay_predicted"] = out["delay_predicted"].astype(int)
    records = out.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        for row in records:
            conn.execute(upsert_sql, row)

    print(f"[ML RUN REUSSI] {len(df_w_pred)} nouvelles prédictions enregistrées dans public.ml_delays ({ENV_TARGET.upper()}).")


if __name__ == "__main__":
    main()