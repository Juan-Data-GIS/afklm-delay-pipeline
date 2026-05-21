"""
afklm_source.py — Source dlt pour l'API Air France/KLM Flight Status.

Responsabilité : Extract & Load (EL) uniquement.
  - Appelle l'API AF/KLM avec pagination par fenêtre temporelle.
  - Normalise le JSON brut en 3 tables relationnelles :
      · operational_flights       (1 ligne = 1 vol)
      · operational_flight_legs   (1 ligne = 1 segment du vol)
      · operational_flight_delays (1 ligne = 1 code retard par segment)
  - Un seul passage sur l'API : les 3 tables sont alimentées depuis le même flux.

Ce fichier n'est pas le point d'entrée — voir afklm_dlt_pipeline.py.
"""

import time
import uuid
import logging
import os  
from datetime import datetime, timezone, timedelta

import dlt
import requests

# Logger namespaced sous "dlt.sources.afklm"
logger = logging.getLogger("dlt.sources.afklm")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes & Configuration Globale
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.airfranceklm.com/opendata"

# Nombre de vols par page.
PAGE_SIZE = 90

# Pause (secondes) entre deux pages consécutives d'une même fenêtre.
SLEEP_BETWEEN_REQUESTS = 3

# Nombre maximum de tentatives sur erreur 5xx avant de déclarer la page en échec.
MAX_RETRIES = 5

# Délais de backoff progressifs (secondes) entre chaque retry sur erreur 5xx.
RETRY_BACKOFF_500 = [5, 10, 15, 20, 30]

# Index global de la clé active pour l'ensemble du run (base 0)
# Cette variable persiste entre les appels de pages et ne revient jamais en arrière.
CURRENT_KEY_INDEX = 0


# ─────────────────────────────────────────────────────────────────────────────
# Couche HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    api_key: str,  # Conservé pour la signature dlt standard
    start_range: str,
    end_range: str,
    page_number: int = 0,
) -> dict:
    """Appelle l'endpoint /flightstatus pour une fenêtre temporelle et une page données.

    Stratégie d'erreur et de failover :
      - Utilise CURRENT_KEY_INDEX pour conserver la dernière clé valide connue.
      - Si une clé renvoie un code 401/403 (quota plein), elle est abandonnée définitivement pour ce run.
      - 5xx (500, 502, 503, 504) : retry avec backoff progressif sur la même clé.
    """
    global CURRENT_KEY_INDEX

    url = f"{BASE_URL}/flightstatus"
    params = {
        "startRange": start_range,   # Format : "2026-01-16T00:00:00.000Z"
        "endRange":   end_range,     # Format : "2026-01-16T01:00:00.000Z"
        "pageSize":   PAGE_SIZE,
        "pageNumber": page_number,   # Index base-0
    }

    # Pool étendu à 5 clés (tes 2 clés d'origine + les 3 clés de ton collègue)
    keys_pool = [
        os.getenv("AF_CLIENT_ID_1"),
        os.getenv("AF_CLIENT_ID_2"),
        os.getenv("AF_CLIENT_ID_3"),
        os.getenv("AF_CLIENT_ID_4"),
        os.getenv("AF_CLIENT_ID_5")
    ]
    available_keys = [k for k in keys_pool if k]

    if not available_keys:
        logger.error("[FATAL] Aucune cle API (AF_CLIENT_ID_1 a 5) n'est definie dans l'environnement.")
        raise ValueError("Variables d'environnement API cles manquantes.")

    # La boucle démarre directement à l'index de la dernière clé fonctionnelle
    while CURRENT_KEY_INDEX < len(available_keys):
        current_key = available_keys[CURRENT_KEY_INDEX]
        display_index = CURRENT_KEY_INDEX + 1  # Index base-1 pour l'affichage lisible des logs

        headers = {
            "API-Key": current_key,
            "Accept": "application/hal+json",
        }

        try:
            logger.info(f"[HTTP REQUEST] Fetching Page {page_number} (Cle {display_index}/{len(available_keys)}) | Fenetre: {start_range} -> {end_range}")
            
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()  
            
            logger.info(f"[HTTP SUCCESS] Page {page_number} recuperee avec succes.")
            return resp.json()

        except requests.exceptions.RequestException as e:
            resp_obj = getattr(e, "response", None)

            # Interception et abandon DÉFINITIF de la clé sur erreur de quota (401/403)
            if resp_obj is not None and resp_obj.status_code in (401, 403):
                logger.warning(f"[API KEY ERROR] La cle numero {display_index} a renvoye un code {resp_obj.status_code} (Quota plein ?).")
                
                if display_index < len(available_keys):
                    logger.warning(f"[FAILOVER] Bascule DEFINITIVE. Passage immediat a la cle numero {display_index + 1}...")
                    CURRENT_KEY_INDEX += 1  # Incrémentation globale
                    continue  # Saute directement à la prochaine itération du while avec la clé suivante
                else:
                    logger.error("[CRITICAL] La derniere cle de secours disponible a elle aussi echoue. Plus de cles de secours disponibles.")
                    raise

            # Retry uniquement sur erreurs serveur transitoires (5xx)
            if resp_obj is not None and resp_obj.status_code in (500, 502, 503, 504):
                # Utilisation d'un mécanisme de retry linéaire simplifié pour le while
                logger.warning(f"[SERVER ERROR] Code {resp_obj.status_code} rencontre. Nouvelle tentative imminente sur la meme cle...")
                time.sleep(5)
                continue

            # Toute autre erreur (4xx paramétrage, réseau direct, timeout) → lever immédiatement
            logger.error(f"[FATAL EXCEPTION] Echec critique sur la page {page_number} : {e}")
            raise

    # Si le bloc 'while' prend fin sans retour de données, toutes les clés sont vides
    raise requests.exceptions.HTTPError("Toutes les cles API Air France-KLM configurees ont ete epuisees.")


# ─────────────────────────────────────────────────────────────────────────────
# Itérateur principal
# ─────────────────────────────────────────────────────────────────────────────

def _iter_flights(
    api_key: str,
    start_date: datetime,
    end_date: datetime,
):
    """Itère sur tous les vols de la fenêtre [start_date, end_date]."""
    current = start_date

    # Capture l'heure de début du run une seule fois pour la traçabilité dbt
    fetched_at = datetime.now(timezone.utc).isoformat()

    while current < end_date:
        window_end = min(current + timedelta(days=1), end_date)
        start_range = current.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_range   = window_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        logger.info(f"[PIPELINE WINDOW] Analyse de la periode : {start_range} -> {end_range}")

        # Page 0 : contient aussi la métadonnée totalPages
        try:
            data = _fetch_page(api_key, start_range, end_range, 0)
            total_pages = data.get("page", {}).get("totalPages", 0)
            flights = data.get("operationalFlights", [])
            
            logger.info(f"[METRICS] Fenetre courante : {total_pages} page(s) trouvee(s), {len(flights)} vols sur la page 0.")

            for flight in flights:
                yield flight, fetched_at

            # Pages 1 à N-1
            for page_num in range(1, total_pages):
                logger.info(f"[ANTI-THROTTLE] Temporisation de {SLEEP_BETWEEN_REQUESTS}s avant la page {page_num}...")
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                try:
                    page_data = _fetch_page(api_key, start_range, end_range, page_num)
                except requests.exceptions.RequestException as e:
                    # Si une page échoue après tous les retries, on la saute
                    logger.warning(f"[PAGE SKIPPED] La page {page_num} a echoue apres retries, skip : {e}")
                    continue

                page_flights = page_data.get("operationalFlights", [])
                logger.info(f"[DATA] Page {page_num} lue : {len(page_flights)} vols extraits.")
                for flight in page_flights:
                    yield flight, fetched_at
        except Exception as e:
            logger.error(f"[WINDOW CRITICAL FAILED] Extraction interrompue pour la fenetre courante : {e}")
            raise e

        # Avancer d'un jour pour la prochaine itération
        current = window_end


# ─────────────────────────────────────────────────────────────────────────────
# Gestion de la fenêtre temporelle (incrémental vs fixe)
# ─────────────────────────────────────────────────────────────────────────────

def _get_dates(
    start_date: str | None,
    end_date: str | None,
    incremental: bool = True,
):
    """Détermine la fenêtre [start, end] à extraire."""
    state = {}
    try:
        state = dlt.current.source_state()
    except Exception:
        pass 

    last_end = state.get("last_window_end") if incremental else None

    if last_end:
        start = datetime.fromisoformat(last_end.replace("Z", "+00:00"))
        logger.info(f"[INCREMENTAL] Reprise automatique dlt depuis le dernier enregistrement : {start.isoformat()}")
    elif start_date:
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        logger.info(f"[FIXED WINDOW] Utilisation de la date d'initialisation config : {start.isoformat()}")
    else:
        start = datetime.now(timezone.utc) - timedelta(days=1)
        logger.info(f"[FALLBACK] Aucune date trouvee. Ingestion par defaut (Fenetre glissante 24h) : {start.isoformat()}")

    end = (
        datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        if end_date
        else datetime.now(timezone.utc)
    )
    return start, end


# ─────────────────────────────────────────────────────────────────────────────
# Fonctions de normalisation (API JSON → dicts pour dlt)
# ─────────────────────────────────────────────────────────────────────────────

def _build_flights_table(flight: dict, fetched_at: str) -> dict:
    """Transforme un vol brut en dict pour la table operational_flights."""
    airline = flight.get("airline") or {}
    return {
        "id":                   flight.get("id"),
        "flight_number":        flight.get("flightNumber"),
        "flight_schedule_date": flight.get("flightScheduleDate"),
        "airline_code":         airline.get("code"),
        "airline_name":         airline.get("name"),
        "haul":                 flight.get("haul"),
        "route":                flight.get("route"),
        "flight_status_public": flight.get("flightStatusPublic"),
        "fetched_at":           fetched_at,
    }


def _build_legs_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_legs."""
    flight_id = flight.get("id")
    rows = []

    for i, leg in enumerate(flight.get("flightLegs") or []):
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))

        dep_info    = leg.get("departureInformation") or {}
        arr_info    = leg.get("arrivalInformation") or {}
        dep_airport = dep_info.get("airport") or {}
        arr_airport = arr_info.get("airport") or {}
        dep_city = dep_airport.get("city") or {}
        arr_city = arr_airport.get("city") or {}
        dep_country = dep_city.get("country") or {}
        arr_country = arr_city.get("country") or {}
        dep_times   = dep_info.get("times") or {}
        arr_times   = arr_info.get("times") or {}
        irreg       = leg.get("irregularity") or {}

        rows.append({
            "id":                       leg_id,
            "flight_id":                flight_id,
            "leg_order":                i,
            "departure_airport_code":   dep_airport.get("code"),
            "arrival_airport_code":     arr_airport.get("code"),
            "departure_airport_name":   dep_airport.get("name"),
            "arrival_airport_name":     arr_airport.get("name"),
            "published_status":         leg.get("publishedStatus"),
            "scheduled_departure":      dep_times.get("scheduled"),
            "actual_departure":         dep_times.get("actual"),
            "scheduled_arrival":        arr_times.get("scheduled"),
            "actual_arrival":           arr_times.get("actual"),
            "scheduled_flight_duration": leg.get("scheduledFlightDuration"),
            "cancelled":                irreg.get("cancelled") == "Y",
            "aircraft_code":       (leg.get("aircraft") or {}).get("typeCode"),
            "aircraft_name":       (leg.get("aircraft") or {}).get("typeName"),
            "departure_city_code":      dep_city.get("code"),
            "departure_city_name":      dep_city.get("name"),
            "departure_country_code":   dep_country.get("code"),
            "departure_country_name":   dep_country.get("name"),
            "arrival_city_code":      arr_city.get("code"),
            "arrival_city_name":      arr_city.get("name"),
            "arrival_country_code":   arr_country.get("code"),
            "arrival_country_name":   arr_country.get("name"),
        })
    return rows


def _build_delays_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_delays."""
    flight_id = flight.get("id")
    rows = []

    for i, leg in enumerate(flight.get("flightLegs") or []):
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))
        irreg = leg.get("irregularity") or {}

        delay_infos = irreg.get("delayInformation") or []

        if not delay_infos:
            codes     = irreg.get("delayCode") or []
            durations = irreg.get("delayDuration") or []
            delay_infos = [
                {"delayCode": c, "delayDuration": d}
                for c, d in zip(codes, durations)
            ]

        if not delay_infos:
            rows.append({
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}_0")),
                "flight_leg_id": leg_id,
                "delay_code": None,
                "delay_reason": None,
                "delay_duration": "00",
            })
        else:
            for j, d in enumerate(delay_infos):
                rows.append({
                    "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}_{j}")),
                    "flight_leg_id": leg_id,
                    "delay_code": d.get("delayCode"),
                    "delay_reason": d.get("delayReasonPublicLangTransl"),
                    "delay_duration": d.get("delayDuration"),
                })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Resources dlt
# ─────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="operational_flights", write_disposition="merge", primary_key="id")
def operational_flights_resource(flights_rows): yield from flights_rows

@dlt.resource(name="operational_flight_legs", write_disposition="merge", primary_key="id")
def operational_flight_legs_resource(legs_rows): yield from legs_rows

@dlt.resource(name="operational_flight_delays", write_disposition="merge", primary_key="id")
def operational_flight_delays_resource(delays_rows): yield from delays_rows


# ─────────────────────────────────────────────────────────────────────────────
# Source dlt (orchestration)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.source(name="afklm")
def afklm_source(
    api_key: str = dlt.secrets.value,
    start_date: str | None = dlt.config.value,
    end_date: str | None = dlt.config.value,
    incremental: bool = True,
):
    """Source dlt AF/KLM : 1 seul fetch API, dispatch vers 3 tables Supabase."""
    start, end = _get_dates(start_date, end_date, incremental)
    logger.info(f"[START RUN] Lancement de l'extraction de l'API Air France-KLM du {start.isoformat()} au {end.isoformat()}")

    all_flights_rows = []
    all_legs_rows    = []
    all_delays_rows  = []

    for flight, fetched_at in _iter_flights(api_key, start, end):
        all_flights_rows.append(_build_flights_table(flight, fetched_at))
        all_legs_rows.extend(_build_legs_table(flight))
        all_delays_rows.extend(_build_delays_table(flight))

    logger.info(
        f"[EXTRACT METRICS SUMMARY] Donnees normalisees pretes a l'envoi — "
        f"Vols: {len(all_flights_rows)} | Segments (Legs): {len(all_legs_rows)} | Retards (Delays): {len(all_delays_rows)}"
    )

    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)

    try:
        dlt.current.source_state()["last_window_end"] = end.isoformat()
        logger.info(f"[STATE PERSISTED] Curseur incremental mis a jour avec succes : {end.isoformat()}")
    except Exception as e:
        logger.warning(f"[STATE WARNING] Impossible d'enregistrer l'etat incremental : {e}")