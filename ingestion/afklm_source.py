"""
afklm_source.py — Source dlt pour l'API Air France/KLM Flight Status.

Responsabilité : Extract & Load (EL) uniquement.
  - Appelle l'API AF/KLM avec pagination par fenêtre temporelle.
  - Normalise le JSON brut en 3 tables relationnelles.
  - Résilient aux pannes serveurs (limite à 5 retries max par page avant abandon).
  - Configuré par défaut sur J-1 complet avec réduction de la verbosité des logs.
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
SLEEP_BETWEEN_REQUESTS = 1.5

# Nombre maximum de tentatives sur erreur 5xx avant de déclarer la page en échec.
MAX_RETRIES = 5

# Index global de la clé active pour l'ensemble du run (base 0)
CURRENT_KEY_INDEX = 0


# ─────────────────────────────────────────────────────────────────────────────
# Couche HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    api_key: str,  
    start_range: str,
    end_range: str,
    page_number: int = 0,
) -> dict:
    """Appelle l'endpoint /flightstatus pour une fenêtre temporelle et une page données."""
    global CURRENT_KEY_INDEX

    url = f"{BASE_URL}/flightstatus"
    params = {
        "startRange": start_range,
        "endRange":   end_range,
        "pageSize":   PAGE_SIZE,
        "pageNumber": page_number,
    }

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

    attempts = 0

    while CURRENT_KEY_INDEX < len(available_keys):
        current_key = available_keys[CURRENT_KEY_INDEX]
        display_index = CURRENT_KEY_INDEX + 1

        headers = {
            "API-Key": current_key,
            "Accept": "application/hal+json",
        }

        try:
            # Réduction de la verbosité des logs
            if page_number % 10 == 0:
                logger.info(f"[HTTP REQUEST] Fetching Page {page_number} (Cle {display_index}/{len(available_keys)})")
            
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()  
            
            return resp.json()

        except requests.exceptions.RequestException as e:
            resp_obj = getattr(e, "response", None)

            if resp_obj is not None and resp_obj.status_code in (401, 403):
                logger.warning(f"[API KEY ERROR] La cle numero {display_index} a renvoye un code {resp_obj.status_code} (Quota plein ?).")
                
                if display_index < len(available_keys):
                    logger.warning(f"[FAILOVER] Bascule DEFINITIVE. Passage immediat a la cle numero {display_index + 1}...")
                    CURRENT_KEY_INDEX += 1  
                    continue  
                else:
                    logger.error("[CRITICAL] La derniere cle de secours disponible a elle aussi echoue. Plus de cles de secours disponibles.")
                    raise

            if resp_obj is not None and resp_obj.status_code in (500, 502, 503, 504):
                attempts += 1
                if attempts < MAX_RETRIES:
                    logger.warning(f"[SERVER ERROR] Code {resp_obj.status_code} rencontre. Tentative {attempts}/{MAX_RETRIES}. Nouvelle tentative dans 5s...")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"[PAGE TIMEOUT] La page {page_number} a echoue {MAX_RETRIES} fois consecutives avec un code {resp_obj.status_code}.")
                    raise requests.exceptions.HTTPError(f"Echec persistant 5xx sur la page {page_number}.", response=resp_obj)

            logger.error(f"[FATAL EXCEPTION] Echec critique sur la page {page_number} : {e}")
            raise

    raise requests.exceptions.HTTPError("Toutes les cles API Air France-KLM configurees ont ete epuisees.")


# ─────────────────────────────────────────────────────────────────────────────
# Itérateur principal
# ─────────────────────────────────────────────────────────────────────────────

def _iter_flights(
    api_key: str,
    start_date: datetime,
    end_date: datetime,
):
    """Itère sur tous les vols de la fenêtre [start_date, end_date] un jour complet à la fois."""
    current_day = start_date
    fetched_at = datetime.now(timezone.utc).isoformat()

    # Découpage atomique jour par jour pour la stabilité temporelle
    while current_day.date() <= end_date.date():
        day_start = datetime(current_day.year, current_day.month, current_day.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = datetime(current_day.year, current_day.month, current_day.day, 23, 59, 59, tzinfo=timezone.utc)

        start_range = day_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_range   = day_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        logger.info(f" [FETCHING DAY] Extraction cible du jour : {day_start.date()}")

        try:
            data = _fetch_page(api_key, start_range, end_range, 0)
            total_pages = data.get("page", {}).get("totalPages", 0)
            flights = data.get("operationalFlights", [])
            
            logger.info(f" [METRICS] {day_start.date()} : {total_pages} page(s) au total, {len(flights)} vols sur la page 0.")

            for flight in flights:
                yield flight, fetched_at

            for page_num in range(1, total_pages):
                # Synthèse claire d'avancement toutes les 10 pages
                if page_num % 10 == 0 or page_num == total_pages - 1:
                    logger.info(f"   Avancement {day_start.date()} : Page {page_num}/{total_pages} en cours de lecture...")
                
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                try:
                    page_data = _fetch_page(api_key, start_range, end_range, page_num)
                except requests.exceptions.RequestException as e:
                    logger.warning(f"[PAGE SKIPPED] La page {page_num} a echoue. Passage force a la suite. Erreur : {e}")
                    continue

                page_flights = page_data.get("operationalFlights", [])
                for flight in page_flights:
                    yield flight, fetched_at
                    
        except Exception as e:
            logger.error(f"[WINDOW CRITICAL FAILED] Extraction interrompue pour le jour {day_start.date()} : {e}")
            raise e

        current_day += timedelta(days=1)


# ─────────────────────────────────────────────────────────────────────────────
# Gestion de la fenêtre temporelle (PRODUCTION READY)
# ─────────────────────────────────────────────────────────────────────────────

def _get_dates(
    start_date: str | None,
    end_date: str | None,
    incremental: bool = True,
):
    state = {}
    try:
        state = dlt.current.source_state()
    except Exception:
        pass 

    last_end = state.get("last_window_end") if incremental else None

    # 1. Si dlt a un checkpoint incrémental (exécution continue)
    if last_end:
        start = datetime.fromisoformat(last_end.replace("Z", "+00:00"))
        logger.info(f"[PRODUCTION - INCREMENTAL] Reprise depuis le checkpoint dlt : {start.isoformat()}")
    
    # 2. Alignement strict sur la date fournie par l'ordonnanceur (Airflow / Manuel spécifique)
    elif start_date and str(start_date).strip():
        parsed_start = datetime.fromisoformat(str(start_date).replace("Z", "+00:00"))
        start = parsed_start.replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"[ORCHESTRATION - DATE INJECTEE] Alignement strict sur la date de l'orchestrateur : {start.isoformat()}")
    
    # 3. Mode secours local (Exécution brute sans argument)
    else:
        now_utc = datetime.now(timezone.utc)
        hier = now_utc - timedelta(days=1)
        start = hier.replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"[BACKUP LOCAL - J-1] Aucune variable fournie, repli automatique sur J-1 local : {start.isoformat()}")

    # Clôture systématique à 23:59:59 de la journée déterminée par le point de départ
    end = start.replace(hour=23, minute=59, second=59, microsecond=0)
    logger.info(f"[FENETRE BORNEE] Clôture de la fenêtre d'extraction : {end.isoformat()}")
        
    return start, end


# ─────────────────────────────────────────────────────────────────────────────
# Fonctions de normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _build_flights_table(flight: dict, fetched_at: str) -> dict:
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
# Source dlt (orchestration sécurisée)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.source(name="afklm")
def afklm_source(
    api_key: str = dlt.secrets.value,
    start_date: str | None = dlt.config.value,
    end_date: str | None = dlt.config.value,
    incremental: bool = True,
):
    """Source dlt AF/KLM : extraction robuste isolée, dispatch vers 3 tables."""
    start, end = _get_dates(start_date, end_date, incremental)
    logger.info(f"[START RUN] Lancement de l'extraction de l'API Air France-KLM du {start.isoformat()} au {end.isoformat()}")

    all_flights_rows = []
    all_legs_rows    = []
    all_delays_rows  = []

    # Flag technique pour savoir si le run a échoué en cours de route
    run_status = "SUCCESS"

    try:
        for flight, fetched_at in _iter_flights(api_key, start, end):
            all_flights_rows.append(_build_flights_table(flight, fetched_at))
            all_legs_rows.extend(_build_legs_table(flight))
            all_delays_rows.extend(_build_delays_table(flight))
    except Exception as extract_error:
        # En cas d'erreur 500 d'Air France, on intercepte pour forcer l'envoi des logs métiers avant de couper
        logger.warning(f"[TRAFFIC CONTROL] Interception du crash pour envoi des metriques DataOps. Erreur originelle : {extract_error}")
        run_status = "FAILED"

    # ---- INJECTION DES METRIQUES POUR GRAFANA CONSOLIDEES (SAUVÉES DU CRASH) ----
    vols_count = len(all_flights_rows)
    legs_count = len(all_legs_rows)
    delays_count = len(all_delays_rows)

    try:
        import inspect
        for frame_info in inspect.stack():
            if 'context' in frame_info.frame.f_locals:
                context = frame_info.frame.f_locals['context']
                ti = context['task_instance']
                
                # TRANSMISSION DE LA DATE METIER (START) VERS L'OBSERVABILITE CENTRALISEE
                metrics = {
                    "event_at": start.strftime("%Y-%m-%d"),
                    "records_processed": vols_count,
                    "legs_processed": legs_count,
                    "delays_processed": delays_count,
                    "pipeline_engine": "dlt",
                    "extraction_status": run_status  # Permet à Grafana de savoir si le volume est partiel
                }
                ti.xcom_push(key='data_metrics', value=metrics)
                break
    except Exception as e:
        logger.warning(f"[METRICS WARNING] Impossible de pousser les volumes vers Airflow XCom: {e}")

    # Si le run a capoté, on lève l'erreur APRES avoir poussé l'XCom technique
    if run_status == "FAILED":
        raise requests.exceptions.HTTPError(f"Echec persistant de l'API Air France sur la journee du {start.date()}. Arret du traitement.")

    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)

    try:
        dlt.current.source_state()["last_window_end"] = end.isoformat()
        logger.info(f"[STATE PERSISTED] Curseur incremental mis a jour avec succes : {end.isoformat()}")
    except Exception as e:
        logger.warning(f"[STATE WARNING] Impossible d'enregistrer l'etat incremental : {e}")