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
import os  # AJOUT : Nécessaire pour lire les clés d'API de secours dans le .env
import json  # AJOUT METRICS : Nécessaire pour formater le errors_log au format JSON pour PostgreSQL
import psycopg2  # AJOUT METRICS : Pour se connecter et écrire directement dans logs.job_runs
from datetime import datetime, timezone, timedelta

import dlt
import requests

# Logger namespaced sous "dlt.sources.afklm" pour apparaître dans les logs dlt
# avec le bon niveau de verbosité (contrôlé par [runtime] log_level dans config.toml).
logger = logging.getLogger("dlt.sources.afklm")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes & Structure Globale de Monitoring (AJOUT METRICS)
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.airfranceklm.com/opendata"

# Nombre de vols par page. L'API AF/KLM peut retourner des 500 si > ~100.
# 50 est un compromis stable entre performance et fiabilité.
PAGE_SIZE = 80

# Pause (secondes) entre deux pages consécutives d'une même fenêtre.
# Évite de déclencher le rate limit de l'API.
SLEEP_BETWEEN_REQUESTS = 1

# Nombre maximum de tentatives sur erreur 5xx avant de déclarer la page en échec.
MAX_RETRIES = 5

# Délais de backoff progressifs (secondes) entre chaque retry sur erreur 5xx.
# Les 5xx AF/KLM sont souvent des throttles déguisés, pas de vraies pannes serveur.
# La progression laisse le temps au serveur de récupérer.
RETRY_BACKOFF_500 = [5, 10, 15, 20, 30]

# AJOUT METRICS : Dictionnaire d'audit partagé tout au long de l'exécution
run_report = {
    "run_id": str(uuid.uuid4()),
    "job_name": "ingestion_afklm",
    "layer": "RAW_INBOUND",
    "pages_recovered": 0,
    "pages_error_count": 0,
    "errors_log": []
}


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

    Stratégie d'erreur :
      - 5xx (500, 502, 503, 504) : retry avec backoff progressif (throttle déguisé).
      - 4xx (403 quota, 400 mauvais paramètre) ou erreur réseau : lève immédiatement.
        Ne pas retenter les 4xx évite les boucles infinies sur quota épuisé.

    Retourne le JSON parsé de la réponse.
    """
    url = f"{BASE_URL}/flightstatus"
    params = {
        "startRange": start_range,   # Format : "2026-01-16T00:00:00.000Z"
        "endRange":   end_range,     # Format : "2026-01-16T01:00:00.000Z"
        "pageSize":   PAGE_SIZE,
        "pageNumber": page_number,   # Index base-0
    }

    # AJOUT : Récupération des deux clés API du fichier .env pour le basculement automatique
    keys_pool = [
        os.getenv("AF_CLIENT_ID_1"),
        os.getenv("AF_CLIENT_ID_2")
    ]
    available_keys = [k for k in keys_pool if k]

    if not available_keys:
        logger.error("[FATAL] Aucune cle API (AF_CLIENT_ID_1 ou AF_CLIENT_ID_2) n'est definie dans l'environnement.")
        raise ValueError("Variables d'environnement API cles manquantes.")

    # AJOUT : Boucle de gestion du failover des cles si une erreur de quota (401/403) survient
    for index, current_key in enumerate(available_keys, start=1):
        headers = {
            "API-Key": current_key,
            "Accept": "application/hal+json", # Format HAL+JSON attendu
        }

        for attempt in range(MAX_RETRIES):
            try:
                # AJOUT : Log verbeux requis pour le suivi des pages sur Airflow
                logger.info(f"[HTTP REQUEST] Fetching Page {page_number} (Cle {index}/{len(available_keys)}) | Fenetre: {start_range} -> {end_range}")
                
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                resp.raise_for_status()  # Lève HTTPError sur tout code >= 400
                
                # AJOUT METRICS : Incrémentation du compteur de pages récupérées avec succès
                run_report["pages_recovered"] += 1
                
                # AJOUT : Confirmation de chargement de page
                logger.info(f"[HTTP SUCCESS] Page {page_number} recuperee avec succes.")
                return resp.json()

            except requests.exceptions.RequestException as e:
                resp_obj = getattr(e, "response", None)
                status_code = resp_obj.status_code if resp_obj is not None else 500

                # AJOUT METRICS : Enregistrement de l'erreur dans la structure d'audit globale
                run_report["pages_error_count"] += 1
                run_report["errors_log"].append({"page": page_number, "status": status_code})

                # AJOUT : Interception du dépassement de quota ou authentification invalide (401/403)
                if resp_obj is not None and resp_obj.status_code in (401, 403):
                    logger.warning(f"[API KEY ERROR] La cle numero {index} a renvoye un code {resp_obj.status_code} (Quota plein ?).")
                    if index < len(available_keys):
                        logger.warning("[FAILOVER] Bascule immediate sur la cle API de secours...")
                        break  # Quitte les retries de cette clé pour passer à la clé suivante
                    else:
                        logger.error("[CRITICAL] La cle de secours a elle aussi echoue. Plus de cles de secours disponibles.")
                        raise

                # Retry uniquement sur erreurs serveur transitoires
                if (
                    resp_obj is not None
                    and resp_obj.status_code in (500, 502, 503, 504)
                    and attempt < MAX_RETRIES - 1
                ):
                    backoff = RETRY_BACKOFF_500[min(attempt, len(RETRY_BACKOFF_500) - 1)]
                    logger.warning(
                        f"[SERVER ERROR] Code {resp_obj.status_code} (Tentative {attempt + 1}/{MAX_RETRIES}). "
                        f"Pause de securite, nouvel essai dans {backoff}s..."
                    )
                    time.sleep(backoff)
                    continue

                # Toute autre erreur (4xx, réseau, timeout) → lever immédiatement
                # AJOUT : Log d'erreur fatale
                logger.error(f"[FATAL EXCEPTION] Echec critique sur la page {page_number} : {e}")
                raise
        else:
            continue
        continue

    # AJOUT : Si toutes les clés du pool d'environnement échouent
    raise requests.exceptions.HTTPError("Toutes les cles API Air France-KLM configurees ont ete epuisees.")


# ─────────────────────────────────────────────────────────────────────────────
# Itérateur principal
# ─────────────────────────────────────────────────────────────────────────────

def _iter_flights(
    api_key: str,
    start_date: datetime,
    end_date: datetime,
):
    """Itère sur tous les vols de la fenêtre [start_date, end_date].

    L'API AF/KLM est paginée par fenêtre temporelle + numéro de page.
    La fenêtre est découpée jour par jour pour éviter des réponses tronquées
    ou incohérentes sur de grandes plages de temps.

    Yield : tuples (flight_dict, fetched_at_iso_string)
      - flight_dict  : objet JSON brut d'un vol (clé "operationalFlights[i]")
      - fetched_at   : horodatage UTC du début du run (identique pour toutes les lignes
                       du même run, utile pour dbt et la traçabilité)
    """
    current = start_date

    # Capture l'heure de début du run une seule fois.
    # Toutes les lignes produites par ce run auront le même fetched_at,
    # ce qui facilite les filtres temporels dans dbt (ex. "dernière ingestion").
    fetched_at = datetime.now(timezone.utc).isoformat()

    while current < end_date:
        # Fenêtre d'1 jour max — au-delà, l'API peut retourner des données tronquées.
        window_end = min(current + timedelta(days=1), end_date)
        start_range = current.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_range   = window_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # AJOUT : Standardisation du format des logs sans icônes pour Airflow
        logger.info(f"[PIPELINE WINDOW] Analyse de la periode : {start_range} -> {end_range}")

        # Page 0 : contient aussi la métadonnée totalPages
        try:
            data = _fetch_page(api_key, start_range, end_range, 0)
            total_pages = data.get("page", {}).get("totalPages", 0)
            flights = data.get("operationalFlights", [])
            
            # AJOUT : Message informatif sur les métadonnées de la page 0
            logger.info(f"[METRICS] Fenetre courante : {total_pages} page(s) trouvee(s), {len(flights)} vols sur la page 0.")

            for flight in flights:
                yield flight, fetched_at

            # Pages 1 à N-1
            for page_num in range(1, total_pages):
                # AJOUT : Trace claire du temps d'attente imposé par l'API
                logger.info(f"[ANTI-THROTTLE] Temporisation de {SLEEP_BETWEEN_REQUESTS}s avant la page {page_num}...")
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                try:
                    page_data = _fetch_page(api_key, start_range, end_range, page_num)
                except requests.exceptions.RequestException as e:
                    # Si une page échoue après tous les retries, on la saute (données partielles
                    # préférées à un run complet en échec). L'erreur est loggée.
                    logger.warning(f"[PAGE SKIPPED] La page {page_num} a echoue apres retries, skip : {e}")
                    continue

                page_flights = page_data.get("operationalFlights", [])
                # AJOUT : Log de comptage des éléments de la page
                logger.info(f"[DATA] Page {page_num} lue : {len(page_flights)} vols extraits.")
                for flight in page_flights:
                    yield flight, fetched_at
        except Exception as e:
            logger.error(f"[WINDOW CRITICAL FAILED] Extraction interrompue pour la fenetre courante : {e}")

        # Avancer d'un jour pour la prochaine itération
        current = window_end


# ─────────────────────────────────────────────────────────────────────────────
# AJOUT METRICS : Écriture finale dans logs.job_runs de Supabase
# ─────────────────────────────────────────────────────────────────────────────

def _write_execution_report_to_supabase():
    """Insère l'audit quantitatif final dans la table d'historique de production."""
    logger.info("[AUDIT] Integration du rapport de log dans la table logs.job_runs...")
    try:
        # Calcul du statut consolidé
        if run_report["pages_error_count"] == 0:
            status = "SUCCESS"
        elif run_report["pages_recovered"] > 0:
            status = "PARTIAL"
        else:
            status = "FAILED"

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode="require",
            connect_timeout=10
        )
        cur = conn.cursor()
        
        query = """
            INSERT INTO logs.job_runs (
                id, job_name, layer, status, 
                pages_recovered, pages_error_count, errors_log, 
                created_at, execution_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), CURRENT_DATE);
        """
        
        cur.execute(query, (
            run_report["run_id"],
            run_report["job_name"],
            run_report["layer"],
            status,
            run_report["pages_recovered"],
            run_report["pages_error_count"],
            json.dumps(run_report["errors_log"]) # Conversion de la liste en format JSON natif pour postgres
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[AUDIT SUCCESS] Enregistrement job fait. Status: {status} | ID: {run_report['run_id']}")
    except Exception as e:
        logger.error(f"[AUDIT ERROR] Impossible d'ecrire dans logs.job_runs : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Gestion de la fenêtre temporelle (incrémental vs fixe)
# ─────────────────────────────────────────────────────────────────────────────

def _get_dates(
    start_date: str | None,
    end_date: str | None,
    incremental: bool = True,
):
    """Détermine la fenêtre [start, end] à extraire.

    Priorité :
      1. Mode incrémental (incremental=True) + state dlt disponible
         → reprend depuis la last_window_end du dernier run réussi.
      2. start_date fourni (depuis config.toml [sources.afklm])
         → fenêtre fixe définie par l'opérateur.
      3. Fallback : hier → maintenant (utile pour un premier run sans config).

    Le state dlt est un dict persisté entre les runs dans ~/.dlt/pipelines/afklm/.
    """
    state = {}
    try:
        state = dlt.current.source_state()
    except Exception:
        pass  # Pas de state disponible (test, dry-run)

    last_end = state.get("last_window_end") if incremental else None

    if last_end:
        start = datetime.fromisoformat(last_end.replace("Z", "+00:00"))
        # AJOUT : Suivi textuel du mode d'ingestion incrémental
        logger.info(f"[INCREMENTAL] Reprise automatique dlt depuis le dernier enregistrement : {start.isoformat()}")
    elif start_date:
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        # AJOUT : Suivi textuel du mode d'ingestion à date fixe
        logger.info(f"[FIXED WINDOW] Utilisation de la date d'initialisation config : {start.isoformat()}")
    else:
        start = datetime.now(timezone.utc) - timedelta(days=1)
        # AJOUT : Suivi textuel du mode fallback
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
    airline = flight.get("airline") or {}  # Guard : "airline" peut être null dans l'API
    return {
        "id":                   flight.get("id"),               # ex. "20260116+AF+0605"
        "flight_number":        flight.get("flightNumber"),     # ex. 605 (int)
        "flight_schedule_date": flight.get("flightScheduleDate"),  # ex. "2026-01-16" (string)
        "airline_code":         airline.get("code"),            # ex. "AF"
        "airline_name":         airline.get("name"),            # ex. "Air France"
        "haul":                 flight.get("haul"),             # ex. "LONG" ou "SHORT"
        "route":                flight.get("route"),            # ex. ["CDG", "JFK"]
        "flight_status_public": flight.get("flightStatusPublic"),  # ex. "OnTime", "Delayed"
        "fetched_at":           fetched_at,                     # Horodatage du run
    }


def _build_legs_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_legs."""
    flight_id = flight.get("id")
    rows = []

    for i, leg in enumerate(flight.get("flightLegs") or []):
        # UUID déterministe pour le leg (flight_id + position dans la liste)
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))

        # Déstructuration des blocs imbriqués avec guards (or {}) sur chaque niveau
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
            "leg_order":                i,                               # 0 = premier segment
            "departure_airport_code":   dep_airport.get("code"),        # ex. "CDG"
            "arrival_airport_code":     arr_airport.get("code"),        # ex. "JFK"
            "departure_airport_name":   dep_airport.get("name"),        # ex. "KASTRUP AIRPORT"
            "arrival_airport_name":     arr_airport.get("name"),        # ex. "KASTRUP AIRPORT"
            "published_status":         leg.get("publishedStatus"),     # ex. "OnTime"
            "scheduled_departure":      dep_times.get("scheduled"),     # ISO 8601 string
            "actual_departure":         dep_times.get("actual"),        # Null si pas encore parti
            "scheduled_arrival":        arr_times.get("scheduled"),
            "actual_arrival":           arr_times.get("actual"),
            "scheduled_flight_duration": leg.get("scheduledFlightDuration"),  # ex. "PT7H30M"
            "cancelled":                irreg.get("cancelled") == "Y", # Converti en booléen Python
            "aircraft_code":       (leg.get("aircraft") or {}).get("typeCode"),  # ex. "77W"
            "aircraft_name":       (leg.get("aircraft") or {}).get("typeName"),  # ex. "EMBRAER 195 AND LEGACY 1000"
            "departure_city_code":      dep_city.get("code"), # ex. "PAR"
            "departure_city_name":      dep_city.get("name"), # ex. "SAVANNAH"
            "departure_country_code":      dep_country.get("code"), # ex. "FR"
            "departure_country_name":      dep_country.get("name"), # ex. "FRANCE"
            "arrival_city_code":      arr_city.get("code"), # ex. "PAR"
            "arrival_city_name":      arr_city.get("name"), # ex. "SAVANNAH"
            "arrival_country_code":      arr_country.get("code"), # ex. "FR"
            "arrival_country_name":      arr_country.get("name"), # ex. "FRANCE"
        })
    return rows


def _build_delays_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_delays."""
    flight_id = flight.get("id")
    rows = []

    for i, leg in enumerate(flight.get("flightLegs") or []):
        leg_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}"))
        irreg = leg.get("irregularity") or {}

        # Format 1 : objet structuré (préféré)
        delay_infos = irreg.get("delayInformation") or []

        if not delay_infos:
            # Format 2 : listes parallèles — reconstruit un format uniforme
            codes     = irreg.get("delayCode") or []
            durations = irreg.get("delayDuration") or []
            delay_infos = [
                {"delayCode": c, "delayDuration": d}
                for c, d in zip(codes, durations)
            ]

        for j, d in enumerate(delay_infos):
            rows.append({
                "id":            str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{flight_id}_{i}_{j}")),
                "flight_leg_id": leg_id,
                "delay_code":    d.get("delayCode"),     # Code IATA du retard (ex. "93")
                "delay_reason":  d.get("delayReasonPublicLangTransl"),     # ex. "This flight was delayed due to unfavourable we..."
                "delay_duration": d.get("delayDuration"), # Durée en minutes (string)
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Resources dlt (une resource = une table Supabase)
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
    api_key: str = dlt.secrets.value,         # Lu depuis .dlt/secrets.toml [sources.afklm]
    start_date: str | None = dlt.config.value, # Lu depuis .dlt/config.toml [sources.afklm]
    end_date: str | None = dlt.config.value,
    incremental: bool = True,
):
    """Source dlt AF/KLM : 1 seul fetch API, dispatch vers 3 tables."""
    start, end = _get_dates(start_date, end_date, incremental)
    # AJOUT : Trace de début d'exécution pour Airflow
    logger.info(f"[START RUN] Lancement de l'extraction de l'API Air France-KLM du {start.isoformat()} au {end.isoformat()}")

    all_flights_rows = []
    all_legs_rows    = []
    all_delays_rows  = []

    # Parcours unique de l'API — alimente les 3 tables simultanément
    for flight, fetched_at in _iter_flights(api_key, start, end):
        all_flights_rows.append(_build_flights_table(flight, fetched_at))
        all_legs_rows.extend(_build_legs_table(flight))
        all_delays_rows.extend(_build_delays_table(flight))

    # AJOUT : Résumé quantitatif global de la collecte
    logger.info(
        f"[EXTRACT METRICS SUMMARY] Donnees normalisees pretes a l'envoi — "
        f"Vols: {len(all_flights_rows)} | Segments (Legs): {len(all_legs_rows)} | Retards (Delays): {len(all_delays_rows)}"
    )

    # Yield les 3 resources → dlt les charge en parallèle dans Supabase
    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)

    # Mémorise la fin de la fenêtre dans le state dlt pour le prochain run incrémental.
    try:
        dlt.current.source_state()["last_window_end"] = end.isoformat()
        # AJOUT : Confirmation de sauvegarde de l'état
        logger.info(f"[STATE PERSISTED] Curseur incremental mis a jour avec succes : {end.isoformat()}")
    except Exception as e:
        # AJOUT : Log d'alerte non bloquante
        logger.warning(f"[STATE WARNING] Impossible d'enregistrer l'etat incremental : {e}")

    # AJOUT METRICS : Appel final pour persister le log de performance du Job
    _write_execution_report_to_supabase()