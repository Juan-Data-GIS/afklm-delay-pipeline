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
from datetime import datetime, timezone, timedelta

import dlt
import requests

# Logger namespaced sous "dlt.sources.afklm" pour apparaître dans les logs dlt
# avec le bon niveau de verbosité (contrôlé par [runtime] log_level dans config.toml).
logger = logging.getLogger("dlt.sources.afklm")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.airfranceklm.com/opendata"

# Nombre de vols par page. L'API AF/KLM peut retourner des 500 si > ~100.
# 50 est un compromis stable entre performance et fiabilité.
PAGE_SIZE = 50

# Pause (secondes) entre deux pages consécutives d'une même fenêtre.
# Évite de déclencher le rate limit de l'API.
SLEEP_BETWEEN_REQUESTS = 5

# Nombre maximum de tentatives sur erreur 5xx avant de déclarer la page en échec.
MAX_RETRIES = 5

# Délais de backoff progressifs (secondes) entre chaque retry sur erreur 5xx.
# Les 5xx AF/KLM sont souvent des throttles déguisés, pas de vraies pannes serveur.
# La progression laisse le temps au serveur de récupérer.
RETRY_BACKOFF_500 = [30, 60, 90, 120, 180]


# ─────────────────────────────────────────────────────────────────────────────
# Couche HTTP
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page(
    api_key: str,
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
    headers = {
        "API-Key": api_key,               # Authentification AF/KLM (header, pas Bearer)
        "Accept": "application/hal+json", # Format HAL+JSON attendu
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()  # Lève HTTPError sur tout code >= 400
            return resp.json()

        except requests.exceptions.RequestException as e:
            resp_obj = getattr(e, "response", None)

            # Retry uniquement sur erreurs serveur transitoires
            if (
                resp_obj is not None
                and resp_obj.status_code in (500, 502, 503, 504)
                and attempt < MAX_RETRIES - 1
            ):
                backoff = RETRY_BACKOFF_500[min(attempt, len(RETRY_BACKOFF_500) - 1)]
                logger.warning(
                    "Erreur %s (tentative %d/%d), retry dans %ds...",
                    resp_obj.status_code, attempt + 1, MAX_RETRIES, backoff,
                )
                time.sleep(backoff)
                continue

            # Toute autre erreur (4xx, réseau, timeout) → lever immédiatement
            raise


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

        logger.info("Fetch window %s → %s", start_range, end_range)

        # Page 0 : contient aussi la métadonnée totalPages
        data = _fetch_page(api_key, start_range, end_range, 0)
        total_pages = data.get("page", {}).get("totalPages", 0)
        flights = data.get("operationalFlights", [])
        logger.info("  → %d page(s), %d vols (page 0)", total_pages, len(flights))

        for flight in flights:
            yield flight, fetched_at

        # Pages 1 à N-1
        for page_num in range(1, total_pages):
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            try:
                page_data = _fetch_page(api_key, start_range, end_range, page_num)
            except requests.exceptions.RequestException as e:
                # Si une page échoue après tous les retries, on la saute (données partielles
                # préférées à un run complet en échec). L'erreur est loggée.
                logger.warning("Page %d échouée après retries, skip : %s", page_num, e)
                continue

            page_flights = page_data.get("operationalFlights", [])
            logger.info("  → page %d : %d vols", page_num, len(page_flights))
            for flight in page_flights:
                yield flight, fetched_at

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
    elif start_date:
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    else:
        start = datetime.now(timezone.utc) - timedelta(days=1)

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
    """Transforme un vol brut en dict pour la table operational_flights.

    Les valeurs sont gardées en types bruts (strings) — le casting en types
    SQL (DATE, UUID) est délégué à dbt 1_raw (::date, ::uuid).
    Cela évite les conflits de types entre dlt et les colonnes Supabase.
    """
    airline = flight.get("airline") or {}  # Guard : "airline" peut être null dans l'API
    return {
        "id":                   flight.get("id"),               # ex. "20260116+AF+0605"
        "flight_number":        flight.get("flightNumber"),     # ex. 605 (int)
        "flight_schedule_date": flight.get("flightScheduleDate"),  # ex. "2026-01-16" (string)
        "airline_code":         airline.get("code"),            # ex. "AF"
        "airline_name":         airline.get("name"),            # ex. "Air France"
        "haul":                 flight.get("haul"),             # ex. "LONG" ou "SHORT"
        "route":                flight.get("route"),            # ex. ["CDG", "JFK"]
        # route est une liste → dlt crée automatiquement une table enfant
        # operational_flights__route (1 ligne par code aéroport).
        "flight_status_public": flight.get("flightStatusPublic"),  # ex. "OnTime", "Delayed"
        "fetched_at":           fetched_at,                     # Horodatage du run
    }


def _build_legs_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_legs.

    Un vol peut avoir plusieurs segments (legs) : ex. CDG→AMS→JFK = 2 legs.
    Chaque leg produit une ligne.

    Identifiant de leg : uuid5(NAMESPACE_DNS, "{flight_id}_{i}")
      - Déterministe : le même leg produit le même UUID à chaque run.
      - Cela garantit que le merge dlt (primary_key="id") détecte correctement
        les doublons et fait un UPDATE plutôt qu'un INSERT.
    """
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
        dep_times   = dep_info.get("times") or {}
        arr_times   = arr_info.get("times") or {}
        irreg       = leg.get("irregularity") or {}

        rows.append({
            "id":                       leg_id,
            "flight_id":                flight_id,
            "leg_order":                i,                              # 0 = premier segment
            "departure_airport_code":   dep_airport.get("code"),        # ex. "CDG"
            "arrival_airport_code":     arr_airport.get("code"),        # ex. "JFK"
            "published_status":         leg.get("publishedStatus"),     # ex. "OnTime"
            "scheduled_departure":      dep_times.get("scheduled"),     # ISO 8601 string
            "actual_departure":         dep_times.get("actual"),        # Null si pas encore parti
            "scheduled_arrival":        arr_times.get("scheduled"),
            "actual_arrival":           arr_times.get("actual"),
            "scheduled_flight_duration": leg.get("scheduledFlightDuration"),  # ex. "PT7H30M"
            "cancelled":                irreg.get("cancelled") == "Y", # Converti en booléen Python
            "aircraft_type_code":       (leg.get("aircraft") or {}).get("typeCode"),  # ex. "77W"
        })
    return rows


def _build_delays_table(flight: dict) -> list[dict]:
    """Transforme un vol brut en liste de dicts pour la table operational_flight_delays.

    Un leg peut avoir plusieurs codes retard (ex. retard météo + retard ATC).
    Chaque code retard produit une ligne.

    L'API AF/KLM renvoie les retards dans deux formats possibles :
      - Format 1 : "delayInformation": [{"delayCode": "93", "delayDuration": "45"}, ...]
      - Format 2 : "delayCode": ["93", "72"], "delayDuration": ["45", "20"]
    Les deux formats sont supportés.

    Identifiant : uuid5(NAMESPACE_DNS, "{flight_id}_{i}_{j}")
      - i = position du leg, j = position du retard dans le leg
      - Déterministe pour le merge dlt.
    """
    flight_id = flight.get("id")
    rows = []

    for i, leg in enumerate(flight.get("flightLegs") or []):
        # Même UUID que dans _build_legs_table pour assurer la cohérence
        # entre flight_leg_id ici et id dans operational_flight_legs.
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
                "delay_duration": d.get("delayDuration"), # Durée en minutes (string)
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Resources dlt (une resource = une table Supabase)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="operational_flights", write_disposition="merge", primary_key="id")
def operational_flights_resource(flights_rows):
    """Resource dlt pour operational_flights.

    write_disposition="merge" + primary_key="id" :
      - Si un vol avec le même id existe déjà → UPDATE (mise à jour du statut, etc.)
      - Sinon → INSERT
    Reçoit une liste de dicts pré-construits par _build_flights_table().
    """
    yield from flights_rows


@dlt.resource(name="operational_flight_legs", write_disposition="merge", primary_key="id")
def operational_flight_legs_resource(legs_rows):
    """Resource dlt pour operational_flight_legs.

    L'id est un UUID déterministe (uuid5) — le merge fonctionne même si le vol
    est rechargé plusieurs fois (même UUID généré à partir du même flight_id + position).
    """
    yield from legs_rows


@dlt.resource(name="operational_flight_delays", write_disposition="merge", primary_key="id")
def operational_flight_delays_resource(delays_rows):
    """Resource dlt pour operational_flight_delays."""
    yield from delays_rows


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
    """Source dlt AF/KLM : 1 seul fetch API, dispatch vers 3 tables.

    Architecture :
      - UN seul passage sur l'API (_iter_flights) pour les 3 tables.
        Si chaque resource appelait _iter_flights séparément → 3× les appels API.
      - Les 3 listes (flights, legs, delays) sont construites en mémoire puis
        passées aux resources qui les yield vers dlt.
      - dlt charge les 3 tables en parallèle (20 workers par défaut).
        Les contraintes FK ont été supprimées des tables Supabase pour permettre
        ce chargement parallèle sans erreurs d'ordre d'insertion.
    """
    start, end = _get_dates(start_date, end_date, incremental)
    logger.info("Fetching flights %s → %s", start.isoformat(), end.isoformat())

    all_flights_rows = []
    all_legs_rows    = []
    all_delays_rows  = []

    # Parcours unique de l'API — alimente les 3 tables simultanément
    for flight, fetched_at in _iter_flights(api_key, start, end):
        all_flights_rows.append(_build_flights_table(flight, fetched_at))
        all_legs_rows.extend(_build_legs_table(flight))
        all_delays_rows.extend(_build_delays_table(flight))

    logger.info(
        "Fetch terminé : %d vols, %d legs, %d delays",
        len(all_flights_rows), len(all_legs_rows), len(all_delays_rows),
    )

    # Yield les 3 resources → dlt les charge en parallèle dans Supabase
    yield operational_flights_resource(all_flights_rows)
    yield operational_flight_legs_resource(all_legs_rows)
    yield operational_flight_delays_resource(all_delays_rows)

    # Mémorise la fin de la fenêtre dans le state dlt pour le prochain run incrémental.
    # Lors du prochain run, _get_dates() lira "last_window_end" et reprendra depuis là.
    try:
        dlt.current.source_state()["last_window_end"] = end.isoformat()
    except Exception:
        pass  # Silencieux si le state n'est pas disponible (tests, dry-run)
