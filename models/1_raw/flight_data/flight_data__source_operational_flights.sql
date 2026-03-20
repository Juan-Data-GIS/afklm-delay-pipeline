-- raw.flight_data__source_operational_flights
-- Entité : un vol = une combinaison (numéro de vol, date, compagnie).
-- Un vol peut être composé de plusieurs legs (tronçons physiques).
-- Source : table `operational_flights` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par vol.
-- Rôle   : typage uniquement (int, date, timestamptz). Aucune transformation métier.
{{ config(schema='raw', materialized='view') }}
select
    id,
    flight_number::int as flight_number,
    flight_schedule_date::date as flight_schedule_date,
    airline_code,
    airline_name,
    haul,
    route,
    flight_status_public,
    fetched_at::timestamptz as fetched_at
from {{ source('flight_data', 'operational_flights') }}
