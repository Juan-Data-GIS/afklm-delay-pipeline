-- raw.flight_data__source_operational_flight_legs
-- Entité : un leg = un tronçon physique d'un vol entre deux aéroports (décollage → atterrissage).
-- Un vol peut avoir plusieurs legs (ex. CDG → NBO → JNB = 2 legs pour le même vol).
-- Source : table `operational_flight_legs` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par leg (leg_order distingue les tronçons d'un même vol).
-- Rôle   : cast des timestamps en timestamptz. Aucune transformation métier.
{{ config(schema='raw', materialized='view') }}
select
    id,
    flight_id,
    leg_order,
    departure_airport_code,
    arrival_airport_code,
    published_status,
    scheduled_departure::timestamptz as scheduled_departure,
    actual_departure::timestamptz as actual_departure,
    scheduled_arrival::timestamptz as scheduled_arrival,
    actual_arrival::timestamptz as actual_arrival,
    scheduled_flight_duration,
    cancelled,
    aircraft_type_code
from {{ source('flight_data', 'operational_flight_legs') }}
