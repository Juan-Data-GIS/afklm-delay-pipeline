-- raw.flight_data__source_operational_flight_delays
-- Entité : un événement de retard sur un leg. Un même leg peut avoir plusieurs codes retard
--          (ex. retard météo + retard technique = 2 lignes pour le même leg).
-- Source : table `operational_flight_delays` chargée par dlt depuis l'API AF/KLM.
-- Grain  : 1 ligne par événement de retard (N lignes possibles par leg).
-- Rôle   : aucun cast, aucune transformation. La durée (ISO 8601) est parsée en couche int.
{{ config(schema='raw', materialized='view') }}
select
    id,
    flight_leg_id,
    delay_code,
    delay_duration
from {{ source('flight_data', 'operational_flight_delays') }}
