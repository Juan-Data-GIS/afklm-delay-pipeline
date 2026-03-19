-- raw.flight_data__source_operational_flight_delays
-- 1:1 avec operational_flight_delays, cast uniquement
{{ config(schema='raw', materialized='view') }}
select
    id,
    flight_leg_id,
    delay_code,
    delay_duration
from {{ source('flight_data', 'operational_flight_delays') }}
