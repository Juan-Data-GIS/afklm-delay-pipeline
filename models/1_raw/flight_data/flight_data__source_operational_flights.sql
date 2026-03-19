-- raw.flight_data__source_operational_flights
-- 1:1 avec operational_flights, cast uniquement
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
