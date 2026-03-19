-- raw.flight_data__source_operational_flight_legs
-- 1:1 avec operational_flight_legs, cast uniquement
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
