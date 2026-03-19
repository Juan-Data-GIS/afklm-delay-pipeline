-- mart.fct_flight_legs
-- SELECT sur int_legs_ready, renommage FK pour le ML
-- Aucune transformation — toute la logique est dans int
{{ config(schema='mart', materialized='table') }}
select
    leg_id,
    flight_id,
    flight_number,
    airline_code as airline_key,
    departure_airport_code as departure_airport_key,
    arrival_airport_code as arrival_airport_key,
    flight_schedule_date as date_key,
    scheduled_departure,
    actual_departure,
    scheduled_arrival,
    actual_arrival,
    cancelled,
    delay_code,
    delay_duration_minutes,
    departure_delay_minutes,
    arrival_delay_minutes,
    scheduled_flight_duration_min,
    aircraft_type_code,
    departure_weekday,
    departure_month,
    departure_hour,
    departure_monthday,
    dep_airport_nb_departing,
    dep_airport_nb_arriving,
    arr_airport_nb_departing,
    arr_airport_nb_arriving,
    is_delayed
from {{ ref('flight_data__int_legs_ready') }}
