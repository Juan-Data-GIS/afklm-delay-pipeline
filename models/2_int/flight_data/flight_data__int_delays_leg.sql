-- int.flight_data__int_delays_leg
-- JOIN flights + legs + delays, agrège les delays par leg, parse duration
-- Grain : 1 ligne par leg
{{ config(schema='int', materialized='view') }}

with legs as (
    select
        l.id as leg_id,
        l.flight_id,
        f.flight_number,
        f.flight_schedule_date,
        f.airline_code,
        l.leg_order,
        l.departure_airport_code,
        l.arrival_airport_code,
        l.published_status,
        l.scheduled_departure,
        l.actual_departure,
        l.scheduled_arrival,
        l.actual_arrival,
        l.scheduled_flight_duration,
        l.cancelled,
        l.aircraft_type_code,
        round(extract(epoch from (l.actual_departure - l.scheduled_departure)) / 60)::int as departure_delay_minutes,
        round(extract(epoch from (l.actual_arrival - l.scheduled_arrival)) / 60)::int as arrival_delay_minutes
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
),
delay_parsed as (
    select
        flight_leg_id,
        delay_code,
        {{ parse_iso8601_duration_minutes('delay_duration') }} as delay_min
    from {{ ref('flight_data__source_operational_flight_delays') }}
    where delay_duration is not null
),
delay_agg as (
    select
        flight_leg_id,
        sum(delay_min)::int as delay_duration_minutes,
        min(delay_code) as delay_code
    from delay_parsed
    group by flight_leg_id
)
select
    legs.leg_id,
    legs.flight_id,
    legs.flight_number,
    legs.flight_schedule_date,
    legs.airline_code,
    legs.leg_order,
    legs.departure_airport_code,
    legs.arrival_airport_code,
    legs.published_status,
    legs.scheduled_departure,
    legs.actual_departure,
    legs.scheduled_arrival,
    legs.actual_arrival,
    legs.scheduled_flight_duration,
    legs.cancelled,
    legs.aircraft_type_code,
    legs.departure_delay_minutes,
    legs.arrival_delay_minutes,
    delay_agg.delay_code,
    delay_agg.delay_duration_minutes
from legs
left join delay_agg on legs.leg_id = delay_agg.flight_leg_id
