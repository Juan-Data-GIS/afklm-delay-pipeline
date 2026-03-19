-- int.flight_data__int_legs_ready
-- Dernier modèle int : join delays_leg + airport_congestion (départ + arrivée)
-- Toutes les features ML : temporelles, congestion, is_delayed
-- Grain : 1 ligne par leg
{{ config(schema='int', materialized='view') }}

with base as (
    select
        d.leg_id,
        d.flight_id,
        d.flight_number,
        d.flight_schedule_date,
        d.airline_code,
        d.departure_airport_code,
        d.arrival_airport_code,
        d.scheduled_departure,
        d.actual_departure,
        d.scheduled_arrival,
        d.actual_arrival,
        d.cancelled,
        d.delay_code,
        d.delay_duration_minutes,
        d.departure_delay_minutes,
        d.arrival_delay_minutes,
        d.scheduled_flight_duration,
        d.aircraft_type_code,
        {{ parse_iso8601_duration_minutes('d.scheduled_flight_duration') }} as scheduled_flight_duration_min,
        extract(dow from d.scheduled_departure)::int as departure_weekday,
        extract(month from d.scheduled_departure)::int as departure_month,
        extract(hour from d.scheduled_departure)::int as departure_hour,
        extract(day from d.scheduled_departure)::int as departure_monthday
    from {{ ref('flight_data__int_delays_leg') }} d
),
with_dep_congestion as (
    select
        b.*,
        coalesce(dep.nb_departing, 0) as dep_airport_nb_departing,
        coalesce(dep.nb_arriving, 0) as dep_airport_nb_arriving
    from base b
    left join {{ ref('flight_data__int_airport_congestion') }} dep
        on b.departure_airport_code = dep.airport_code
        and b.flight_schedule_date = dep.flight_schedule_date
),
with_arr_congestion as (
    select
        w.*,
        coalesce(arr.nb_departing, 0) as arr_airport_nb_departing,
        coalesce(arr.nb_arriving, 0) as arr_airport_nb_arriving
    from with_dep_congestion w
    left join {{ ref('flight_data__int_airport_congestion') }} arr
        on w.arrival_airport_code = arr.airport_code
        and w.flight_schedule_date = arr.flight_schedule_date
)
select
    leg_id,
    flight_id,
    flight_number,
    flight_schedule_date,
    airline_code,
    departure_airport_code,
    arrival_airport_code,
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
    case
        when coalesce(departure_delay_minutes, 0) >= 15
          or coalesce(arrival_delay_minutes, 0) >= 15
          or coalesce(delay_duration_minutes, 0) >= 15
        then true
        else false
    end as is_delayed
from with_arr_congestion
