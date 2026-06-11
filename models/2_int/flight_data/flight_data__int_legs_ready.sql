-- int.flight_data__int_legs_ready
-- Modèle int final : agrège toutes les features nécessaires au ML en une seule ligne par leg.
-- Construit en trois étapes (CTEs chaînées) :
--   1. base          : reprend int_delays_leg + features temporelles (heure, jour, mois, jour du mois).
--   2. dep_congestion: joint la congestion de l'aéroport de départ pour ce jour.
--   3. arr_congestion: joint la congestion de l'aéroport d'arrivée pour ce jour.
-- is_delayed = true si l'un des trois indicateurs de retard atteint 15 minutes ou plus
--              (seuil standard IATA de retard significatif).
-- Grain : 1 ligne par leg. Alimentation directe de fct_flight_legs (mart).
{{ config(schema='int', materialized='view') }}

with base as (
    select
        d.leg_id,
        d.flight_id,
        d.flight_number,
        d.flight_schedule_date,
        d.airline_code,
        d.airline_name,
        d.departure_airport_code,
        d.arrival_airport_code,
        d.departure_airport_name,
        d.arrival_airport_name,
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
        d.aircraft_code,
        d.aircraft_name,
        d.departure_city_code,
        d.departure_city_name,
        d.departure_country_code,
        d.departure_country_name,
        d.arrival_city_code,
        d.arrival_city_name,
        d.arrival_country_code,
        d.arrival_country_name,
        {{ parse_iso8601_duration_minutes('d.scheduled_flight_duration') }} as scheduled_flight_duration_minutes,
        extract(dow from d.scheduled_departure)::int as departure_weekday,
        extract(month from d.scheduled_departure)::int as departure_month,
        extract(hour from d.scheduled_departure)::int as departure_hour,
        extract(day from d.scheduled_departure)::int as departure_monthday
    from {{ ref('flight_data__int_delays_leg') }} d
),
with_dep_congestion as (
    select
        b.*,
        coalesce(dep.nb_flight_departing, 0) as nb_flight_departing_departure_airport,
        coalesce(dep.nb_flight_arriving, 0) as nb_flight_arriving_departure_airport
    from base b
    left join {{ ref('flight_data__int_airport_congestion') }} dep
        on b.departure_airport_code = dep.airport_code
        and b.flight_schedule_date = dep.flight_schedule_date
),
with_arr_congestion as (
    select
        w.*,
        coalesce(arr.nb_flight_departing, 0) as nb_flight_departing_arrival_airport,
        coalesce(arr.nb_flight_arriving, 0) as nb_flight_arriving_arrival_airport
    from with_dep_congestion w
    left join {{ ref('flight_data__int_airport_congestion') }} arr
        on w.arrival_airport_code = arr.airport_code
        and w.flight_schedule_date = arr.flight_schedule_date
),
with_delay_airport as (
    select
        a.*,
        coalesce(dap.departure_airport_delayed_share, 0) as departure_airport_delayed_share
    from with_arr_congestion a
    left join {{ ref('flight_data__int_airport_delays') }} dap
        on a.departure_airport_code = dap.departure_airport_code
        --and a.flight_schedule_date = dap.flight_schedule_date
        and cast(a.flight_schedule_date as DATE) = cast(dap.flight_schedule_date as DATE)

),
with_delay_aircraft as (
    select
        wdap.*,
        coalesce(dac.aircraft_delayed_share, 0) as aircraft_delayed_share
    from with_delay_airport wdap
    left join {{ ref('flight_data__int_aircraft_delays') }} dac
        on wdap.aircraft_code = dac.aircraft_code
        and cast(wdap.flight_schedule_date as DATE) = cast(dac.flight_schedule_date as DATE)
),
with_delay_airline as (
    select
        wdac.*,
        coalesce(dal.airline_delayed_share, 0) as airline_delayed_share
    from with_delay_aircraft wdac
    left join {{ ref('flight_data__int_airline_delays') }} dal
        on wdac.airline_code = dal.airline_code
        and cast(wdac.flight_schedule_date as DATE) = cast(dal.flight_schedule_date as DATE)
)
select
    leg_id,
    flight_id,
    flight_number,
    flight_schedule_date,
    airline_code,
    airline_name,
    departure_airport_code,
    arrival_airport_code,
    departure_airport_name,
    arrival_airport_name,
    scheduled_departure,
    actual_departure,
    scheduled_arrival,
    actual_arrival,
    cancelled,
    delay_code,
    delay_duration_minutes,
    departure_delay_minutes,
    arrival_delay_minutes,
    scheduled_flight_duration_minutes,
    aircraft_code,
    aircraft_name,
    departure_city_code,
    departure_city_name,
    departure_country_code,
    departure_country_name,
    arrival_city_code,
    arrival_city_name,
    arrival_country_code,
    arrival_country_name,
    departure_weekday,
    departure_month,
    departure_hour,
    departure_monthday,
    nb_flight_departing_departure_airport,
    nb_flight_arriving_departure_airport,
    nb_flight_departing_arrival_airport,
    nb_flight_arriving_arrival_airport,
    departure_airport_delayed_share,
    aircraft_delayed_share,
    airline_delayed_share,
    case
        when coalesce(departure_delay_minutes, 0) >= 15
          or coalesce(arrival_delay_minutes, 0) >= 15
          or coalesce(delay_duration_minutes, 0) >= 15
        then true
        else false
    end as is_delayed
from with_delay_airline