-- int.flight_data__int_airport_congestion
-- Feature ML : proxy de congestion aéroportuaire.
-- Calcule, pour chaque aéroport et chaque jour, le nombre de vols au départ et à l'arrivée.
-- Un aéroport très chargé ce jour-là est plus susceptible de générer des retards en cascade.
-- FULL OUTER JOIN departures/arrivals : capture les aéroports qui ne figurent que d'un seul côté.
-- Grain : 1 ligne par (airport_code, flight_schedule_date).
{{ config(schema='int', materialized='view') }}

with legs_with_date as (
    select
        l.departure_airport_code,
        l.arrival_airport_code,
        f.flight_schedule_date
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
),
departures as (
    select
        departure_airport_code as airport_code,
        flight_schedule_date,
        count(*) as nb_departing
    from legs_with_date
    where departure_airport_code is not null
    group by departure_airport_code, flight_schedule_date
),
arrivals as (
    select
        arrival_airport_code as airport_code,
        flight_schedule_date,
        count(*) as nb_arriving
    from legs_with_date
    where arrival_airport_code is not null
    group by arrival_airport_code, flight_schedule_date
)
select
    coalesce(d.airport_code, a.airport_code) as airport_code,
    coalesce(d.flight_schedule_date, a.flight_schedule_date) as flight_schedule_date,
    coalesce(d.nb_departing, 0) as nb_departing,
    coalesce(a.nb_arriving, 0) as nb_arriving
from departures d
full outer join arrivals a
    on d.airport_code = a.airport_code
    and d.flight_schedule_date = a.flight_schedule_date
