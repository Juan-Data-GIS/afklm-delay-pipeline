-- int.flight_data__int_airport_delays
-- Feature ML :  Proportion de retards sur les sept derniers jours pour chaque aéroport de départ  
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'aéroport de départ 
-- Est-ce que l'aéroport a tendance à avoir des vols en retard ? 
-- Grain : 1 ligne par (airportCode, flightScheduleDate).
{{ config(schema='int', materialized='view') }}

with flights_within_7_days as (
    select
        l.departure_airport_code,
        d.delay_duration,
        l.cancelled,
        cast(f.flight_schedule_date as DATE) 
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = false
)
select
    fsd.departure_airport_code,
    fsd.flight_schedule_date,
    sum(CASE WHEN fsd2.delay_duration != '00' THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(fsd2.delay_duration), 0) as departure_airport_delayed_share
from flights_within_7_days fsd
left join flights_within_7_days fsd2
    on fsd.departure_airport_code = fsd2.departure_airport_code
    and fsd2.flight_schedule_date between fsd.flight_schedule_date - INTERVAL '7 days' and fsd.flight_schedule_date
group by
    fsd.departure_airport_code,
    fsd.flight_schedule_date