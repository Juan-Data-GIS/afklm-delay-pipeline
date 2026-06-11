-- int.flight_data__int_aircraft_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque appareil 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'appareil
-- Est-ce que ce type d'appareil a tendance à provoquer du retard ? 
-- 1 ligne par (aircraftCode, flightScheduleDate).
{{ config(schema='int', materialized='table') }}

with daily_aircraft_stats as (
    select
        l.aircraft_code,
        cast(f.flight_schedule_date as date) as flight_date,
        case when d.delay_duration != '00' then 1 else 0 end as is_delayed
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = false and l.aircraft_code is not null
)
select 
    t1.aircraft_code,
    t1.flight_date as flight_schedule_date,
    round(sum(t2.is_delayed) * 100.0 / nullif(count(t2.is_delayed), 0), 2) as aircraft_delayed_share
from daily_aircraft_stats t1
-- Passage en INNER JOIN pour optimiser le plan d'exécution de PostgreSQL
join daily_aircraft_stats t2 
    on t1.aircraft_code = t2.aircraft_code
    and t2.flight_date between t1.flight_date - interval '7 days' and t1.flight_date
group by t1.aircraft_code, t1.flight_date