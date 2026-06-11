-- int.flight_data__int_airport_delays
-- Feature ML :  Proportion de retards sur les sept derniers jours pour chaque aéroport de départ  
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'aéroport de départ 
-- Est-ce que l'aéroport a tendance à avoir des vols en retard ? 
-- Grain : 1 ligne par (airportCode, flightScheduleDate).
{{ config(schema='int', materialized='table') }}

-- Étape 1 : Pré-agrégation quotidienne par aéroport de départ
with daily_airport_stats as (
    select
        l.departure_airport_code,
        cast(f.flight_schedule_date as date) as flight_date,
        case when d.delay_duration != '00' then 1 else 0 end as is_delayed
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = false
)
-- Étape 2 : Calcul de la feature ML glissante sur les 7 derniers jours
select
    t1.departure_airport_code,
    t1.flight_date as flight_schedule_date,
    round(sum(t2.is_delayed) * 100.0 / nullif(count(t2.is_delayed), 0), 2) as departure_airport_delayed_share
from daily_airport_stats t1
left join daily_airport_stats t2
    on t1.departure_airport_code = t2.departure_airport_code
    and t2.flight_date between t1.flight_date - interval '7 days' and t1.flight_date
group by t1.departure_airport_code, t1.flight_date