-- int.flight_data__int_airline_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque compagnie 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité de compagnie aérienne
-- Est-ce que cette compagnie a tendance à avoir des vols en retard ? 
-- 1 ligne par (airlineCode, flightScheduleDate) .
{{ config(
    schema='int', 
    materialized='table',
    indexes=[
      {'columns': ['airline_code', 'flight_schedule_date'], 'type': 'btree'}
    ]
) }}

with daily_airline_stats as (
    select
        f.airline_code,
        cast(f.flight_schedule_date as date) as flight_date,
        case when d.delay_duration != '00' then 1 else 0 end as is_delayed
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = false and f.airline_code is not null
)
select 
    t1.airline_code,
    t1.flight_date as flight_schedule_date,
    round(sum(t2.is_delayed) * 100.0 / nullif(count(t2.is_delayed), 0), 2) as airline_delayed_share
from daily_airline_stats t1
join daily_airline_stats t2 
    on t1.airline_code = t2.airline_code
    and t2.flight_date between t1.flight_date - interval '7 days' and t1.flight_date
group by t1.airline_code, t1.flight_date