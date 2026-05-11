-- int.flight_data__int_airline_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque compagnie 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité de compagnie aérienne
-- Est-ce que cette compagnie a tendance à avoir des vols en retard ? 
-- 1 ligne par (airlineCode, flightScheduleDate).
{{ config(schema='int', materialized='view') }}

with flights_within_7_days as (
    select
        f.airline_code,
        d.delay_duration,
        cast(f.flight_schedule_date as DATE) ,
        l.cancelled
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = 'N'
)
select 
    fsd.airline_code,
    fsd.flight_schedule_date,
        sum(CASE WHEN fsd2.delay_duration != '00' THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(fsd2.delay_duration),0) as airline_delayed_share
FROM flights_within_7_days fsd left join flights_within_7_days fsd2 
    ON fsd.airline_code = fsd2.airline_code
        AND fsd2.flight_schedule_date BETWEEN fsd.flight_schedule_date - INTERVAL '7 days' AND fsd.flight_schedule_date
        GROUP BY
    fsd.airline_code,
    fsd.flight_schedule_date