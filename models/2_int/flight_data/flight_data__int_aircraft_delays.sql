-- int.flight_data__int_aircraft_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque appareil 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'appareil
-- Est-ce que ce type d'appareil a tendance à provoquer du retard ? 
-- 1 ligne par (aircraftCode, flightScheduleDate).
{{ config(schema='int', materialized='view') }}

with flights_within_7_days as (
    select
        l.aircraft_code,
        d.delay_duration,
        l.cancelled,
        cast(f.flight_schedule_date as DATE) 
    from {{ ref('flight_data__source_operational_flight_legs') }} l
    join {{ ref('flight_data__source_operational_flights') }} f on l.flight_id = f.id
    join {{ ref('flight_data__source_operational_flight_delays') }} d on l.id = d.flight_leg_id
    where l.cancelled = false
)
select 
    fsd.aircraft_code,
    fsd.flight_schedule_date,
        sum(CASE WHEN fsd2.delay_duration != '00' THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(fsd2.delay_duration),0) as aircraft_delayed_share
FROM flights_within_7_days fsd left join flights_within_7_days fsd2 
    ON fsd.aircraft_code = fsd2.aircraft_code
        AND fsd2.flight_schedule_date BETWEEN fsd.flight_schedule_date - INTERVAL '7 days' AND fsd.flight_schedule_date
        GROUP BY
    fsd.aircraft_code,
    fsd.flight_schedule_date


    