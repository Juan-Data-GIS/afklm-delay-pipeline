-- int.flight_data__int_aircraft_delays
-- Feature ML : Proportion de retards sur les sept derniers jours pour chaque appareil 
-- Calcule la proportion de vols en retard sur les sept jours précédant une date pour chaque modalité d'appareil
-- Est-ce que ce type d'appareil a tendance à provoquer du retard ? 
-- 1 ligne par (aircraftCode, flightScheduleDate).
{{
  config(
    schema='int',
    materialized='incremental',
    unique_key=['aircraft_code', 'flight_schedule_date'],
    partition_by={'field': 'flight_schedule_date', 'data_type': 'date'}
  )
}}

WITH base AS (
  SELECT
    l.aircraft_code,
    cast(f.flight_schedule_date as DATE) as flight_schedule_date,
    CASE WHEN d.delay_duration != '00' THEN 1 ELSE 0 END as is_delayed
  FROM {{ ref('flight_data__source_operational_flight_legs') }} l
  JOIN {{ ref('flight_data__source_operational_flights') }} f
    ON l.flight_id = f.id
  JOIN {{ ref('flight_data__source_operational_flight_delays') }} d
    ON l.id = d.flight_leg_id
  WHERE l.cancelled = false
  {% if is_incremental() %}
    AND cast(f.flight_schedule_date as DATE) >= (
      SELECT COALESCE(
        MAX(flight_schedule_date) - INTERVAL '7 days',
        '1970-01-01'::DATE
      ) FROM {{ this }}
    )
  {% endif %}
),

daily_counts AS (
  SELECT
    aircraft_code,
    flight_schedule_date,
    SUM(is_delayed) as delayed_count,
    COUNT(*) as total_count
  FROM base
  GROUP BY aircraft_code, flight_schedule_date
)

SELECT
  aircraft_code,
  flight_schedule_date,
  SUM(delayed_count) OVER (
    PARTITION BY aircraft_code
    ORDER BY flight_schedule_date
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
  ) * 100.0 /
  NULLIF(
    SUM(total_count) OVER (
      PARTITION BY aircraft_code
      ORDER BY flight_schedule_date
      RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
    ),
    0
  ) as aircraft_delayed_share
FROM daily_counts