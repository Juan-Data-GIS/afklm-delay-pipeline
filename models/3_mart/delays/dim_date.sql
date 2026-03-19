-- mart.dim_date
-- Lit depuis int_legs_ready
{{ config(schema='mart', materialized='table') }}
select distinct
    flight_schedule_date as date_key,
    flight_schedule_date,
    extract(dow from flight_schedule_date)::int as day_of_week,
    extract(month from flight_schedule_date)::int as month,
    extract(year from flight_schedule_date)::int as year
from {{ ref('flight_data__int_legs_ready') }}
where flight_schedule_date is not null
