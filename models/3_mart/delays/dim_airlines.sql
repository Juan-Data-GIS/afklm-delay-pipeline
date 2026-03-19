-- mart.dim_airlines
-- Lit depuis int_legs_ready
{{ config(schema='mart', materialized='table') }}
select distinct
    airline_code as airline_key,
    airline_code
from {{ ref('flight_data__int_legs_ready') }}
where airline_code is not null
