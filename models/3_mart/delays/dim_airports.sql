-- mart.dim_airports
-- Union des codes aéroport départ et arrivée depuis int_legs_ready
{{ config(schema='mart', materialized='table') }}
select distinct airport_code as airport_key from (
    select departure_airport_code as airport_code from {{ ref('flight_data__int_legs_ready') }}
    union
    select arrival_airport_code from {{ ref('flight_data__int_legs_ready') }}
) u
where airport_code is not null
