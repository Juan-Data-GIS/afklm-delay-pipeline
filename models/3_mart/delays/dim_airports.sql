-- mart.dim_airports
-- Dimension dégénérée : liste distincte des aéroports (départ ∪ arrivée) présents dans les données.
-- En l'état, n'apporte pas d'attributs enrichissants au-delà du code (déjà dans fct_flight_legs).
-- À enrichir avec : nom, ville, pays, timezone, hub flag, taille (nb de gates).
-- Grain : 1 ligne par airport_code.
{{ config(schema='mart', materialized='table') }}
select distinct airport_code as airport_key from (
    select departure_airport_code as airport_code from {{ ref('flight_data__int_legs_ready') }}
    union
    select arrival_airport_code from {{ ref('flight_data__int_legs_ready') }}
) u
where airport_code is not null
