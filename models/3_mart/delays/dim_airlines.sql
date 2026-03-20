-- mart.dim_airlines
-- Dimension dégénérée : liste distincte des compagnies présentes dans les données.
-- En l'état, n'apporte pas d'attributs enrichissants au-delà du code (déjà dans fct_flight_legs).
-- À enrichir avec : nom complet, pays, alliance (Star/SkyTeam/Oneworld), low-cost flag.
-- Grain : 1 ligne par airline_code.
{{ config(schema='mart', materialized='table') }}
select distinct
    airline_code as airline_key,
    airline_code
from {{ ref('flight_data__int_legs_ready') }}
where airline_code is not null
