-- mart.dim_airports
-- Dimension dégénérée : liste distincte des aéroports (départ ∪ arrivée) présents dans les données.
-- En l'état, n'apporte pas d'attributs enrichissants au-delà du code (déjà dans fct_flight_legs).
-- À enrichir avec : nom, ville, pays, timezone, hub flag, taille (nb de gates).
-- Grain : 1 ligne par airport_code.
{{ config(schema='mart', materialized='table') }}
SELECT DISTINCT
    airport_code AS airport_key,
    airport_name
    city_code, 
    city_name,
    country_code,
    country_name
FROM (
    SELECT
        departure_airport_code AS airport_code,
        departure_airport_name AS airport_name,
        departure_city_code AS city_code,
        departure_city_name AS city_name,
        departure_country_code AS country_code,
        departure_country_name AS country_name
    FROM {{ ref('flight_data__int_legs_ready') }}
    WHERE departure_airport_code IS NOT NULL

    UNION

    SELECT
        arrival_airport_code AS airport_code,
        arrival_airport_name AS airport_name,
        arrival_city_code AS city_code,
        arrival_city_name AS city_name,
        arrival_country_code AS country_code,
        arrival_country_name AS country_name
    FROM {{ ref('flight_data__int_legs_ready') }}
    WHERE arrival_airport_code IS NOT NULL
) u
WHERE u.airport_code IS NOT NULL
