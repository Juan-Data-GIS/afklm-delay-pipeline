-- mart.fct_flight_legs
-- Table de faits centrale de la pipeline. Consommée par ml_score.py et Metabase.
-- Contient toutes les features ML prêtes à l'emploi : temporelles, congestion, retards, cible (is_delayed).
-- Aucune transformation : toute la logique métier est dans les couches int.
-- Les colonnes airport/airline/date sont renommées en *_key pour indiquer leur rôle de clé étrangère
-- vers les dimensions (dim_airlines, dim_airports, dim_date).
-- Grain : 1 ligne par leg (tronçon physique d'un vol).
{{ config(schema='mart', materialized='table') }}
select
    leg_id,
    flight_id,
    flight_number,
    airline_code as airline_key,
    departure_airport_code as departure_airport_key,
    arrival_airport_code as arrival_airport_key,
    flight_schedule_date as date_key,
    scheduled_departure,
    actual_departure,
    scheduled_arrival,
    actual_arrival,
    cancelled,
    delay_code,
    delay_duration_minutes,
    departure_delay_minutes,
    arrival_delay_minutes,
    scheduled_flight_duration_min,
    aircraft_type_code,
    departure_weekday,
    departure_month,
    departure_hour,
    departure_monthday,
    dep_airport_nb_departing,
    dep_airport_nb_arriving,
    arr_airport_nb_departing,
    arr_airport_nb_arriving,
    is_delayed
from {{ ref('flight_data__int_legs_ready') }}
