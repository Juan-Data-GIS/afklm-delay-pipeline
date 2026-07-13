-- mart.fct_flight_legs
-- Table de faits centrale de la pipeline. Consommée par ml_score.py et Metabase.
-- Contient toutes les features ML prêtes à l'emploi : temporelles, congestion, retards, cible (is_delayed).
-- Aucune transformation : toute la logique métier est dans les couches int.
-- Les colonnes airport/airline/date sont renommées en *_key pour indiquer leur rôle de clé étrangère
-- vers les dimensions (dim_airlines, dim_airports, dim_date).
-- Grain : 1 ligne par leg (tronçon physique d'un vol).

{{ config(
    schema='mart', 
    materialized='incremental',
    unique_key='leg_id',
    pre_hook="SET statement_timeout = '600000';"
) }}
-- a laisser lors d'un full refresh / sécurité "anti-crash".

select
    leg_id,
    flight_id,
    flight_number,
    airline_code as airline_key,
    airline_name,
    departure_airport_code as departure_airport_key,
    arrival_airport_code as arrival_airport_key,
    departure_airport_name,
    arrival_airport_name,
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
    scheduled_flight_duration_minutes,
    aircraft_code,
    aircraft_name,
    departure_weekday,
    departure_month,
    departure_hour,
    departure_monthday,
    nb_flight_departing_departure_airport,
    nb_flight_arriving_departure_airport,
    nb_flight_departing_arrival_airport,
    nb_flight_arriving_arrival_airport,
    departure_airport_delayed_share,
    aircraft_delayed_share,
    airline_delayed_share,
    is_delayed
from {{ ref('flight_data__int_legs_ready') }}

{% if is_incremental() %}
  -- Mode incrémental : On ne traite que les vols récents pour éviter le Full Table Scan.
  -- On remonte 3 jours en arrière par sécurité pour capter les mises à jour de statuts de vols (actual_arrival etc.)
  where scheduled_departure >= (select max(scheduled_departure) - interval '3 day' from {{ this }})
{% endif %}