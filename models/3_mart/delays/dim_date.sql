-- mart.dim_date
-- Dimension date issue des dates de vol présentes dans les données.
-- Ajoute year, month, day_of_week — les autres features temporelles (heure, jour du mois)
-- sont déjà dans fct_flight_legs et n'ont pas besoin d'être répétées ici.
-- À enrichir avec : semaine ISO, flag week-end, flag jour férié, saison, vacances scolaires.
-- Grain : 1 ligne par flight_schedule_date.
{{ config(schema='mart', materialized='table') }}
select distinct
    flight_schedule_date as date_key,
    flight_schedule_date,
    extract(dow from flight_schedule_date)::int as day_of_week,
    extract(month from flight_schedule_date)::int as month,
    extract(year from flight_schedule_date)::int as year
from {{ ref('flight_data__int_legs_ready') }}
where flight_schedule_date is not null
