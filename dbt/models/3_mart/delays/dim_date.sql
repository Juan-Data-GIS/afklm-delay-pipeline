-- mart.dim_date
-- Dimension date issue des dates de vol présentes dans les données.
-- Ajoute year, month, day_of_week — les autres features temporelles (heure, jour du mois)
-- sont déjà dans fct_flight_legs et n'ont pas besoin d'être répétées ici.
-- À enrichir avec : semaine ISO, flag week-end, flag jour férié, saison, vacances scolaires.
-- Grain : 1 ligne par flight_schedule_date.
{{ config(schema='mart', materialized='table') }}
select distinct
    flight_schedule_date as date_key,
    cast(flight_schedule_date as DATE) ,
    extract(dow from cast(flight_schedule_date as DATE))::int as day_of_week,
    extract(month from cast(flight_schedule_date as DATE))::int as month,
    extract(year from cast(flight_schedule_date as DATE))::int as year
from {{ ref('flight_data__int_legs_ready') }}
where flight_schedule_date is not null
