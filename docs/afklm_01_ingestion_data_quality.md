# RUNBOOK DE PRODUCTION - DAG_01 : INGESTION & DATA QUALITY

### Liens de Référence Centraux
* **[Consulter la Documentation Architecture & Métier complète](https://docs.google.com/document/d/1b4PKEDvnL44BoxI8lgq3NKAGMQe5oYWxlz9Co0Z6Yqg/edit?tab=t.0)**
* **[Ouvrir le Manuel de Résolution des Incidents Global](https://docs.google.com/document/d/1kOJaZubnS5pS8xcjRqp6QYoYkYT0e09v2i0AOu9HmpI/edit?tab=t.0)**

---

## 1. Description du Flux
* **Nom Airflow** : `afklm_01_ingestion_data_quality`
* **Responsabilité** : Extraction incrémentale de l'API Open Data Air France-KLM (Flight Status), normalisation à la volée via le framework **DLT (Data Load Tool)**, dispatch relationnel vers la couche RAW (Supabase Postgres) et validation des contraintes de qualité.
* **Tables impactées (Schéma public)** : `operational_flights`, `operational_flight_legs`, `operational_flight_delays`.

## 2. Procédure d'Astreinte (Run Failed)
En cas d'alerte ou de pastille rouge sur l'interface Airflow, suivre les étapes de diagnostic dans l'ordre :

### Étape 1 : Isolation de la tâche en erreur
* **Échec sur `afklm_el_dlt_pipeline`** : Problème lié à l'extraction ou à l'écriture en base. Vérifier les jetons API et la chaîne de connexion PostgreSQL.
* **Échec sur `afklm_dq_verify_ingestion`** : Données extraites non conformes aux règles de gestion (ex. clés primaires manquantes, volumes aberrants).

### Étape 2 : Analyse des causes racines fréquents
1. **Erreur HTTP `403 Forbidden` / `429 Too Many Requests`** : Quota d'appels API consommé. Bien que le script intègre un système de *failover* automatique entre deux clés, un blocage complet impose de laisser le flux en pause. Le mécanisme de *checkpoint* de DLT reprendra sans perte de données.
2. **Erreur `ConfigFieldMissingException`** : La variable universelle d'accès à la base de données cible (`DESTINATION__POSTGRES__CREDENTIALS`) est absente ou désalignée dans le fichier `.env` de l'infrastructure Docker.

### Étape 3 : Stratégie de Reprise (Idempotence)
Le pipeline est conçu pour être **strictement idempotent**. 
* **Relance classique** : Effectuer un simple **Clear** sur la tâche ou le run en échec. DLT écrasera les données partielles de la journée en effectuant un `UPSERT` (Merge) basé sur les clés primaires uniques, évitant toute duplication.

## 3. Reprise d'Historique (Backfill)
Pour forcer le rejeu d'une journée spécifique du passé et contourner le curseur incrémental DLT :
1. Cliquer sur **Trigger DAG w/ config** dans Airflow.
2. Injecter la configuration JSON suivante en adaptant les dates cibles :
```json
{
  "incremental": false,
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD"
}