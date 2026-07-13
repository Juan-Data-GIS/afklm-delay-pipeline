# RUNBOOK DE PRODUCTION - DAG : OPENMETEO_01_INGESTION_WEATHER

### Liens de Référence Centraux
* **[Consulter la Documentation API Officielle Open-Meteo](https://open-meteo.com/en/docs/historical-weather-api)**

---

## 1. Description du Flux
* **Nom Airflow** : `openmeteo_01_ingestion_weather`
* **Responsabilité** : Extraction quotidienne des métriques climatiques horaires (température, précipitations, vent) pour les hubs du réseau Air France-KLM. Les coordonnées GPS sont lues depuis la table de référence. Le chargement est opéré par **DLT** vers la table brute de la couche Bronze.
* **Tables impactées (Schéma public)** : `referentiel_airports` (lecture), `b_openmeteo_weather` (écriture).

## 2. Procédure d'Astreinte (Run Failed)
En cas d'échec du pipeline, suivre les étapes de diagnostic suivantes :

### Étape 1 : Isolation de la tâche en erreur
* **Échec sur `openmeteo_el_weather_pipeline`** : Erreur lors de la phase d'appel à l'API externe ou lors de l'écriture des paquets dans Supabase.

### Étape 2 : Analyse des causes racines fréquentes
1. **Erreur `psycopg2.OperationalError` ou Connexion Timeout** : Supabase est inaccessible ou la variable d'environnement `DESTINATION__POSTGRES__CREDENTIALS` a expiré ou est mal configurée.
2. **Erreur HTTP `429 Too Many Requests`** : L'adresse IP de la machine de production a dépassé le seuil de tolérance de l'API gratuite Open-Meteo.
3. **Aucun aéroport trouvé** : La table `public.referentiel_airports` est vide. Le script s'arrête préventivement. Vérifier que le script d'initialisation SQL a bien été exécuté.

### Étape 3 : Stratégie de Reprise (Idempotence)
Le pipeline utilise une disposition d'écriture en mode `merge` (Upsert) basée sur la clé primaire composite `(airport_code, weather_timestamp)`.
* **Action** : Effectuer un **Clear** sur la tâche en échec. Le flux écrasera les données horaires partielles ou existantes de la journée sans aucun risque de doublon.