# Airflow — Orchestration du pipeline AFKLM Delay Prediction

**Document de référence pour la soutenance.**
Explique en détail les deux DAGs qui orchestrent le pipeline end-to-end : ingestion API → transformation SQL → scoring ML → reload du service d'inférence, avec l'observabilité transversale.

---

## Sommaire

1. [Vue d'ensemble](#1-vue-densemble)
2. [DAG 01 — `afklm_01_ingestion_data_quality`](#2-dag-01--afklm_01_ingestion_data_quality)
3. [DAG 02 — `afklm_02_transformation_scoring`](#3-dag-02--afklm_02_transformation_scoring)
4. [Observabilité transversale — `monitoring_utils.py`](#4-observabilité-transversale--monitoring_utilspy)
5. [Patterns architecturaux clés](#5-patterns-architecturaux-clés)
6. [Métriques attendues et SLA](#6-métriques-attendues-et-sla)
7. [Points de démo pour la soutenance](#7-points-de-démo-pour-la-soutenance)
8. [Questions/réponses probables du jury](#8-questionsréponses-probables-du-jury)
9. [Fiche synthèse à mémoriser](#9-fiche-synthèse-à-mémoriser)

---

## 1. Vue d'ensemble

Le pipeline se décompose en **deux DAGs Airflow enchaînés automatiquement**, qui matérialisent un workflow ELT + ML complet, quotidien et idempotent.

```
┌────────────────────────────┐         ┌────────────────────────────────┐
│ DAG 01                     │  auto   │ DAG 02                         │
│ INGESTION & DATA QUALITY   │────────▶│ TRANSFORMATION & SCORING       │
│ Owner: data_engineers      │ trigger │ Owner: analytics_engineers     │
│                            │         │                                │
│ • API AF/KLM → Postgres    │         │ • dbt run + dbt test           │
│   (via dlt, EL idempotent) │         │ • ML scoring XGBoost           │
│ • Data quality checks      │         │ • Reload FastAPI (inférence)   │
└────────────────────────────┘         └────────────────────────────────┘
        │                                          │
        └──────────────┬───────────────────────────┘
                       ▼
        monitoring_utils.log_event()
        ├─ logs.airflow_events    (events atomiques)
        └─ logs.pipeline_runs     (agrégat par run, upsert)
                       ▼
                 Grafana dashboards
```

### Pourquoi deux DAGs et non un seul ?

**Séparation des responsabilités** :
- DAG 01 → équipe **Data Engineering** (extraction, ingestion, qualité brute).
- DAG 02 → équipe **Analytics Engineering** (modélisation SQL, ML).

**Bénéfices concrets** :
- **Rejeu indépendant** : après un fix dbt, on rejoue le DAG 02 sans re-fetcher l'API AFKLM (économie de quota et de temps).
- **Découplage temporel** : un backfill du DAG 02 sur 30 jours ne nécessite pas de re-ingérer.
- **Alertes séparées** : chaque équipe reçoit ses propres notifications d'échec.
- **Scheduling différencié** possible plus tard (ex: ingestion 4h du matin, transformation toutes les 6h).

---

## 2. DAG 01 — `afklm_01_ingestion_data_quality`

### 2.1 Objectif métier (1 phrase)

> Chaque jour, extraire les vols de la veille depuis l'API Open Data Air France-KLM, les charger de manière idempotente dans Postgres (Supabase), puis valider que l'ingestion s'est bien passée.

### 2.2 Caractéristiques techniques

| Attribut | Valeur | Justification |
|---|---|---|
| **DAG ID** | `afklm_01_ingestion_data_quality` | Numéroté pour l'ordonnancement inter-DAGs |
| **Schedule** | `None` (manuel) | Cron `42 4 * * *` **désactivé volontairement** en phase de dev — trigger explicite |
| **Owner** | `afklm_data_engineers` | Routage des alertes par équipe |
| **Retries** | 1 avec backoff 5min | Tolérance aux erreurs transitoires réseau/API |
| **catchup** | `False` | Pas de rattrapage automatique — les backfills sont **explicites et audités** |
| **Tags** | `afklm`, `ingestion`, `dlt` | Filtrage rapide dans l'UI Airflow |

### 2.3 Paramètres runtime

Le DAG accepte 3 paramètres configurables au trigger (via UI ou API) :

| Param | Type | Défaut | Effet |
|---|---|---|---|
| `start_date` | ISO date | `""` (vide) | Si vide → **J-1 automatique**. Sinon → date métier explicite (backfill) |
| `end_date` | ISO date | `""` (vide) | Fin de fenêtre pour backfill multi-jours |
| `env_target` | enum | `"local"` | Aiguillage réseau : `local` (Postgres Docker interne), `dev` / `prod` (Supabase Cloud) |

### 2.4 Graphe des tâches

```
log_start_pipeline
       │
       ▼
afklm_el_dlt_pipeline          [venv: pipeline_venv]
       │
       ▼
afklm_dq_verify_ingestion      [venv: pipeline_venv]
       │
       ▼
trigger_transformation_scoring [TriggerDagRunOperator → DAG 02]
```

> Note technique : la tâche `log_success_pipeline` est définie dans le code mais **orpheline** (non chaînée dans les `>>`). Amélioration à apporter : l'insérer entre `verify_ingestion_quality` et `trigger_next_dag` pour tracer le succès complet en base.

### 2.5 Détail de chaque tâche

#### Task 1 — `log_start_pipeline` (PythonOperator)

**Responsabilité** : Marquer le démarrage du run dans le monitoring centralisé.

**Ce qu'elle fait** :
1. Lit le contexte Airflow via `get_current_context()`.
2. Détermine la **date métier** (`business_date`) :
   - Si `start_date` fourni → date métier = param.
   - Sinon → date métier = `logical_date - 1 jour` (règle J-1).
3. Appelle `log_event(...)` qui écrit dans les deux tables Postgres :
   - `INSERT` dans `logs.airflow_events` (event `dag_started`, layer `ORCHESTRATION`).
   - `UPSERT` dans `logs.pipeline_runs` (status `RUNNING`, `started_at = NOW()`).

**Pourquoi c'est important** : chaque run laisse une empreinte en base **avant même** que l'ingestion démarre. Si la task 2 crash sans logger, on garde une trace du démarrage → l'observabilité résiste aux pannes.

#### Task 2 — `afklm_el_dlt_pipeline` (ExternalPythonOperator)

**Responsabilité** : Extract + Load depuis l'API AFKLM vers Postgres, via **dlt** (Data Load Tool).

**Environnement d'exécution** : venv isolé `/home/airflow/pipeline_venv/bin/python`, préinstallé au build de l'image Airflow. Contient `dlt`, `requests`, `psycopg2`, `sqlalchemy`, **séparés** du core Airflow (Pydantic v2, etc.).

**Séquence interne détaillée** :

1. **Fenêtrage temporel** : si `start_date` vide, calcule `[J-1T00:00Z, J-1T23:59Z]` UTC. Sinon aligne strictement sur la date fournie.

2. **Injection des variables d'environnement** pour dlt :
   ```python
   os.environ["SOURCES__AFKLM__START_DATE"] = day_start
   os.environ["SOURCES__AFKLM__END_DATE"]   = day_end
   os.environ["SOURCES__AFKLM__INCREMENTAL"] = "False"
   os.environ["ENV_TARGET"] = env_target
   ```
   Convention dlt : `SOURCES__<name>__<param>` surcharge la config.

3. **Appel de `afklm_dlt_pipeline.main()`** qui exécute :

   a. **Aiguillage réseau** (`configure_dlt_destination`) : construit dynamiquement la chaîne de connexion selon `ENV_TARGET` :
   - `local` → `postgresql://data_engineer:...@postgres_local:5432/data_hub` (**réseau Docker interne**, pas de SSL).
   - `dev` / `prod` → Supabase Cloud (`AFKLM_DB_HOST`, `sslmode=require`).
   - Injecté dans `DESTINATION__POSTGRES__CREDENTIALS` (convention dlt).

   b. **Instanciation** de la source dlt `afklm_source(...)`.

   c. **Exécution** de `pipeline.run(source_instance)` qui lance les 3 phases de dlt :
   - **Extract** : appels HTTP paginés à `https://api.airfranceklm.com/opendata/flightstatus`.
   - **Normalize** : transformation JSON hiérarchique → schéma tabulaire relationnel.
   - **Load** : écriture Postgres via `COPY` + `MERGE` (UPSERT).

4. **Patterns clés dans l'extraction** (`afklm_source.py`) :

   - **Pagination** : `pageSize=90`, itération jusqu'à `totalPages` retourné par l'API.
   - **Découpage jour par jour** : chaque journée est extraite indépendamment (une journée en échec ne casse pas le backfill entier).
   - **Sleep entre requêtes** : `1.5s` (respect des quotas AFKLM).
   - **Failover multi-clés API** : pool de 5 `AF_CLIENT_ID_1..5`. Sur `401` / `403` (quota épuisé) → **bascule définitive** vers la clé suivante pour le reste du run. Sur `5xx` → retry × 5 avec pause 5s.
   - **Résilience granulaire** : si une page échoue après tous les retries, elle est loguée `PAGE SKIPPED` et le run continue (perte partielle documentée, pas d'arrêt total).

5. **Normalisation** : chaque vol API est éclaté en **3 tables relationnelles** :

   | Table | Grain | Clé primaire |
   |---|---|---|
   | `operational_flights` | 1 ligne / vol | `id` (fourni par l'API) |
   | `operational_flight_legs` | 1 ligne / tronçon physique | `leg_id` = `uuid5(NAMESPACE, "flight_id_i")` **UUID stable** |
   | `operational_flight_delays` | 1 ligne / cause de retard | `id` = `uuid5(NAMESPACE, "flight_id_i_j")` |

   L'utilisation de **UUID v5 déterministes** garantit que le même leg (même flight_id, même index) génère toujours le même `leg_id`, condition nécessaire à l'idempotence de dlt et du ML aval.

6. **Idempotence dlt** : chaque `@dlt.resource` est configurée en `write_disposition="merge"` avec `primary_key`. Un rejeu de la même journée **UPSERT** au lieu de dupliquer.

7. **Checkpoint incrémental** : `dlt.current.source_state()["last_window_end"] = end.isoformat()`. En mode `incremental=True`, un run suivant reprend depuis ce curseur.

8. **XCom** : push des métriques (`records_processed`, `legs_processed`, `delays_processed`, `extraction_status`) pour les tasks suivantes et l'observabilité.

9. **Retour** : `{"vols_ingested": total_vols}` (récupéré en XCom par la task de log).

**Ce qui rend cette task robuste** :

| Risque | Mécanisme de mitigation |
|---|---|
| Quota API épuisé | Failover sur 5 clés |
| Erreur serveur AFKLM (5xx) | 5 retries avec backoff |
| Une page corrompue | Skip individuel, run continue |
| Rejeu accidentel | UPSERT dlt → zéro doublon |
| Crash à mi-parcours | XCom des métriques poussé **avant** de lever l'erreur |

#### Task 3 — `afklm_dq_verify_ingestion` (ExternalPythonOperator)

**Responsabilité** : Contrôle qualité minimal post-ingestion.

**Ce qu'elle fait** (`verify_ingestion.py`) :
1. Charge `.env` via `python-dotenv` (compat exécution hors Docker).
2. Détecte le contexte : `AIRFLOW_CTX_DAG_ID` présent → Airflow container ; sinon → exécution manuelle locale.
3. Détermine host/port/user/pass selon `ENV_TARGET`.
4. **Test de connexion + comptage** :
   ```sql
   SELECT COUNT(*) FROM public.operational_flights;
   ```
5. Log le résultat. Si erreur → `sys.exit(1)` (fait échouer la task, alerte Grafana).

**Pourquoi** : garde-fou minimal. Si la table n'existe pas ou est vide juste après l'ingestion, on veut le savoir immédiatement, pas en aval quand dbt échouera avec des erreurs obscures.

**Extensions futures possibles** : vérifier PK nulles, volumes anormaux (> 3× moyenne mobile 7j), timestamps futurs, doublons logiques.

#### Task 4 — `trigger_transformation_scoring` (TriggerDagRunOperator)

**Responsabilité** : Déclencher automatiquement le DAG 02.

**Configuration transmise** :
```python
conf={
    "env_target": "{{ params.env_target }}",
    "start_date": "{{ params.start_date
                       if params.start_date
                       else macros.ds_add(data_interval_start.strftime('%Y-%m-%d'), -1) }}"
}
```

**Comportement** : `wait_for_completion=False` — le DAG 01 se termine dès le trigger envoyé, **sans bloquer** sur l'exécution du DAG 02. Les deux graphes restent visuellement distincts dans l'UI.

**Pourquoi ce pattern** : découplage temporel classique en event-driven. Le DAG 01 signale « le RAW est prêt », le DAG 02 réagit indépendamment.

---

## 3. DAG 02 — `afklm_02_transformation_scoring`

### 3.1 Objectif métier (1 phrase)

> Transformer les données brutes en modèles analytiques (raw → int → mart) via dbt, calculer les prédictions de retards via un modèle XGBoost, et rafraîchir le service d'inférence FastAPI pour que Streamlit expose les nouveaux scores.

### 3.2 Caractéristiques techniques

| Attribut | Valeur |
|---|---|
| **DAG ID** | `afklm_02_transformation_scoring` |
| **Schedule** | `None` (déclenché par DAG 01 ou manuel) |
| **Owner** | `afklm_analytics_engineers` |
| **Retries** | 1 avec backoff 5min |
| **Tags** | `afklm`, `transformation`, `dbt`, `ml` |

### 3.3 Configuration reçue

Passée par le DAG 01 via `dag_run.conf` :
- `env_target` (aiguillage réseau)
- `start_date` (date métier à conserver pour la traçabilité end-to-end)

### 3.4 Graphe des tâches

```
log_start_transformation
       │
       ▼
afklm_t_dbt_run               [venv: dbt_venv]
       │
       ▼
afklm_t_dbt_test              [venv: dbt_venv]
       │
       ▼
afklm_ml_compute_predictions  [venv: system Airflow]
       │
       ▼
afklm_ml_trigger_fastapi      [venv: pipeline_venv]
       │
       ▼
log_success_transformation
```

### 3.5 Détail de chaque tâche

#### Task 1 — `log_start_transformation` (PythonOperator)

Symétrique du DAG 01 : log `dag_started` sur `logs.airflow_events` + upsert sur `logs.pipeline_runs`. Récupère la `business_date` depuis `dag_run.conf["start_date"]` (transmise par le DAG 01), ce qui permet de **corréler les deux runs** dans les dashboards.

#### Task 2 — `afklm_t_dbt_run` (ExternalPythonOperator, venv=`dbt_venv`)

**Responsabilité** : Exécuter toutes les transformations SQL définies dans le repo dbt.

**Ce qu'elle fait** :
1. Positionne les variables d'environnement dbt :
   ```python
   DBT_PROFILES_DIR = "/opt/airflow/dbt"
   DBT_PROJECT_DIR  = "/opt/airflow/dbt"
   DBT_TARGET       = env_target
   ```
2. Invoque dbt via l'**API Python native** (`dbtRunner`), pas via subprocess :
   ```python
   dbt = dbtRunner()
   result = dbt.invoke(["run", "--profiles-dir", "/opt/airflow/dbt", ...])
   ```
3. Parse `target/run_results.json` pour extraire `rows_affected` par modèle et calculer un total.
4. Retourne `{"records_processed": total_rows, "pipeline_engine": "dbt"}` (utilisé par le log de succès).
5. Failure → `RuntimeError` (task échoue).

**Modèles exécutés — 13 modèles organisés en 3 couches** (config depuis `dbt_project.yml`) :

**Couche `1_raw`** — Vues de typage
- Schéma : `raw` (nom Postgres résolu : `<default>_raw`, ex: `public_raw`)
- Matérialisation : `view` (léger, pas de duplication de données)
- Rôle : abstraire la couche d'ingestion dlt, fournir un contrat stable pour l'aval

| Modèle | Description |
|---|---|
| `flight_data__source_operational_flights` | Sélection + typage depuis la table dlt `operational_flights` |
| `flight_data__source_operational_flight_legs` | Idem pour `operational_flight_legs` |
| `flight_data__source_operational_flight_delays` | Idem pour `operational_flight_delays` |

**Couche `2_int`** — Modèles intermédiaires (features ML pré-calculées)
- Schéma : `int`
- Matérialisation : `table` (**figée physiquement** pour éviter de recalculer les features lourdes à chaque appel du mart)

| Modèle | Description |
|---|---|
| `flight_data__int_legs_ready` | Legs prêts à l'usage : parsing durée ISO 8601, calcul `is_delayed` défensif avec `LEFT JOIN` (protège des `NULL` sur legs sans retards) |
| `flight_data__int_aircraft_delays` | Agrégat retards par aéronef (share historique) |
| `flight_data__int_airline_delays` | Agrégat retards par compagnie + index B-tree sur la clé |
| `flight_data__int_airport_delays` | Agrégat retards par aéroport |
| `flight_data__int_airport_congestion` | Trafic aéroport (nb vols départ / arrivée) |
| `flight_data__int_delays_leg` | Détail des causes par leg |

**Couche `3_mart`** — Modèles analytiques
- Schéma : `mart`
- Consommateurs : `ml_run.py`, Streamlit, Grafana

| Modèle | Matérialisation | Description |
|---|---|---|
| `dim_date` | `table` | Dimension calendrier (issue des dates de vol présentes en base) |
| `dim_airlines` | `table` | Dimension compagnie |
| `dim_airports` | `table` | Dimension aéroport |
| **`fct_flight_legs`** | **`incremental`** | **Table de faits centrale** — 1 ligne par leg avec toutes les features enrichies |

**Focus sur `fct_flight_legs`** (le modèle le plus critique) :
```sql
{{ config(
    schema='mart',
    materialized='incremental',
    unique_key='leg_id',
    pre_hook="SET statement_timeout = '600000';"
) }}
...
{% if is_incremental() %}
  where scheduled_departure >= (
    select max(scheduled_departure) - interval '3 day' from {{ this }}
  )
{% endif %}
```

Trois choses à retenir :
- **`unique_key='leg_id'`** : garantit l'idempotence (dbt fait un MERGE).
- **`pre_hook` sur `statement_timeout`** : contourne le timeout par défaut de Supabase (30s) — étend à 10 minutes.
- **Fenêtre glissante de 3 jours** dans le filtre `is_incremental()` : on retraite systématiquement les 3 derniers jours pour capter les **mises à jour de statuts** (un vol J-1 peut passer d'`ON_TIME` à `DELAYED` en fonction de l'`actual_arrival` connu après coup).

#### Task 3 — `afklm_t_dbt_test` (ExternalPythonOperator, venv=`dbt_venv`)

**Responsabilité** : Exécuter `dbt test` sur tous les tests génériques (non-null, unique, relationships) et personnalisés définis dans les `schema.yml`.

Failure → `RuntimeError` (bloque le pipeline avant que le ML consomme des données corrompues).

#### Task 4 — `afklm_ml_compute_predictions` (PythonOperator)

**Responsabilité** : Charger le modèle XGBoost, prédire les retards des nouveaux legs, persister dans `public.ml_delays`.

**Séquence interne** (`ml/ml_run.py`) :

1. **Aiguillage réseau** : construit `DB_URI` selon `ENV_TARGET` (SQLAlchemy engine).

2. **Chargement incrémental** (le pattern MLOps le plus important du pipeline) :
   ```sql
   SELECT l.* FROM public_mart.fct_flight_legs l
   LEFT JOIN public.ml_delays d ON l.leg_id = d.leg_id
   WHERE l.cancelled = false
     AND d.leg_id IS NULL
   ```
   Ne prend que les legs **non annulés** et **non encore scorés**. Complexité constante par run (proportionnelle au delta, pas au volume total).

   **Fallback** : si `ml_delays` n'existe pas (tout premier run) → chargement complet automatique. Zéro friction à l'onboarding.

3. **Chargement des 3 artefacts ML** depuis Supabase Storage (URLs signées) :

   | Artefact | Rôle |
   |---|---|
   | `MODEL_MEANS_URL` | Dictionnaire d'imputation des NaN (moyennes d'entraînement) |
   | `MODEL_SCALER_URL` | `StandardScaler` sklearn (normalisation des features) |
   | `MODEL_XGB_URL` | Modèle XGBoost sérialisé (Classifier) |

   Chargés via `urllib` + `pickle` avec un **unpickler custom** (`SafeDataOpsUnpickler`) qui gère les ruptures de compatibilité pandas v1 ↔ v2 (`StringDtype`, `NDArrayBacked`). Si le pickle est trop ancien, fallback sur un dictionnaire d'imputation par défaut.

4. **Préparation des features** — 11 variables explicatives :

   | Groupe | Variables |
   |---|---|
   | Temporel | `departure_monthday`, `departure_weekday`, `departure_hour` |
   | Durée | `scheduled_flight_duration_minutes` |
   | Congestion départ | `nb_flight_departing_departure_airport`, `nb_flight_arriving_departure_airport` |
   | Congestion arrivée | `nb_flight_departing_arrival_airport`, `nb_flight_arriving_arrival_airport` |
   | Historique retards | `departure_airport_delayed_share`, `aircraft_delayed_share`, `airline_delayed_share` |

   Cible : `is_delayed` (booléen → 0/1).

   Pipeline : imputation NaN → renommage colonnes en CamelCase legacy → `scaler.transform(X)` → matrice normalisée.

5. **Prédiction** : `y_pred = model.predict(X_scaled)`.

6. **Enrichissement** :
   ```python
   df_w_pred["delay_predicted"] = y_pred
   df_w_pred["timestamp"] = pd.Timestamp.now()
   ```

7. **Persistance idempotente** en base :
   ```sql
   CREATE TABLE IF NOT EXISTS public.ml_delays (
       leg_id          UUID PRIMARY KEY,
       flight_id       VARCHAR(50),
       delay_predicted INTEGER,
       timestamp       TIMESTAMP
   );

   INSERT INTO public.ml_delays (leg_id, flight_id, delay_predicted, timestamp)
   VALUES (...)
   ON CONFLICT (leg_id) DO UPDATE SET
       delay_predicted = EXCLUDED.delay_predicted,
       timestamp       = EXCLUDED.timestamp;
   ```

8. **Log final** : `"[ML RUN REUSSI] N nouvelles prédictions enregistrées dans public.ml_delays"`.

**Ce qui rend ce ML production-ready** :
- **Incrémentalité** : coût par run ∝ delta, pas au volume total → scale-friendly.
- **Idempotence** : rejeu safe (UPSERT + `LEFT JOIN` protecteur).
- **Auditabilité** : colonne `timestamp` = quand chaque prédiction a été calculée. Utile pour :
  - le monitoring de model drift (comparer distributions par jour).
  - remonter à la version du modèle qui a produit une prédiction donnée.
- **Résilience binaire** : l'unpickler custom encaisse les upgrades pandas sans crash.

#### Task 5 — `afklm_ml_trigger_fastapi` (ExternalPythonOperator, venv=`pipeline_venv`)

**Responsabilité** : Notifier le service d'inférence FastAPI qu'il doit recharger ses données depuis Postgres.

**Ce qu'elle fait** :
```python
response = requests.post(
    "http://afklm-formation-fastapi:8000/v1/scoring/reload",
    timeout=30
)
response.raise_for_status()
```

Utilise le **DNS Docker interne** (`afklm-formation-fastapi` = nom du service dans `docker-compose.yml`), pas `localhost` — l'appel reste dans le réseau privé Docker Compose.

**Pourquoi** : sans ce reload, FastAPI continuerait à servir des prédictions issues d'une session précédente. Le pattern "trigger reload" évite d'implémenter un polling côté FastAPI (moins de charge, pas de latence de découverte).

#### Task 6 — `log_success_transformation` (PythonOperator)

Log `dbt_run_success` (layer `TRUSTED`) avec le total `records_processed`. Marque le run comme `SUCCESS` dans `logs.pipeline_runs` avec `finished_at` et `duration_sec` calculés automatiquement par le `UPSERT`.

---

## 4. Observabilité transversale — `monitoring_utils.py`

### 4.1 Deux tables, deux niveaux de granularité

Chaque event génère **deux écritures** en base (via `_persist_event`) :

**`logs.airflow_events`** — événements atomiques

| Colonne | Description |
|---|---|
| `event_at` | Horodatage physique de l'event |
| `app` | `airflow` |
| `level` | `INFO` / `WARNING` / `ERROR` |
| `layer` | `ORCHESTRATION` / `RAW_INBOUND` / `TRUSTED` / `REFINED` |
| `dag_id`, `task_id`, `run_id` | Identifiants Airflow |
| `event_type` | `dag_started`, `ingestion_success`, `dbt_run_failure`, etc. |
| `message` | Message humain (français) |
| `extra` | JSONB avec métriques additionnelles (`vols_ingested`, `duration_sec`, `business_date`, etc.) |

**`logs.pipeline_runs`** — vue agrégée par run (upsert sur `run_id`)

| Colonne | Description |
|---|---|
| `run_id`, `dag_id`, `date_metier` | Clé fonctionnelle unique |
| `started_at`, `finished_at`, `duration_sec` | Chronologie (calcul auto sur SUCCESS/FAILED) |
| `status` | `RUNNING` / `SUCCESS` / `FAILED` |
| `vols_ingested`, `transformation_rows` | Métriques métiers (extraction intelligente selon le nom du DAG/task) |
| `error_message` | Trace si failure (facilite le triage sans ouvrir Airflow) |
| `execution_context` | JSONB des params/conf du run |

### 4.2 Routage selon l'environnement

```python
def _get_log_conn_id() -> str:
    return "postgres_local" if ENV_TARGET == "local" else "supabase_prd"
```

- **dev local** → logs dans Postgres Docker (`postgres_local`).
- **prod** → logs dans Supabase.

Les deux `postgres_conn_id` sont configurés côté Airflow (Connections UI ou variable d'env).

### 4.3 Callbacks automatiques sur échec

Chaque task métier déclare :
```python
on_failure_callback=operator_failure_callbacks(
    layer="RAW_INBOUND",
    event_type="ingestion_failure"
)
```

En cas d'échec, un event est loggué **automatiquement** avec l'exception (str), le `layer`, le `task_id`, sans intervention manuelle. Zéro chance d'oublier un log d'erreur.

### 4.4 Dashboards Grafana (consommateurs)

Les deux tables alimentent Grafana (via datasource `postgres_local` ou `supabase_prd`) pour afficher :
- Runs par jour par DAG (taux de succès/échec).
- Timeline des durées d'exécution (détection de dégradation).
- Volumétrie ingérée / transformée (série temporelle).
- Top erreurs récurrentes (comptage groupé par `error_message`).
- Distribution des `event_type` par layer.

---

## 5. Patterns architecturaux clés

### 5.1 Venvs isolés (`ExternalPythonOperator`)

Chaque task lourde tourne dans son propre venv Python **préinstallé au build** de l'image Airflow :

| Venv | Localisation | Dépendances |
|---|---|---|
| `pipeline_venv` | `/home/airflow/pipeline_venv` | `dlt`, `requests`, `psycopg2`, `sqlalchemy` |
| `dbt_venv` | `/home/airflow/dbt_venv` | `dbt-core`, `dbt-postgres` |

**Pourquoi cette isolation** :
- Airflow 3.x contraint fortement ses dépendances (Pydantic v2, FastAPI récent).
- dbt et dlt ont des versions transitives incompatibles entre elles.
- Un venv par task = zéro conflit + upgrade ciblé + reproductibilité.

### 5.2 Aiguillage réseau via `ENV_TARGET`

Toutes les tâches lisent `os.environ["ENV_TARGET"]` et construisent leur `connection_string` dynamiquement. Un même DAG peut donc pointer :
- `local` → réseau Docker interne (`postgres_local:5432`, sans SSL).
- `dev` / `prod` → Supabase Cloud (`aws-*.pooler.supabase.com`, SSL requis).

Zéro duplication de code, un seul artefact déployable, config par ENV.

### 5.3 Idempotence à tous les étages

| Composant | Mécanisme |
|---|---|
| dlt (raw) | `write_disposition="merge"` + `primary_key` sur les 3 resources |
| dbt (mart) | `materialized='incremental'` + `unique_key='leg_id'` |
| ML (`ml_delays`) | `INSERT ... ON CONFLICT (leg_id) DO UPDATE` |
| Monitoring (`pipeline_runs`) | `INSERT ... ON CONFLICT (run_id) DO UPDATE` |

**Conséquence** : un rejeu du DAG sur la même date **ne produit jamais de doublons**. Le pipeline est safe à re-clear ou re-trigger sans peur d'effets de bord.

### 5.4 Fenêtrage incrémental multi-niveaux

- **dlt** : checkpoint `last_window_end` en `source_state`.
- **dbt** : `is_incremental()` sur `fct_flight_legs` avec **fenêtre glissante 3 jours** pour capter les mises à jour de statut.
- **ML** : `LEFT JOIN` sur `ml_delays` pour ne scorer que les nouveaux legs.

Le pipeline est **conçu nativement pour la fréquence quotidienne** : chaque run traite uniquement le delta, donc **temps constant** indépendamment de l'historique.

### 5.5 Failover multi-clés API

Le pool `AF_CLIENT_ID_1..5` permet de contourner les quotas quotidiens de l'API AFKLM. En cas de `401` / `403` sur une clé, **bascule définitive** vers la suivante pour le reste du run (pas de retry sur la clé morte, économie de tentatives).

### 5.6 Corrélation cross-DAG via `date_metier`

Le DAG 01 transmet `start_date` au DAG 02 dans `dag_run.conf`. Les deux runs partagent donc la même `date_metier` dans `logs.pipeline_runs` → on peut **corréler visuellement** l'ingestion et la transformation d'une même journée métier dans Grafana.

### 5.7 XCom robuste au crash

Dans `afklm_source.py`, les métriques (`vols_count`, `legs_count`, `delays_count`) sont **poussées dans XCom AVANT** de lever l'erreur en cas d'échec partiel. Grafana affiche donc la volumétrie partielle réellement ingérée, pas 0.

---

## 6. Métriques attendues et SLA

### 6.1 Volumétrie typique par run journalier

| Métrique | Ordre de grandeur | Commentaire |
|---|---|---|
| Vols ingérés | ~1 400 / jour | Périmètre AF + KLM opérationnels |
| Legs ingérés | ~1 500 / jour | Un vol peut avoir plusieurs tronçons |
| Delays ingérés | ~800 / jour | Uniquement les vols effectivement retardés |
| Durée DAG 01 | 3-10 min | Selon pagination API et failover éventuel |
| Durée DAG 02 | 2-6 min | dbt run + ML incrémental |
| Prédictions ML | 100-1 500 / run | Selon incrémentalité (dépend de la fenêtre glissante dbt) |

### 6.2 SLA cibles (objectifs de production)

- **DAG 01** : succès quotidien avant 06:00 UTC (pour disponibilité dashboards à 08:00 heure locale).
- **DAG 02** : succès dans les 15min après le DAG 01.
- **FastAPI reload** : < 30s.
- **Disponibilité FastAPI** : > 99% (probes Docker healthcheck).

---

## 7. Points de démo pour la soutenance

### Démo 1 — Trigger manuel depuis l'UI Airflow

1. Ouvrir `http://localhost:8081` (Airflow UI).
2. Login (admin / password généré au premier init — visible dans `docker compose logs airflow-init`).
3. Toggle le DAG `afklm_01_ingestion_data_quality` sur "on".
4. Cliquer **"Trigger DAG w/ config"** → remplir `{ "env_target": "dev" }` → Trigger.
5. Montrer le graphe qui devient vert task par task en ~5 min.
6. Passer sur le DAG 02 auto-déclenché → montrer le graphe complet.

### Démo 2 — Lire les logs métier dans Grafana

1. Dans Grafana (`http://localhost:3000`), ouvrir le dashboard "Pipeline Runs".
2. Montrer la timeline des runs, les métriques ingested/transformed, les taux de succès.
3. Cliquer sur un run pour voir les events détaillés (`logs.airflow_events`).

### Démo 3 — Prédictions dans Streamlit

1. Ouvrir `http://localhost:8501`.
2. Aller sur la page "Predictions" — montrer les scores fraîchement calculés (colonne `timestamp` récente).
3. Filtrer par aéroport / compagnie pour montrer le taux de retard prédit.

### Démo 4 — Rejeu idempotent

1. Depuis Airflow UI, faire "Clear" d'une task du DAG 01.
2. Observer que la volumétrie en base **ne bouge pas** entre les deux runs (UPSERT au lieu d'INSERT).
3. Montrer le SQL `SELECT COUNT(*) FROM public.operational_flights;` identique avant/après.

### Démo 5 — Backfill explicite d'une journée passée

1. Trigger le DAG 01 avec `{ "start_date": "2026-06-15", "env_target": "dev" }`.
2. Montrer que l'API est re-fetchée pour cette date spécifique (logs verbeux `[FETCHING DAY] 2026-06-15`).
3. Après succès, montrer que `fct_flight_legs` s'est enrichi via l'incremental dbt.

---

## 8. Questions/réponses probables du jury

**Q : Pourquoi dlt et pas Airbyte / Fivetran / un script custom ?**
> dlt est un framework **Python-first** et **open-source**. Contrairement à Airbyte/Fivetran (SaaS ou self-hosted lourd), il tient dans un `pip install` et s'exécute dans un venv de 100 Mo. Il gère nativement la normalisation JSON → tables relationnelles, l'idempotence via `write_disposition="merge"`, et les checkpoints incrémentaux. Un script custom aurait demandé de réimplémenter tout ça, ce qui aurait pris des semaines.

**Q : Pourquoi Airflow et pas Prefect / Dagster / GitHub Actions ?**
> Airflow reste le standard de facto en France pour les pipelines de production (compatibilité SI existants, écosystème plugins, communauté). Airflow 3.x apporte le `dag-processor` en process séparé, l'UI FastAPI, et une meilleure isolation des dépendances via `ExternalPythonOperator`. Prefect et Dagster sont excellents mais moins matures en environnement enterprise.

**Q : Pourquoi Supabase et pas AWS RDS / GCP CloudSQL ?**
> Supabase = Postgres managé + Auth + Storage + Realtime en une API unifiée, gratuit jusqu'à 500 Mo. Pour une formation / MVP, c'est imbattable en time-to-market. En prod scale, on migrerait vers RDS/AlloyDB.

**Q : Comment gérez-vous la sécurité des credentials ?**
> Aucun secret n'est commité — le repo contient `profiles.yml.example` et `.env.example` avec des placeholders. Les vrais fichiers `profiles.yml` et `.env` sont gitignorés. Les URLs signées Supabase (`MODEL_*_URL`) expirent dans le temps et peuvent être rotées. En prod, on passerait sur AWS Secrets Manager ou Doppler.

**Q : Pourquoi UUID v5 pour `leg_id` et pas UUID v4 ?**
> UUID v5 est **déterministe** (hash SHA-1 d'un namespace + nom). Le même `(flight_id, leg_order)` génère toujours le même `leg_id`, ce qui est **essentiel pour l'idempotence** de dlt et du ML. UUID v4 serait aléatoire → chaque rejeu créerait des doublons.

**Q : Pourquoi une fenêtre glissante de 3 jours dans `fct_flight_legs` incremental ?**
> Un vol de J-1 peut voir son statut mis à jour à J ou J+1 (arrivée effective, changement de porte, annulation tardive). Sans fenêtre glissante, on figerait les données au moment du premier scoring. Trois jours = compromis entre coût de recalcul et fraîcheur des données.

**Q : Comment le ML est-il monitoré (drift, performance) ?**
> La colonne `timestamp` dans `ml_delays` permet de tracer chaque prédiction dans le temps. Pour le model drift, on peut comparer la distribution des `delay_predicted` et des features (via Grafana ou un dashboard custom). C'est actuellement passif — un DAG mensuel de détection de drift est prévu (voir améliorations).

**Q : Que se passe-t-il si le DAG 01 échoue en plein milieu ?**
> Trois choses. (1) Les données déjà ingérées restent en base (UPSERT). (2) L'XCom des métriques est poussé **avant** de lever l'erreur, donc Grafana affiche la volumétrie partielle. (3) Le checkpoint dlt (`last_window_end`) n'est PAS mis à jour tant que le run n'est pas complet — un rejeu reprendra la journée entière.

**Q : Comment scale-t-on ce pipeline à 10× la volumétrie ?**
> Trois axes : (1) **dlt parallélisation** : monter le `workers` dans le config pour paralléliser Extract/Normalize/Load. (2) **dbt threads** : passer de 4 à 8-16 threads dans profiles.yml. (3) **Airflow Celery/K8s executor** : passer de LocalExecutor à un executor distribué. Le design incrémental garantit que le temps par run reste constant.

**Q : Quelle stratégie de reprise en cas d'incident majeur ?**
> Les tables raw sont idempotentes → on peut re-jouer une plage `[start_date, end_date]` sans peur. `fct_flight_legs` est incremental mais peut être `--full-refresh` en cas de corruption suspectée. `ml_delays` est UPSERT donc auto-corrigé au prochain run.

---

## 9. Fiche synthèse à mémoriser

> Le pipeline **AFKLM Delay Prediction** est orchestré par **deux DAGs Airflow séquentiels et découplés**.
>
> Le **DAG 01** ingère quotidiennement les vols de la veille depuis l'API Open Data Air France-KLM via **dlt** (Extract-Load idempotent, failover multi-clés sur 5 API tokens, pagination robuste), les persiste dans **Supabase Postgres** en 3 tables relationnelles (`operational_flights`, `operational_flight_legs`, `operational_flight_delays` avec UUID v5 déterministes), puis vérifie la qualité minimale. Il déclenche automatiquement le **DAG 02** via `TriggerDagRunOperator`.
>
> Le **DAG 02** exécute **dbt run + dbt test** (13 modèles répartis en 3 couches raw → int → mart, avec `fct_flight_legs` en `incremental` sur fenêtre glissante 3 jours), puis lance le **scoring ML incrémental** via un modèle **XGBoost** sur 11 features (features temporelles, congestion, historique retards), avec un **unpickler custom** pour la compatibilité pandas v1↔v2. Les prédictions sont persistées dans `public.ml_delays` en UPSERT idempotent, puis l'API **FastAPI** est notifiée de recharger pour que **Streamlit** expose immédiatement les nouveaux scores.
>
> L'observabilité est centralisée sur **deux tables Postgres** (`logs.airflow_events` pour les events atomiques, `logs.pipeline_runs` pour l'agrégat par run) alimentées à chaque étape par `monitoring_utils.log_event()`, et visualisées via **Grafana**.
>
> L'ensemble tourne sur une stack **Docker Compose de 14 services** : Airflow (`apiserver` + `scheduler` + `dag-processor` + `init`), FastAPI, Streamlit, 2 Postgres (metadata Airflow + local dataops), Prometheus, Grafana, Alertmanager, cAdvisor, Node Exporter, Postgres Exporter.

---

*Document rédigé pour la soutenance — révision juillet 2026.*
