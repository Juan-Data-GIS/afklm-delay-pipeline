# afklm-delay-pipeline

Pipeline complet pour l'analyse des retards AF/KLM.  
**Warehouse :** Supabase (PostgreSQL) | **BI :** Metabase | **Ingestion :** dlt / Airbyte

---

## Architecture globale

```
[Source externe]
      │
      ▼
[dlt / Airbyte]          ← Extract & Load (EL)
      │
      ▼
Supabase (schema: public)
      │
      ▼
[dbt]                    ← Transform (raw → int → mart)
      │
      ▼
mart.fct_delays
      │
      ▼
[ml_score.py]            ← Step ML Python (scikit-learn, etc.)
      │
      ▼
ml_delays_scored (Supabase)
      │              │
      ▼              ▼
[pg_export.py]   [Front interne]
(système externe) (lecture à la demande)
```

---

## Séquence d'exécution orchestrée

| Étape | Outil | Action |
|-------|-------|--------|
| 1 | dlt / Airbyte | Charge les données brutes dans Supabase |
| 2 | `dbt run` | Produit `mart.fct_delays` (raw → int → mart) |
| 3 | `python ml_score.py` | Lit `mart.fct_delays`, écrit `ml_delays_scored` |
| 4 | `python pg_export.py` | Lit `ml_delays_scored`, pousse vers le système externe |
| 5 | Front interne | Disponible en continu, lit `ml_delays_scored` à la demande |

> Les étapes 1 à 4 sont des **batchs séquentiels** (orchestrateur ou cron).  
> L'étape 5 est **permanente** : le front lit directement Supabase via PostgREST ou une API dédiée.

---

## TL;DR — Couches dbt

| Couche dbt | Dossier | Schéma source | Schéma cible |
|------------|---------|---------------|--------------|
| **1_raw** | `1_raw/<source>` | Externe / Supabase | `raw` |
| **2_int** | `2_int/<source>` | `raw` | `int` |
| **3_mart** | `3_mart/<domaine>` | `int` | `mart` |

---

## Structure des dossiers

```
models/
├── 1_raw/                    # Ingestion et déclaration des sources
│   └── <source_system>/
│       ├── sources.yml
│       ├── <source>__source_<model>.sql
│       └── <source>__raw_ingest_<model>.sql   # optionnel
├── 2_int/                    # Couche intermédiaire (transformations lourdes)
│   ├── <source_system>/
│   │   ├── <source>__int_<model>_<grain>.sql
│   │   └── properties.yml
│   └── common/
│       ├── int_<model>_<grain>.sql
│       └── properties.yml
└── 3_mart/                   # Faits et dimensions (consommation Metabase)
    ├── common/
    │   └── dim_date.sql
    └── <domaine>/
        ├── dim_<model>.sql
        ├── fct_<model>_<grain>.sql
        ├── properties.yml
        └── exposures.yml
```

---

## 0. Ingestion — dlt / Airbyte (Extract & Load)

### Rôle

Avant que dbt ne transforme quoi que ce soit, les données doivent être **chargées dans Supabase** depuis les sources externes (APIs, bases opérationnelles, fichiers, SaaS). Ce rôle est assuré par **dlt** (léger, code-first, Python) ou **Airbyte** (UI, connecteurs prêts à l'emploi).

### Pourquoi c'est indispensable

Utiliser un outil d'ingestion dédié n'est pas optionnel : c'est une **sécurité structurelle** pour tout le reste de la pipeline.

- **Contrat de schéma garanti** : dlt/Airbyte s'assurent que les données arrivent toujours avec la même structure dans Supabase. La couche `1_raw` de dbt peut donc s'appuyer dessus sans défensive supplémentaire.
- **Gestion des aléas de connexion** : coupures réseau, timeouts, changements d'endpoint, rotation de clé API — ces problèmes sont absorbés par l'outil d'ingestion, pas par dbt.
- **Chargement incrémental natif** : gestion des watermarks, déduplication, modes `append` / `merge` / `replace` sans code custom.
- **Évolution de schéma** : si la source ajoute ou renomme une colonne, dlt/Airbyte le détectent et adaptent la table cible sans casser la pipeline.
- **Suppression de l'anti-pattern `raw_ingest`** : sans outil dédié, on est tenté de faire de l'ingestion dans dbt (`raw_ingest_*.sql`), ce qui mélange les responsabilités. Avec dlt/Airbyte, les données sont déjà dans Supabase avant que dbt ne démarre — chaque outil fait son métier.

### Comparatif

| | dlt | Airbyte |
|---|---|---|
| Setup | `pip install dlt` | Docker / Airbyte Cloud |
| Approche | Code Python | UI + connecteurs |
| Idéal pour | Projet solo / sources custom | Équipes / sources multiples standard |
| Destination Supabase | `destination="postgres"` | Connecteur natif |

### Ce que dbt reçoit

Après l'étape EL, les données brutes sont disponibles dans le schéma `public` de Supabase (ou un schéma dédié). Elles sont déclarées dans `sources.yml` avec des checks de freshness — **aucun modèle `raw_ingest` n'est nécessaire**.

---

## 1. Couche Raw (1_raw)

### Rôle

Point d'entrée unique des données dans le warehouse Supabase. Les dossiers correspondent aux systèmes sources (SaaS, bases, APIs, fichiers).

### sources.yml

- **Objectif :** Déclarer les tables/vues amont gérées par d'autres équipes ou processus externes.
- **Description de la source :** Système d'origine, équipe responsable.
- **Description des tables :** Rôle de la table, grain.
- **Freshness :** Définir `warn_after` et `error_after` pour la fraîcheur des données.

Exemple de structure :

```yaml
sources:
  - name: <source_system>
    database: "{{ env_var('AFKLM_DB_NAME') }}"
    schema: public
    loaded_at_field: updated_at
    description: 'Description du système source et équipe propriétaire'
    tables:
      - name: <table_name>
        description: 'Description de la table et son grain'
        freshness:
          warn_after: {count: 7, period: day}
          error_after: {count: 14, period: day}
```

**Convention de nommage** (si le schéma est contrôlé par l'équipe BI) :  
`raw.<source_system>__raw_<model_name>`

### Modèles source

**Objectif :** Après que les tables/vues ont été déclarées dans `sources.yml`, on introduit une couche « source model » fine au-dessus de **toutes** les tables/vues. Cette couche crée une copie des données (quasi non transformées) en un lieu centralisé dans le projet Supabase, facilitant ainsi le travail des utilisateurs finaux.

**Convention de nommage :** `<folder_name>__source_<model_name>.sql`  
*(ex. `flight_data__source_flights.sql`)*

**Transformations autorisées :**

| ✅ Autorisé | ❌ Interdit |
|-------------|------------|
| Renommage de colonnes | Joins — les modèles source doivent être en correspondance 1:1 avec les tables des systèmes sources |
| Cast de types | Agrégations — conserver le grain original |
| Calculs simples (ex. centimes → euros, unix → timestamp) | |
| Catégorisation (CASE, booléens) | |

**Matérialisation :** `view` par défaut. Table incrémentale si la table upstream est lente (volume élevé ou vue coûteuse). Les modèles source apparaissent en **vert** dans le graphe de lineage dbt.

---

### Anti-pattern : Raw Ingest

**Objectif :** En l'absence d'outil d'orchestration dédié (ex. Dagster, Airflow), il peut être tentant d'utiliser dbt pour définir une vue qui exécute une requête externe vers une base hors Supabase (ex. PostgreSQL externe). L'inconvénient de cette approche est que les modèles `raw_ingest` ne peuvent pas être déclarés dans `sources.yml` — ils disposeront néanmoins d'un modèle source au-dessus d'eux.

**Convention de nommage :** `<folder_name>__raw_ingest_<model_name>.sql`  
*(ex. `flight_data__raw_ingest_flights.sql`)*

**Transformations autorisées :**

| ✅ Autorisé | ❌ Interdit |
|-------------|------------|
| Cast vers des types acceptés par PostgreSQL | Toute autre transformation — la donnée doit être aussi brute que possible |

**Matérialisation :** `view` par défaut. Les modèles `raw_ingest` apparaissent en **vert** dans le graphe de lineage dbt.

---

## 2. Couche Intermediate (2_int)

### Rôle

Couche optionnelle où se font les **transformations lourdes** des modèles source en blocs réutilisables. Ces blocs alimentent les faits et dimensions de la couche mart.

- **Jamais exposée** directement aux utilisateurs finaux ni via des dashboards.
- Les dossiers correspondent majoritairement aux systèmes sources ou aux données amont gérées par d'autres équipes (SaaS, bases, APIs, fichiers). Un dossier `common` peut exister pour les modèles susceptibles d'être partagés entre plusieurs cas d'usage.

### Modèles int

**Objectif :** Empiler des couches de logique lourde avec des objectifs précis et testables — en joignant les modèles source pour former les entités que l'on souhaite. Pensez aux modèles int comme des **CTEs qui seraient elles-mêmes testables et réutilisables** par plusieurs autres modèles.

**Convention de nommage :** `<source_system>__int_<model>_<grain>.sql`  
*(ex. `flight_data__int_flights_daily.sql`)*  
Les objets communs n'ont pas besoin du préfixe source : `int_<model>_<grain>.sql`  
*(ex. `int_delay_measures_daily.sql`)*

**Transformations autorisées :**

| ✅ Autorisé | ❌ Interdit |
|-------------|------------|
| Joins | Référencer directement `sources.yml` — utiliser uniquement des modèles source, d'autres modèles int, ou parfois des faits/dimensions |
| Filtrage / suppression de lignes | |
| Agrégations (préciser le grain dans le nom du modèle) | |
| Nouvelles colonnes calculées | |
| Catégorisation (CASE, booléens) — uniquement si non réalisable en couche source | |
| Dépliage de données semi-structurées (JSON) | |

**Matérialisation :** `view` par défaut. Incrémentale si les vues sont trop lentes.

### properties.yml

- **Description :** Objectif du modèle et grain.
- **Tests recommandés :**
  - `unique` ou `dbt_utils.unique_combination_of_columns`
  - `not_null`
  - `dbt_expectations.expect_column_distinct_count_to_equal`
  - `dbt_expectations.expect_column_sum_to_be_between`
  - `dbt_expectations.expect_column_distinct_count_to_equal`

---

## 3. Couche Mart (3_mart)

### Rôle

Création des modèles **Faits** et **Dimensions** prêts à être consommés directement par les utilisateurs finaux ou via Metabase. Les dossiers peuvent être orientés source ou domaine métier. Un dossier `common` regroupe les modèles partagés entre plusieurs domaines.

### Modèles Fact

**Objectif :** Données quantitatives et mesurables, représentant souvent des événements métier ou des transactions.

**Convention de nommage :** `fct__<model_name>_<grain>.sql`  
*(ex. `fct__delays_daily.sql`)*

**Transformations autorisées :**

| ✅ Autorisé | ❌ Interdit |
|-------------|------------|
| Joins | Définir le même concept différemment pour différentes équipes ou systèmes |
| Nouvelles colonnes calculées | Référencer directement `sources.yml` |

**Matérialisation :** `table` par défaut. Les modèles Fact apparaissent en **jaune** dans le graphe de lineage dbt.

### Modèles Dimension

**Objectif :** Attributs descriptifs qui fournissent du contexte et des axes de catégorisation aux faits — temps, géographie, produits, clients, etc.

**Convention de nommage :** `dim__<model_name>.sql`  
*(ex. `dim__airports.sql`)*

**Transformations autorisées :**

| ✅ Autorisé | ❌ Interdit |
|-------------|------------|
| Joins | Créer une table `dim_user` différente pour chaque plateforme |
| Nouvelles colonnes calculées | Référencer directement `sources.yml` |

**Matérialisation :** `table` par défaut. Les modèles Dimension apparaissent en **jaune** dans le graphe de lineage dbt.

### Règle de conception — limiter la complexité

Les faits et dimensions peuvent s'appuyer sur des modèles int ou directement sur des modèles source. Sauter la couche int ne doit être envisagé que si la logique du fait ou de la dimension est relativement simple et courte.

**Règle d'or : ne jamais assembler plus de 5 ou 6 concepts dans un seul Fait ou Dimension.** Deux modèles int qui regroupent chacun 3 concepts, puis un Fait/Dimension qui les combine, produiront une chaîne logique bien plus lisible.

*Exemple concret :*

**Option A — à éviter**
```
fct_happy_meals_cooked_daily = (Tomate + salade + fromage + pain) + (viande × grill) + ((lait + glace) × mixeur) + (frites + huile)
```

**Option B — à privilégier**
```
fct_happy_meals_cooked_daily = Hamburgers + Milkshakes + Frites

int_hamburgers_daily  = (Tomate + salade + fromage + pain) + (viande × grill)
int_milkshakes_daily  = (Lait + glace) × mixeur
int_frites_daily      = Frites + huile
```

En plus d'une logique de Fait allégée, chaque modèle de l'Option B est testable et débogable indépendamment. Les blocs int (ex. `int_milkshakes_daily`) deviennent également des briques réutilisables : `fct_drinks_daily = (milkshakes + softdrinks)`.

### properties.yml

- **Description :** Objectif du modèle et grain.
- **Tests recommandés :**
  - `unique` ou `dbt_utils.unique_combination_of_columns`
  - `not_null`
  - `dbt_expectations.expect_column_distinct_count_to_equal`
  - `dbt_expectations.expect_column_sum_to_be_between`
  - `dbt_expectations.expect_column_distinct_count_to_equal`

### exposures.yml

**Objectif :** Documenter les objets **en aval** des modèles dbt : dashboards Metabase, synchronisations rETL, applications, pipelines data science. Cette documentation facilite grandement l'analyse de lineage et d'impact.

Les exposures apparaissent également dans le **site de documentation dbt**. Les exposures vers les dashboards Metabase peuvent être générées automatiquement via l'intégration dbt → Metabase.

---

## 4. Machine Learning — ml_score.py

### Rôle

Step Python autonome qui s'exécute **après `dbt run`**. Il enrichit les données de la couche mart avec des scores ou prédictions produits par un modèle ML (scikit-learn ou équivalent).

### Quand faire le ML dans dbt vs hors dbt

| Cas | Recommandation |
|-----|----------------|
| Règles métier simples (seuils, CASE, ratios) | Dans dbt (`2_int` ou `3_mart`) |
| Modèle ML léger (régression linéaire, scoring simple) | Dans dbt via `dbt-python` (si le warehouse le supporte) |
| Modèle ML lourd (random forest, XGBoost, réseau de neurones) | **Hors dbt**, step Python dédié |

### Séquence du step ML

```
1. Lire mart.fct_delays depuis Supabase
2. Appliquer le modèle Python (scikit-learn, joblib, etc.)
3. Écrire le résultat enrichi dans ml_delays_scored (Supabase)
```

### Table de sortie : ml_delays_scored

Contient toutes les colonnes de `mart.fct_delays` enrichies de colonnes de prédiction (ex. `delay_score`, `delay_predicted_min`, `risk_category`). Cette table est la source pour `pg_export.py` et le front interne.

> **Lineage dbt :** déclarer `ml_delays_scored` dans un `sources.yml` dédié permet de conserver la lineage complète et d'activer les freshness checks.

---

## 5. Export & Consommation aval

### pg_export.py

Script Python batch qui lit `ml_delays_scored` et pousse les données vers un système externe (partenaire, API, autre base). S'exécute après le step ML dans la séquence orchestrée.

### Front interne

Application web interne qui lit `ml_delays_scored` directement depuis Supabase (via PostgREST ou une API dédiée). Contrairement aux étapes 1–4, le front est **permanent et synchrone** : il affiche toujours l'état courant de la table au moment de la requête.

---

## Schémas Supabase

| Schéma | Contenu |
|--------|---------|
| `public` | Tables brutes chargées par dlt / Airbyte |
| `raw` | Modèles source dbt (vues fine au-dessus de `public`) |
| `int` | Modèles intermédiaires dbt |
| `mart` | Faits et dimensions dbt (consommation Metabase) |
| *(hors dbt)* | `ml_delays_scored` — table écrite par `ml_score.py` |

---

## Adaptations BigQuery → Supabase

| Aspect | BigQuery | Supabase (PostgreSQL) |
|--------|----------|------------------------|
| Hiérarchie | `project.dataset.table` | `schema.table` |
| Datasets | `bi_tracking_raw`, etc. | Schémas `raw`, `int`, `mart` |
| Types | `TIMESTAMP`, `INT64` | `TIMESTAMPTZ`, `BIGINT` |
| Fonctions | `SAFE_CAST`, `PARSE_DATE` | `CAST`, `TO_DATE` |

---

## Intégration Metabase

- **Connexion :** Metabase se connecte à Supabase.
- **Schémas exposés :** Principalement `mart`.
- **Exposures :** Documenter les dashboards dans `exposures.yml` pour la lineage dbt.
- **Permissions :** Restreindre l'accès Metabase au schéma `mart`.

---

## Commandes

```bash
# Ingestion (EL)
dlt run pipeline.py          # ou via Airbyte UI / scheduler

# Transformation dbt
dbt run
dbt test
dbt source freshness
dbt docs generate
dbt docs serve

# ML & export (à lancer après dbt run)
python ml_score.py
python pg_export.py
```

---

## Ressources

- [Documentation dbt](https://docs.getdbt.com/docs/introduction)
- [Discourse dbt](https://discourse.getdbt.com/)
- [Communauté dbt](https://community.getdbt.com/)
