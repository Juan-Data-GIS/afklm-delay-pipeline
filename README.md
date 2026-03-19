# Pipeline de prédiction des retards AF/KLM

Pipeline de données end-to-end pour l'analyse et la prédiction des retards de vols Air France / KLM.

**Stack :** API AF/KLM → dlt → Supabase (PostgreSQL) → dbt → ML (scikit-learn / XGBoost)

---

## Contexte du projet

L'objectif est de construire un pipeline de données complet qui :
1. **Collecte** les données de vols en temps réel depuis l'API officielle AF/KLM
2. **Transforme** ces données brutes en un schéma analytique propre et documenté
3. **Prédit** les retards à l'aide d'un modèle de machine learning entraîné sur les données historiques

Le projet s'appuie sur deux dépôts Git distincts :

| Dépôt | Rôle |
|-------|------|
| `dst_airlines` | Environnement de développement et d'exploration (notebooks, prototypes, documentation de travail) |
| `afklm-delay-pipeline` *(ce repo)* | Pipeline de production validé — code stable, prêt à être présenté et déployé |

---

## Architecture globale

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE                                    │
│              API AF/KLM (developer.airfranceklm.com)            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │   dlt (Python)  │  ← Extract & Load
                    │  ingestion/     │     Appelle l'API, normalise
                    └───────┬─────────┘    et charge dans Supabase
                            │
            ┌───────────────▼───────────────┐
            │         Supabase              │
            │     (PostgreSQL cloud)        │
            │                               │
            │  schema: public               │
            │  ├── operational_flights      │  ← tables brutes créées par dlt
            │  ├── operational_flight_legs  │
            │  └── operational_flight_delays│
            └───────────────┬───────────────┘
                            │
                    ┌───────▼────────┐
                    │   dbt Core     │  ← Transform
                    │   models/      │     raw → int → mart
                    └───────┬─────────┘
                            │
            ┌───────────────▼───────────────┐
            │         Supabase              │
            │                               │
            │  schema: raw   (vues sources) │
            │  schema: int   (features ML)  │
            │  schema: mart  (fct_flight_legs, dim_*) │
            └───────────────┬───────────────┘
                            │
                    ┌───────▼────────┐
                    │  ml_score.py   │  ← Scoring ML  [en cours]
                    │  (XGBoost)     │
                    └───────┬─────────┘
                            │
                    ml_delays_scored (Supabase)
```

---

## Outils utilisés

### dlt — Data Load Tool

dlt est une bibliothèque Python légère qui s'occupe de la partie **Extract & Load** du pipeline. Concrètement :

- Il appelle l'API AF/KLM avec pagination automatique
- Il normalise la réponse JSON (qui est imbriquée et complexe) en **3 tables relationnelles plates**
- Il crée automatiquement les tables dans Supabase à la première exécution (pas besoin de SQL manuel)
- Il gère le chargement incrémental (on ne recharge pas tout à chaque fois)

C'est l'équivalent d'un "tuyau" automatique entre l'API et la base de données.

### dbt Core — Data Build Tool

dbt est l'outil qui s'occupe de la **transformation** des données. Il travaille entièrement en SQL, directement dans Supabase. Les données brutes passent par 3 couches successives :

| Couche | Dossier | Ce qu'elle fait |
|--------|---------|-----------------|
| **raw** | `models/1_raw/` | Renomme et type les colonnes brutes — aucune logique métier |
| **int** (intermédiaire) | `models/2_int/` | Calcule les retards, la congestion aéroportuaire — transformations lourdes |
| **mart** | `models/3_mart/` | Produit le schéma final prêt pour le ML : `fct_flight_legs`, `dim_airports`, `dim_airlines`, `dim_date` |

L'analogie : les données brutes sont comme des ingrédients non préparés. dbt est la cuisine qui les transforme étape par étape en un plat fini, documenté et testable.

### mise — Gestionnaire d'environnement

mise gère la version de Python utilisée dans ce projet (3.13.2) et définit la variable `DBT_PROFILES_DIR` qui force dbt à utiliser le fichier de configuration local (`profiles.yml`) plutôt qu'un fichier global partagé. Cela garantit l'**isolation** entre ce projet et d'autres projets dbt sur la même machine.

---

## Deux environnements Supabase : dev et prod

Le projet utilise deux bases de données Supabase distinctes dans l'organisation **AirLines DST** :

| Environnement | Projet Supabase | Région AWS | Usage |
|---------------|----------------|------------|-------|
| **dev** | `afklm_delay_db_dev` | eu-west-1 | Développement, tests, validation de la pipeline |
| **prod** | `afklm_delay_db_prod` | eu-central-1 | Données de production, présentation finale |

Cette séparation est fondamentale : on ne risque jamais d'écraser des données de production par accident pendant le développement.

### Comment switcher d'environnement

```bash
# Activer l'environnement dev
source ./switch-env.sh dev

# Activer l'environnement prod
source ./switch-env.sh prod
```

Le script copie le bon fichier `.env` (`.env.dev` ou `.env.prod`) et exporte toutes les variables dans le shell courant. dlt et dbt lisent ensuite ces variables automatiquement — aucune configuration à modifier à la main.

> **Note :** Le mot-clé `source` est obligatoire pour que les variables soient disponibles dans le shell courant (pas seulement dans un sous-processus).

---

## Structure du projet

```
afklm-delay-pipeline/
│
├── ingestion/                    ← Scripts dlt (Extract & Load)
│   ├── afklm_source.py           ← Définition de la source API AF/KLM
│   ├── afklm_dlt_pipeline.py     ← Pipeline dlt (exécuter pour charger les données)
│   └── verify_ingestion.py       ← Vérification post-ingestion
│
├── models/                       ← Modèles dbt (SQL)
│   ├── 1_raw/                    ← Couche source (renommage, typage)
│   ├── 2_int/                    ← Couche intermédiaire (features ML)
│   └── 3_mart/                   ← Couche finale (fct + dim)
│
├── .dlt/
│   ├── config.toml               ← Configuration dlt (versionnable, pas de secrets)
│   └── secrets.toml              ← Clé API AF/KLM (gitignore — local uniquement)
│
├── .env                          ← Environnement actif (gitignore — local uniquement)
├── .env.dev                      ← Credentials Supabase DEV (gitignore)
├── .env.prod                     ← Credentials Supabase PROD (gitignore)
├── .mise.toml                    ← Version Python + DBT_PROFILES_DIR
├── profiles.yml                  ← Configuration dbt (gitignore — local uniquement)
├── dbt_project.yml               ← Définition du projet dbt
├── switch-env.sh                 ← Script de bascule dev ↔ prod
└── requirements.txt              ← Dépendances Python avec versions pinnées
```

---

## Installation

### Prérequis

- [mise](https://mise.jdx.dev/) installé (`brew install mise`)
- Accès à l'organisation Supabase **AirLines DST**
- Clé API AF/KLM (developer.airfranceklm.com)

### Étapes

```bash
# 1. Cloner le repo et entrer dans le dossier
git clone <url-du-repo>
cd afklm-delay-pipeline

# 2. Faire confiance au fichier mise (une seule fois)
mise trust

# 3. Installer Python (si pas déjà présent)
mise install

# 4. Créer et activer l'environnement virtuel Python
python -m venv venv
source venv/bin/activate

# 5. Installer les dépendances
pip install -r requirements.txt

# 6. Créer les fichiers de secrets locaux
cp .dlt/secrets.toml.example .dlt/secrets.toml
# → éditer .dlt/secrets.toml et renseigner la clé API AF/KLM

# Les fichiers .env.dev et .env.prod sont fournis séparément (hors Git)
```

---

## Exécution du pipeline

### En développement (dev)

```bash
source ./switch-env.sh dev      # → pointe sur afklm_delay_db_dev

# Étape 1 : Ingestion (si pas déjà fait)
python ingestion/afklm_dlt_pipeline.py

# Étape 2 : Transformation dbt
dbt debug                       # vérifie la connexion Supabase
dbt run                         # construit tous les modèles

# Étape 3 : Tests et documentation
dbt test
dbt docs generate && dbt docs serve
```

### En production (prod)

```bash
source ./switch-env.sh prod     # → pointe sur afklm_delay_db_prod

# Même séquence — dlt crée automatiquement les tables si elles n'existent pas
python ingestion/afklm_dlt_pipeline.py
dbt run
```

---

## Isolation et sécurité

Tous les fichiers contenant des secrets ou des données d'environnement sont **gitignorés** — ils ne sont jamais versionnés :

| Fichier | Contenu | Pourquoi ignoré |
|---------|---------|-----------------|
| `.env` | Variables actives (host, password…) | Credentials en clair |
| `.env.dev` | Credentials Supabase dev | Credentials en clair |
| `.env.prod` | Credentials Supabase prod | Credentials en clair |
| `profiles.yml` | Config dbt avec host/password | Credentials en clair |
| `.dlt/secrets.toml` | Clé API AF/KLM | Secret API |

Les fichiers `.example` versionnés (`profiles.yml.example`, `.dlt/secrets.toml.example`) servent de templates documentés sans aucun secret.

---

## Modèles dbt — détail

### Couche raw (`1_raw`)

Renommage et typage des colonnes brutes chargées par dlt. Aucune logique métier. Matérialisés en **vues**.

| Modèle | Source | Description |
|--------|--------|-------------|
| `flight_data__source_operational_flights` | `operational_flights` | Un vol par ligne |
| `flight_data__source_operational_flight_legs` | `operational_flight_legs` | Un segment par ligne |
| `flight_data__source_operational_flight_delays` | `operational_flight_delays` | Un code retard par segment |

### Couche int (`2_int`)

Calculs et enrichissements pour préparer les features du modèle ML. Matérialisés en **vues**.

| Modèle | Description |
|--------|-------------|
| `flight_data__int_delays_leg` | Calcul du retard réel par segment (parse des durées ISO 8601) |
| `flight_data__int_legs_ready` | Jointure vol + segment + retards, features de base |
| `flight_data__int_airport_congestion` | Indicateur de congestion par aéroport et fenêtre horaire |

### Couche mart (`3_mart`)

Schéma en étoile prêt pour le ML et la visualisation. Matérialisés en **tables**.

| Modèle | Type | Description |
|--------|------|-------------|
| `fct_flight_legs` | Fait | Table centrale avec toutes les features ML (retards, congestion, dimensions) |
| `dim_airports` | Dimension | Référentiel des aéroports |
| `dim_airlines` | Dimension | Référentiel des compagnies |
| `dim_date` | Dimension | Calendrier |

---

## Commandes utiles

```bash
# Switcher d'environnement
source ./switch-env.sh dev
source ./switch-env.sh prod

# Vérifier l'environnement actif
cat .env | grep -E "DBT_TARGET|AFKLM_DB_HOST"

# dbt
dbt debug                    # test de connexion
dbt run                      # exécuter tous les modèles
dbt run --select 3_mart      # exécuter seulement la couche mart
dbt test                     # lancer les tests de qualité
dbt docs generate            # générer la documentation
dbt docs serve               # visualiser la documentation et le lineage

# dlt
python ingestion/afklm_dlt_pipeline.py    # lancer l'ingestion
python ingestion/verify_ingestion.py      # vérifier les tables chargées
```

---

## Statut du projet

| Composant | Statut |
|-----------|--------|
| Source API AF/KLM (dlt) | Validé en dev |
| Modèles dbt (raw → int → mart) | En cours de validation |
| Configuration mise + switch-env | Opérationnel |
| Isolation dev / prod | Opérationnel |
| Scoring ML (ml_score.py) | En cours |
| Pipeline complet en prod | A venir |

> **Le pipeline est actuellement en phase de validation.** Les données sont correctement ingérées en environnement de développement (`afklm_delay_db_dev`). La validation complète de la chaîne dbt (raw → int → mart) et l'intégration du scoring ML constituent les prochaines étapes avant la mise en production sur `afklm_delay_db_prod`.

---

## Ressources

- [Documentation dlt](https://dlthub.com/docs)
- [Documentation dbt Core](https://docs.getdbt.com/docs/introduction)
- [API AF/KLM](https://developer.airfranceklm.com)
- [Supabase](https://supabase.com/docs)
- [mise](https://mise.jdx.dev/)
