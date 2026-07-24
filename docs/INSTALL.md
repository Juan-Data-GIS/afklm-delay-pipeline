# Guide d'Installation - DST Airlines

Ce document décrit les étapes pour déployer et exécuter l'environnement de développement complet du projet sur une nouvelle machine.

## 1. Prérequis Système

Assurez-vous que les outils suivants sont installés sur votre poste de travail :
* **Git** : Pour cloner le dépôt.
* **Docker Desktop** : (Si sous Windows, vous pouvez activer l'intégration **WSL 2** dans les paramètres Docker).
* **VS Code** (recommandé) : Avec les extensions "WSL" et "Docker".

## 2. Clonage du dépôt

Ouvrez un terminal (idéalement sous WSL si vous êtes sur Windows) et clonez le projet :

```bash
git clone <URL_DE_VOTRE_DEPOT_GIT>
cd afklm-delay-pipeline
```

## 3. Configuration des variables d'environnement

L'architecture nécessite des clés de connexion pour fonctionner.
Créez un fichier nommé exactement `.env` à la racine du projet (au même niveau que le `docker-compose.yml`) :

```bash
cp .env.example .env
# → éditer .env : AFKLM_DB_*, MODEL_*_URL, ENV_TARGET, DBT_TARGET

cp profiles.yml.example profiles.yml
# → profiles.yml est gitignoré ; documente les targets local / dev / prod

# Créer le fichier secret Alertmanager (obligatoire, sinon crash au boot)
touch .smtp_password
# → laisser vide si vous ne recevrez pas les mails (Alertmanager démarrera quand même)
# → renseigner 1 seule ligne = Google App Password 16 chars pour recevoir les alertes
```

> Le fichier `.smtp_password` est monté dans le container Alertmanager comme secret file (`smtp_auth_password_file`). S'il est absent sur l'hôte, Docker crée un dossier vide à sa place et Alertmanager crash. S'il est vide, Alertmanager démarre mais l'envoi de mail échoue silencieusement — utile pour les postes qui ne font pas la démo mail.

Points critiques à vérifier dans `.env` :

| Variable | Valeur recommandée | Rôle |
|----------|-------------------|------|
| `ENV_TARGET` | `prod` | Pilote FastAPI, ML, monitoring_utils (Supabase vs Postgres Docker) |
| `DBT_TARGET` | `prod` | Target dbt dans `profiles.yml` — doit rester cohérent avec `ENV_TARGET` |
| `AIRFLOW_LOG_TO_DB` | `1` | Active l'écriture des logs Airflow dans `logs.airflow_events` |

*(Note : le fichier `.env` est ignoré par Git via le `.gitignore` pour des raisons de sécurité).*

## 4. Démarrage de l'infrastructure

Une fois le `.env` en place, lancez la construction des images et le démarrage des conteneurs en mode détaché :

```bash
docker compose up -d --build
```

Le premier lancement prendra quelques minutes, le temps que Docker télécharge les images de base (Airflow, Python, Postgres) et installe les librairies (`requirements.txt`).

## 5. Vérification du déploiement

Vérifiez que tous les services sont `Up` avec la commande :

```bash
docker compose ps
```

Vous pouvez ensuite accéder aux interfaces web depuis votre navigateur :
* **Airflow** : http://localhost:8081 *(Login: admin / Mot de passe: admin)*
* **API FastAPI** : http://localhost:8000/docs
* **Dashboard Streamlit** : http://localhost:8501
* **Grafana** : http://localhost:3000 *(Login: admin / Mot de passe: admin)*

> **Note d'architecture :** Les ports locaux (8081, 8501, 5433...) ont été spécifiquement choisis et "décalés" par rapport à leurs valeurs par défaut (8080, 8501, 5432) pour ne pas interférer avec d'autres projets potentiellement en cours d'exécution sur votre machine.

## 6. Bootstrap Supabase (première fois uniquement)

Quand `ENV_TARGET=prod`, FastAPI, ML et les callbacks Airflow écrivent dans le schéma `logs` sur **Supabase Cloud**. Ces tables n'existent pas par défaut : il faut les créer une seule fois.

1. Ouvrir le projet Supabase → **SQL Editor**
2. Copier-coller le contenu de [`postgres_init/supabase_logs_bootstrap.sql`](../postgres_init/supabase_logs_bootstrap.sql)
3. Exécuter le script (idempotent : peut être relancé sans erreur)
4. Vérifier :

```sql
SELECT COUNT(*) FROM logs.airflow_events;
```

Le résultat doit être `0` sans erreur. Les tables créées sont :
- `logs.airflow_events` — événements unitaires (dlt / dbt / ML / API)
- `logs.pipeline_runs` — agrégation des runs Airflow (Grafana)

> **Note :** le conteneur Docker `postgres_local` crée déjà ces tables via `postgres_init/init_dataops_logs.sql` au premier boot. Cette étape 6 ne concerne **que** Supabase.

## 7. Airflow Connection `supabase_prd`

Indispensable dès que `ENV_TARGET=prod` : `monitoring_utils.py` utilise la connection Airflow `supabase_prd` pour écrire dans `logs.airflow_events` / `logs.pipeline_runs` (via `PostgresHook`).

### Via l'UI Airflow

1. Ouvrir http://localhost:8081 (login : `admin` / `admin`)
2. **Admin** → **Connections** → **+**
3. Remplir :

| Champ | Valeur |
|-------|--------|
| Connection Id | `supabase_prd` |
| Connection Type | `Postgres` |
| Host | valeur de `AFKLM_DB_HOST` dans `.env` |
| Schema | `postgres` |
| Login | valeur de `AFKLM_DB_USER` |
| Password | valeur de `AFKLM_DB_PASSWORD` |
| Port | `5432` |
| Extra | `{"sslmode": "require"}` |

### Via la CLI (recommandé, reproductible)

Depuis la racine du projet, avec le `.env` chargé :

```bash
# Charger les variables si besoin
set -a && source .env && set +a

docker exec afklm-formation-apiserver \
  airflow connections add supabase_prd \
    --conn-type postgres \
    --conn-host "$AFKLM_DB_HOST" \
    --conn-login "$AFKLM_DB_USER" \
    --conn-password "$AFKLM_DB_PASSWORD" \
    --conn-schema postgres \
    --conn-port 5432 \
    --conn-extra '{"sslmode":"require"}'
```

Vérification :

```bash
docker exec afklm-formation-apiserver airflow connections get supabase_prd
```

> **Rappel :** sans cette connection, les callbacks de logging Airflow échouent silencieusement (warning) quand `ENV_TARGET=prod`, et Streamlit / Grafana ne verront aucun événement pipeline.

## 8. Arrêter proprement l'environnement

Pour arrêter l'environnement à la fin de votre session sans perdre vos données (dbt, bases locales), utilisez :

```bash
docker compose stop
```

*(Pour tout relancer le lendemain, un simple `docker compose start` suffira).*
