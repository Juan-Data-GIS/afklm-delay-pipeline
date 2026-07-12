# 🛠️ Guide d'Installation - DST Airlines

Ce document décrit les étapes pour déployer et exécuter l'environnement de développement complet du projet sur une nouvelle machine.

## 1. Prérequis Système

Assurez-vous que les outils suivants sont installés sur votre poste de travail :
* **Git** : Pour cloner le dépôt.
* **Docker Desktop** : (Si sous Windows, vous pouvezactiver l'intégration **WSL 2** dans les paramètres Docker).
* **VS Code** (recommandé) : Avec les extensions "WSL" et "Docker".

## 2. Clonage du dépôt

Ouvrez un terminal (idéalement sous WSL si vous êtes sur Windows) et clonez le projet :

git clone <URL_DE_VOTRE_DEPOT_GIT>
cd afklm-pipeline/dev

## 3. Configuration des variables d'environnement

L'architecture nécessite des clés de connexion pour fonctionner. 
Créez un fichier nommé exactement `.env` à la racine du projet (au même niveau que le `docker-compose.yml`).

Copiez-collez le template ci-dessous dans votre fichier `.env` et remplacez les valeurs `<...>` par les identifiants fournis par l'équipe :

# --- SUPABASE (BDD POSTGRES) ---
DB_HOST=<aws-x-eu-west-1.pooler.supabase.com>
DB_NAME=postgres
DB_USER=<postgres.votre_id>
DB_PASSWORD=<Votre_Mot_De_Passe_Supabase>
DB_PORT=5432

# --- AIR FRANCE - KLM API ---
AIRFRANCE_API_KEY=<Votre_Cle_API_AFKLM>

# --- DOCKER CONFIGURATION ---
AIRFLOW_UID=1000

*(Note : Ce fichier `.env` est ignoré par Git via le `.gitignore` pour des raisons de sécurité).*

## 4. Démarrage de l'infrastructure

Une fois le `.env` en place, lancez la construction des images et le démarrage des conteneurs en mode détaché :

docker compose up -d --build

Le premier lancement prendra quelques minutes, le temps que Docker télécharge les images de base (Airflow, Python, Postgres) et installe les librairies (`requirements.txt`).

## 5. Vérification du déploiement

Vérifiez que tous les services sont `Up` avec la commande :

docker compose ps

Vous pouvez ensuite accéder aux interfaces web depuis votre navigateur :
* **Airflow** : http://localhost:8081 *(Login: airflow / Mot de passe: airflow)*
* **API FastAPI** : http://localhost:8000/docs
* **Dashboard Streamlit** : http://localhost:8505

> **Note d'architecture :** *Les ports locaux (8081, 8505, 5440...) ont été spécifiquement choisis et "décalés" par rapport à leurs valeurs par défaut (8080, 8501, 5432) pour ne pas interférer avec d'autres projets potentiellement en cours d'exécution sur votre machine.*

## 6. Arrêter proprement l'environnement

Pour arrêter l'environnement à la fin de votre session sans perdre vos données (dbt, bases locales), utilisez :

docker compose stop

*(Pour tout relancer le lendemain, un simple `docker compose start` suffira).*