# RUNBOOK DE PRODUCTION - DAG_02 : TRANSFORMATION & SCORING ML

### Liens de Référence Centraux
* **[Consulter la Documentation Architecture & Métier complète](https://docs.google.com/document/d/1b4PKEDvnL44BoxI8lgq3NKAGMQe5oYWxlz9Co0Z6Yqg/edit?tab=t.0)**
* **[Ouvrir le Manuel de Résolution des Incidents Global](https://docs.google.com/document/d/1kOJaZubnS5pS8xcjRqp6QYoYkYT0e09v2i0AOu9HmpI/edit?tab=t.0)**

---

## 1. Description du Flux
* **Nom Airflow** : `afklm_02_transformation_scoring`
* **Responsabilité** : Orchestration des transformations SQL complexes via **dbt (Data Build Tool)** pour structurer les couches *Silver* et *Mart*, suivie du déclenchement des requêtes d'inférence (Scoring des retards de vols) auprès de l'API interne **FastAPI** (Modèle XGBoost).
* **Déclenchement** : Automatique par capteur (*TriggerDagRunOperator*) à la suite du succès du DAG_01.

## 2. Procédure d'Astreinte (Run Failed)

### Étape 1 : Diagnostic du composant défaillant
* **Échec sur `afklm_t_dbt_run` ou `afklm_t_dbt_test`** : Erreur de modélisation SQL, contrainte d'intégrité non respectée ou problème d'accès aux volumes système.
* **Échec sur les tâches de Scoring / ML** : Rupture de communication avec le service d'inférence ou corruption des artefacts du modèle.

### Étape 2 : Résolution des Incidents Critiques
1. **Erreur dbt : `Permission denied: /opt/airflow/dbt/logs`**
   * *Cause* : Verrouillage des privilèges sur le système de fichiers hôte lors de la communication entre le conteneur Docker Linux et l'environnement Windows via WSL.
   * *Résolution* : Exécuter la commande suivante sur le terminal de la machine hôte pour réinitialiser les droits d'accès :
     `sudo chmod -R 777 /mnt/c/Projets/afklm-pipeline/production/afklm-delay-pipeline/dbt`
2. **Erreur ML : `ConnectionRefusedError: [Errno 111] Connect call failed`**
   * *Cause* : Le conteneur hébergeant l'API FastAPI (port 8000) est arrêté ou n'a pas survécu à un manque de ressources sur l'hôte.
   * *Résolution* : Forcer le redémarrage du service d'inférence en ligne de commande :
     `docker compose restart fastapi_service`

### Étape 3 : Stratégie de Reprise
Toutes les transformations dbt sont exécutées sous forme de tables ou vues matérialisées éphémères, et l'API ML écrase les anciennes prédictions de retard pour un même identifiant de vol.
* **Action** : Effectuer un **Clear** global sur le run. Le pipeline va recalculer les indicateurs et ré-exécuter le scoring sans causer d'effets de bord.