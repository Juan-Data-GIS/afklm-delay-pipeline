#  Monitoring & Observabilité Pipeline

##  Résumé Macro
Le module `monitoring_utils.py` est le composant central d'observabilité DataOps de la stack. Il fournit des décorateurs et des écouteurs de cycle de vie (callbacks) permettant de capter de manière homogène l'état de santé, les temps d'exécution et la volumétrie de données de nos différents flux de données (Airflow, scripts autonomes, dbt).

###  Informations Clés
* **Date de mise en production :** Juin 2026
* **Cible de stockage :** Base locale PostgreSQL (`data_hub`), Schéma `logs`, Table `pipeline_runs`.
* **Rôle d'infrastructure :** Sert de source de vérité unique pour le collecteur *Postgres Exporter* afin d'alimenter nos tableaux de bord de supervision **Grafana**.

---

##  Documentation Complète
Pour obtenir le guide d'architecture détaillé, les procédures d'investigation de niveau 2/3 (DSI & Support), ainsi que le dictionnaire complet des données, veuillez consulter le document officiel de l'entreprise :

 **Lien du Google Docs :** [Documentation d'Architecture d'Exploitation & Support](https://docs.google.com/document/d/1tXVgcKK-u6dadu_Av09NgEOeGb6X1Oy4pKKZjhj8RS4/edit?tab=t.0)