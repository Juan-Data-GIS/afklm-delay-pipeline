-- postgres_init/init_dataops_logs.sql
-- Ce script s'exécute automatiquement à l'initialisation du conteneur local

-- 1. Connexion forcée sur la base applicative créée par l'environnement
\c data_hub;

-- 2. Création de la couche d'observabilité DataOps
CREATE SCHEMA IF NOT EXISTS logs;

-- 3. Provisionnement de la table centrale de centralisation des logs
CREATE TABLE IF NOT EXISTS logs.airflow_events (
    id SERIAL PRIMARY KEY,
    event_at TIMESTAMPTZ NOT NULL,
    app VARCHAR(50) DEFAULT 'airflow',
    level VARCHAR(10) NOT NULL,
    layer VARCHAR(50) NOT NULL,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    run_id VARCHAR(255),
    event_type VARCHAR(100),
    message TEXT,
    extra JSONB
);

-- Indexation préventive pour les performances de lecture (Streamlit / API)
CREATE INDEX IF NOT EXISTS idx_airflow_events_at ON logs.airflow_events (event_at DESC);