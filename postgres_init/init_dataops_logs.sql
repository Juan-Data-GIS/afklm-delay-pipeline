-- postgres_init/init_dataops_logs.sql
-- Ce script s'exécute automatiquement à l'initialisation du conteneur local

-- Connexion forcée sur la base applicative créée par l'environnement
\c data_hub;

-- Création de la couche d'observabilité DataOps
CREATE SCHEMA IF NOT EXISTS logs;

-- Table principale de centralisation des logs (Evenements unitaires)
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

-- Indexation pour les performances de lecture
CREATE INDEX IF NOT EXISTS idx_airflow_events_at ON logs.airflow_events (event_at DESC);


-- Table d'agrégation des Runs
CREATE TABLE IF NOT EXISTS logs.pipeline_runs (
    run_id VARCHAR(255) NOT NULL,
    dag_id VARCHAR(100) NOT NULL,
    date_metier DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) DEFAULT 'RUNNING',
    vols_ingested INT DEFAULT 0,
    transformation_rows INT DEFAULT 0,
    finished_at TIMESTAMPTZ,
    duration_sec INT,
    CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id)
);

-- Indexation pour les performances de lecture ( Grafana )
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON logs.pipeline_runs (date_metier DESC);