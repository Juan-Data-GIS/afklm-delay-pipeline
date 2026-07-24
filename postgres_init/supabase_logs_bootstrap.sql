-- postgres_init/supabase_logs_bootstrap.sql
-- A executer une seule fois via Supabase SQL Editor pour provisionner
-- la couche d'observabilite (equivalent Supabase de init_dataops_logs.sql).
-- Idempotent : peut etre relance sans erreur.
--
-- Ne pas utiliser \c (commande psql) : le SQL Editor Supabase n'en a pas besoin.

CREATE SCHEMA IF NOT EXISTS logs;

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

CREATE INDEX IF NOT EXISTS idx_airflow_events_at
    ON logs.airflow_events (event_at DESC);

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
    error_message TEXT,
    execution_context JSONB,
    CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date
    ON logs.pipeline_runs (date_metier DESC);
