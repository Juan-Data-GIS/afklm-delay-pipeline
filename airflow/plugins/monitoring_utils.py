import json
import logging
import os
from datetime import datetime, timezone

LOGGER = logging.getLogger("airflow.task")

LOG_TABLE = "logs.airflow_events"
RUN_TABLE = "logs.pipeline_runs"

_CORE_KEYS = frozenset({
    "app",
    "level",
    "layer",
    "dag_id",
    "task_id",
    "run_id",
    "event_type",
    "message",
    "timestamp",
})


def _get_log_conn_id() -> str:
    env_target = os.environ.get("ENV_TARGET", "local").strip().lower()
    return "postgres_local" if env_target == "local" else "supabase_prd"


def _db_logging_enabled() -> bool:
    return os.environ.get("AIRFLOW_LOG_TO_DB", "1").lower() not in ("0", "false", "no")


def _resolve_run_id(explicit: str | None, payload: dict) -> str | None:
    if explicit:
        return str(explicit)
    run_id = payload.get("run_id")
    if run_id:
        return str(run_id)
    try:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        if ctx and ctx.get("run_id"):
            return str(ctx["run_id"])
        dag_run = ctx.get("dag_run")
        if dag_run is not None and getattr(dag_run, "run_id", None):
            return str(dag_run.run_id)
    except Exception:
        pass
    return None


def _extract_date_metier(payload: dict) -> datetime:
    # 1. PRIORITÉ ABSOLUE : La date métier explicite transmise
    if payload.get("explicit_business_date"):
        try:
            return datetime.fromisoformat(str(payload["explicit_business_date"]).replace("Z", "+00:00"))
        except Exception:
            pass
    # 2. SINON, on tente de lire le contexte Airflow
    try:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        if ctx:
            dag_run = ctx.get("dag_run")
            
            if dag_run and dag_run.conf:
                if dag_run.conf.get("start_date"):
                    try:
                        return datetime.fromisoformat(str(dag_run.conf["start_date"]).strip().replace("Z", "+00:00"))
                    except Exception:
                        pass
                if dag_run.conf.get("date_metier"):
                    try:
                        return datetime.fromisoformat(str(dag_run.conf["date_metier"]).strip().replace("Z", "+00:00"))
                    except Exception:
                        pass

            if ctx.get("params") and hasattr(ctx["params"], "get"):
                p_start = ctx["params"].get("start_date")
                if p_start:
                    try:
                        return datetime.fromisoformat(str(p_start).strip().replace("Z", "+00:00"))
                    except Exception:
                        pass

            if ctx.get("logical_date"):
                return ctx["logical_date"]
            if ctx.get("data_interval_start"):
                return ctx["data_interval_start"]
    except Exception as err:
        LOGGER.debug("Erreur lors de l'extraction de la date métier: %s", err)

    return datetime.now(timezone.utc)


def _persist_pipeline_run(payload: dict, run_id: str, date_metier: datetime) -> None:
    """Met à jour ou insère l'état d'avancement du pipeline de production de manière dynamique."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import Json
    
    dag_id = payload.get("dag_id", "unknown_dag")
    task_id = payload.get("task_id", "unknown_task")
    event_type = payload.get("event_type", "")
    level = payload.get("level", "INFO")
    message = payload.get("message", "")
    
    # CORRECTION : Extraction sécurisée des metrics (Fallback sur 0 avant le int())
    val_ingest = payload.get("vols_ingested") or payload.get("rows_inserted") or 0
    vols_ingested = int(val_ingest) if "ingest" in str(dag_id) or "ingest" in str(task_id) else 0
    
    val_trans = payload.get("rows_inserted") or payload.get("transformation_rows") or payload.get("records_processed") or payload.get("row_count") or 0
    transformation_rows = int(val_trans) if "transform" in str(dag_id) or "transform" in str(task_id) or "dbt" in str(task_id) else 0
    
    # Si le payload provient d'une fonction générique sans variables explicites
    if vols_ingested == 0 and transformation_rows == 0:
        records = payload.get("records_processed") or payload.get("row_count") or 0
        if "dbt" in str(task_id) or "transform" in str(task_id) or "dbt" in str(event_type):
            transformation_rows = int(records)
        else:
            vols_ingested = int(records) if "ingest" in str(task_id) or "ingest" in str(dag_id) else 0

    # détection des statuts d'erreur et de succès
    status = "RUNNING"
    if "failure" in str(event_type).lower() or "fail" in str(event_type).lower() or level == "ERROR":
        status = "FAILED"
    elif "success" in str(event_type).lower() or "reload" in str(event_type).lower():
        status = "SUCCESS"

    # Captation du message d'erreur si présent pour le propager sur Grafana
    error_message = payload.get("exception") or message if status == "FAILED" else None
    
    # Extraction de l'ensemble des clés secondaires pour la colonne execution_context
    execution_context = {k: v for k, v in payload.items() if k not in _CORE_KEYS}

    # calcul exact de la durée et persistance de l'erreur
    sql = f"""
        INSERT INTO {RUN_TABLE} (
            run_id, dag_id, date_metier, started_at, status, 
            vols_ingested, transformation_rows, error_message, execution_context
        )
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = CASE WHEN EXCLUDED.status IN ('SUCCESS', 'FAILED') THEN NOW() ELSE {RUN_TABLE}.finished_at END,
            duration_sec = CASE WHEN EXCLUDED.status IN ('SUCCESS', 'FAILED') THEN EXTRACT(EPOCH FROM (NOW() - {RUN_TABLE}.started_at))::INTEGER ELSE {RUN_TABLE}.duration_sec END,
            vols_ingested = CASE WHEN EXCLUDED.vols_ingested > 0 THEN EXCLUDED.vols_ingested ELSE {RUN_TABLE}.vols_ingested END,
            transformation_rows = CASE WHEN EXCLUDED.transformation_rows > 0 THEN EXCLUDED.transformation_rows ELSE {RUN_TABLE}.transformation_rows END,
            error_message = COALESCE(EXCLUDED.error_message, {RUN_TABLE}.error_message),
            execution_context = COALESCE(EXCLUDED.execution_context, {RUN_TABLE}.execution_context);
    """
    
    try:
        conn_id = _get_log_conn_id()
        hook = PostgresHook(postgres_conn_id=conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    run_id, dag_id, date_metier.date(), status, 
                    vols_ingested, transformation_rows, error_message, 
                    Json(execution_context) if execution_context else None
                ))
            conn.commit()
    except Exception as run_err:
        LOGGER.warning("[MONITORING ACCÈS LOGS] Impossible d'écrire dans la table pipeline_run: %s", run_err)


def _persist_event(payload: dict) -> None:
    if not _db_logging_enabled():
        return

    from psycopg2.extras import Json
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    event_at = payload.get("timestamp")
    if isinstance(event_at, str):
        try:
            event_at = datetime.fromisoformat(event_at.replace("Z", "+00:00"))
        except ValueError:
            event_at = datetime.now(timezone.utc)
    elif not isinstance(event_at, datetime):
        event_at = datetime.now(timezone.utc)

    extra = {k: v for k, v in payload.items() if k not in _CORE_KEYS}
    run_id = _resolve_run_id(payload.get("run_id"), payload)

    row = (
        event_at,
        payload.get("app", "airflow"),
        payload.get("level"),
        payload.get("layer"),
        payload.get("dag_id"),
        payload.get("task_id"),
        run_id,
        payload.get("event_type"),
        payload.get("message"),
        Json(extra) if extra else None,
    )

    sql = f"""
        INSERT INTO {LOG_TABLE} (
            event_at, app, level, layer, dag_id, task_id,
            run_id, event_type, message, extra
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn_id = _get_log_conn_id()
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, row)
        conn.commit()

    if run_id:
        payload["run_id"] = run_id
        date_metier = _extract_date_metier(payload)
        _persist_pipeline_run(payload, run_id, date_metier)


def log_event(
    *,
    level: str,
    layer: str,
    message: str,
    dag_id: str,
    task_id: str,
    event_type: str | None = None,
    run_id: str | None = None,
    explicit_timestamp: datetime | None = None,
    **extra,
) -> None:
    # L'événement physique se produit toujours maintenant
    event_at = datetime.now(timezone.utc)

    payload = {
        "app": "airflow",
        "level": level.upper(),
        "layer": layer.lower(),
        "dag_id": dag_id,
        "task_id": task_id,
        "event_type": event_type,
        "message": message,
        "timestamp": event_at.isoformat(),
        **extra,
    }
    
    # On stocke la date métier explicitement pour que _persist_pipeline_run puisse la lire
    if explicit_timestamp:
        payload["explicit_business_date"] = explicit_timestamp.isoformat() if hasattr(explicit_timestamp, "isoformat") else str(explicit_timestamp)
        # Optionnel : on l'ajoute dans extra pour la retrouver dans le JSON d'airflow_events
        payload["business_date"] = payload["explicit_business_date"]

    if run_id:
        payload["run_id"] = run_id

    try:
        _persist_event(payload)
    except Exception as exc:
        LOGGER.warning("log_event DB persist failed: %s", exc)


def log_operator_failure(context, *, layer: str, event_type: str = "task_failure", message: str | None = None) -> None:
    ti = context["task_instance"]
    dr = context.get("dag_run")
    exc = context.get("exception")
    logical_date = context.get("logical_date") or context.get("execution_date")
    log_event(
        level="error",
        layer=layer,
        message=message or f"task failed for {ti.task_id}",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        event_type=event_type,
        run_id=str(dr.run_id) if dr else None,
        explicit_timestamp=logical_date,
        exception=str(exc) if exc else None,
    )


def log_operator_success(context, *, layer: str, event_type: str, message: str | None = None) -> None:
    ti = context["task_instance"]
    dr = context.get("dag_run")
    row_count = context.get("return_value")
    extra = {}
    
    logical_date = context.get("logical_date") or context.get("execution_date")

    extra_metrics = ti.xcom_pull(task_ids=ti.task_id, key='data_metrics') or {}
    if isinstance(extra_metrics, dict):
        extra.update(extra_metrics)

    if isinstance(row_count, dict):
        extra.update(row_count)
    elif row_count is not None and "row_count" not in extra:
        extra["row_count"] = row_count

    log_event(
        level="INFO",
        layer=layer,
        message=message or f"task success for {ti.task_id}",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        event_type=event_type,
        run_id=str(dr.run_id) if dr else None,
        explicit_timestamp=logical_date,
        **extra,
    )


def operator_failure_callbacks(*, layer: str, event_type: str = "task_failure"):
    def _log_failure(context):
        log_operator_failure(context, layer=layer, event_type=event_type)
    return _log_failure


def operator_success_callbacks(*, layer: str, event_type: str):
    def _log_success(context):
        log_operator_success(context, layer=layer, event_type=event_type)
    return _log_success