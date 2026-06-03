import json
import logging
import os
from datetime import datetime, timezone

LOGGER = logging.getLogger("airflow.task")

# Table de destination pour la centralisation DataOps
LOG_TABLE = "logs.airflow_events"

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
    """Détermine dynamiquement la connexion Airflow à utiliser selon la cible."""
    env_target = os.environ.get("ENV_TARGET", "local").strip().lower()
    return "postgres_local" if env_target == "local" else "supabase_prd"


def _db_logging_enabled() -> bool:
    return os.environ.get("AIRFLOW_LOG_TO_DB", "1").lower() not in ("0", "false", "no")


def _resolve_run_id(explicit: str | None, payload: dict) -> str | None:
    if explicit:
        return explicit
    run_id = payload.get("run_id")
    if run_id:
        return str(run_id)
    try:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        if not ctx:
            return None
        if ctx.get("run_id"):
            return str(ctx["run_id"])
        dag_run = ctx.get("dag_run")
        if dag_run is not None and getattr(dag_run, "run_id", None):
            return str(dag_run.run_id)
    except Exception:
        return None
    return None


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
    # 1. Priorité absolue au timestamp explicite (Sera éventuellement écrasé par la date métier J-1/Backfill)
    if explicit_timestamp:
        event_at = explicit_timestamp
    else:
        event_at = datetime.now(timezone.utc)

    # 2. Synchronisation dynamique avec la date métier si fournie par l'XCom (Résolution de ta demande event_at)
    if extra and "event_at" in extra:
        try:
            event_at = datetime.strptime(str(extra["event_at"]), "%Y-%m-%d")
        except Exception:
            pass
    else:
        try:
            from airflow.sdk import get_current_context
            ctx = get_current_context()
            if ctx:
                dag_run = ctx.get("dag_run")
                if dag_run and dag_run.conf and dag_run.conf.get("start_date"):
                    try:
                        date_str = dag_run.conf["start_date"]
                        event_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    except Exception:
                        pass
                elif ctx.get("logical_date"):
                    event_at = ctx["logical_date"]
                elif ctx.get("data_interval_start"):
                    event_at = ctx["data_interval_start"]
        except Exception as ctx_err:
            LOGGER.debug("Impossible de capturer le contexte temporel Airflow : %s", ctx_err)

    # 3. Construction du payload final
    payload = {
        "app": "airflow",
        "level": level.upper(),
        "layer": layer.lower(),
        "dag_id": dag_id,
        "task_id": task_id,
        "event_type": event_type,
        "message": message,
        "timestamp": event_at.isoformat() if hasattr(event_at, "isoformat") else str(event_at),
        **extra,
    }
    if run_id:
        payload["run_id"] = run_id

    json_payload = json.dumps(payload, default=str)
    print(json_payload)

    try:
        _persist_event(payload)
    except Exception as exc:
        LOGGER.warning("log_event DB persist failed: %s", exc)


def log_operator_failure(
    context,
    *,
    layer: str,
    event_type: str = "task_failure",
    message: str | None = None,
) -> None:
    ti = context["task_instance"]
    exc = context.get("exception")
    logical_date = context.get("logical_date") or context.get("execution_date")

    log_event(
        level="error",
        layer=layer,
        message=message or f"task failed for {ti.task_id}",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        event_type=event_type,
        explicit_timestamp=logical_date,
        exception=str(exc) if exc else None,
    )


def log_operator_success(
    context,
    *,
    layer: str,
    event_type: str,
    message: str | None = None,
) -> None:
    ti = context["task_instance"]
    row_count = context.get("return_value")
    extra = {}
    
    logical_date = context.get("logical_date") or context.get("execution_date")

    # Récupération dynamique des métriques transmises par le script
    extra_metrics = ti.xcom_pull(task_ids=ti.task_id, key='data_metrics') or {}
    if isinstance(extra_metrics, dict):
        extra.update(extra_metrics)

    if row_count is not None and "row_count" not in extra:
        extra["row_count"] = row_count

    records = extra.get("records_processed")
    legs = extra.get("legs_processed")
    
    computed_message = message
    if not computed_message:
        if records is not None:
            if legs is not None:
                computed_message = f"Pipeline Ingestion REUSSI : {records} vols et {legs} segments synchronises."
            else:
                computed_message = f"Pipeline Transformation REUSSI : {records} enregistrements mis a jour."
        else:
            computed_message = f"task success for {ti.task_id}"

    log_event(
        level="INFO",
        layer=layer,
        message=computed_message,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        event_type=event_type,
        explicit_timestamp=logical_date,
        **extra,
    )


def operator_failure_callbacks(*, layer: str, event_type: str = "task_failure"):
    def _log_failure(context):
        log_operator_failure(context, layer=layer, event_type=event_type)
    return [_log_failure]