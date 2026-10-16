"""
SciPulse — Callbacks Airflow d'observabilité (TP 8, monitoring)

Chaque fin de tâche (succès, échec, retry) est indexée dans `pipeline-logs` :
le dashboard « SciPulse — Monitoring Pipeline » en tire le taux de succès/échec
et la durée d'exécution par tâche.

Ce module est exécuté par Airflow lui-même (pas par le virtualenv du pipeline) :
il n'utilise que `requests`, toujours présent dans l'image Airflow, et ne lève
jamais d'exception (un problème de monitoring ne doit pas faire échouer le pipeline).
"""

import logging
import os
from datetime import datetime, timezone

import requests

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
LOG_INDEX = "pipeline-logs"
log = logging.getLogger(__name__)

# Même mapping que src/utils/mappings.py (PIPELINE_LOGS_SETTINGS) : si l'index n'existe
# pas encore, on le crée explicitement plutôt que de laisser le mapping dynamique
# transformer dag_id/task_id en champs `text`.
_PIPELINE_LOGS_INDEX = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic_templates": [
            {"strings_as_keywords": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}}
        ],
        "properties": {
            "@timestamp": {"type": "date"},
            "duration": {"type": "float"},
            "index_size_mb": {"type": "float"},
        },
    },
}
_index_ready = False


def _ensure_index():
    global _index_ready
    if not _index_ready:
        if requests.head(f"{ES_HOST}/{LOG_INDEX}", timeout=5).status_code == 404:
            requests.put(f"{ES_HOST}/{LOG_INDEX}", json=_PIPELINE_LOGS_INDEX, timeout=5)
        _index_ready = True


def _index_task_event(context, state: str):
    try:
        _ensure_index()
        ti = context["task_instance"]
        duration = ti.duration
        if duration is None and ti.start_date is not None:
            duration = (datetime.now(timezone.utc) - ti.start_date).total_seconds()
        doc = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "airflow_task",
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context.get("run_id"),
            "state": state,
            "try_number": ti.try_number,
            "duration": float(duration) if duration is not None else None,
            "start_date": ti.start_date.isoformat() if ti.start_date else None,
            "end_date": (ti.end_date or datetime.now(timezone.utc)).isoformat(),
            "operator": ti.operator,
        }
        exception = context.get("exception")
        if exception is not None:
            doc["error"] = str(exception)[:1000]
        requests.post(f"{ES_HOST}/{LOG_INDEX}/_doc", json=doc, timeout=5)
    except Exception as e:  # jamais bloquant
        log.warning("Monitoring SciPulse : événement non indexé (%s)", e)


def on_task_success(context):
    _index_task_event(context, "success")


def on_task_failure(context):
    _index_task_event(context, "failed")


def on_task_retry(context):
    _index_task_event(context, "up_for_retry")


MONITORING_CALLBACKS = {
    "on_success_callback": on_task_success,
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}
