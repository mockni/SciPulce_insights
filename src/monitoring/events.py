"""
SciPulse — Événements d'observabilité du pipeline (index `pipeline-logs`)

Chaque étape du pipeline publie des événements structurés :
  - layer_write    : une couche a été écrite (couche, source, lignes, objets, durée)
  - quality_check  : résultat d'une suite Great Expectations (taux d'erreur)
  - layer_snapshot : état d'un bucket MinIO (volume, fraîcheur) — voir pipeline_metrics
  - airflow_task   : fin d'une tâche Airflow (callbacks des DAGs)

Ces documents alimentent le dashboard « SciPulse — Monitoring Pipeline ».
Ce ne sont pas des données métier : Kibana n'expose les données métier que via la couche Gold.

La publication ne lève jamais d'exception : un problème de monitoring ne doit
pas faire échouer le pipeline. SCIPULSE_MONITORING=0 la désactive (tests).
"""

import logging
import os
from datetime import datetime, timezone

from src.config import ES_HOST, INDEX_PIPELINE_LOGS

log = logging.getLogger("scipulse.monitoring")

PIPELINE_LOGS_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic_templates": [
            {"strings_as_keywords": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}}
        ],
        "properties": {
            "@timestamp": {"type": "date"},
            "type": {"type": "keyword"},
            "layer": {"type": "keyword"},
            "source": {"type": "keyword"},
            "suite": {"type": "keyword"},
            "success": {"type": "boolean"},
            "rows": {"type": "long"},
            "objects": {"type": "long"},
            "size_mb": {"type": "float"},
            "freshness_minutes": {"type": "float"},
            "error_rate": {"type": "float"},
            "expectations_evaluated": {"type": "integer"},
            "expectations_failed": {"type": "integer"},
            "duration": {"type": "float"},
            "dag_id": {"type": "keyword"},
            "task_id": {"type": "keyword"},
            "run_id": {"type": "keyword"},
            "state": {"type": "keyword"},
        },
    },
}

_client = None
_index_ready = False


def enabled() -> bool:
    return os.getenv("SCIPULSE_MONITORING", "1") != "0"


def _get_client():
    global _client
    if _client is None:
        from elasticsearch import Elasticsearch

        _client = Elasticsearch(ES_HOST, request_timeout=5, max_retries=0)
    return _client


def ensure_index(es=None):
    """Crée l'index pipeline-logs avec son mapping s'il n'existe pas."""
    global _index_ready
    es = es or _get_client()
    if not es.indices.exists(index=INDEX_PIPELINE_LOGS):
        es.indices.create(index=INDEX_PIPELINE_LOGS, settings=PIPELINE_LOGS_SETTINGS["settings"],
                          mappings=PIPELINE_LOGS_SETTINGS["mappings"])
    _index_ready = True


def airflow_context() -> dict:
    """dag_id / run_id / task_id quand le script est lancé par un BashOperator."""
    ctx = {}
    for env, field in [("AIRFLOW_CTX_DAG_ID", "dag_id"), ("AIRFLOW_CTX_DAG_RUN_ID", "run_id"),
                       ("AIRFLOW_CTX_TASK_ID", "task_id")]:
        if os.getenv(env):
            ctx[field] = os.getenv(env)
    return ctx


def emit(event_type: str, **fields) -> bool:
    """Publie un événement. Retourne True s'il a été indexé."""
    if not enabled():
        return False
    doc = {"@timestamp": datetime.now(timezone.utc).isoformat(), "type": event_type,
           **airflow_context(), **{k: v for k, v in fields.items() if v is not None}}
    try:
        es = _get_client()
        if not _index_ready:
            ensure_index(es)
        es.index(index=INDEX_PIPELINE_LOGS, document=doc)
        return True
    except Exception as e:  # jamais bloquant
        log.warning("Événement de monitoring non indexé (%s) : %s", event_type, e)
        return False


def emit_layer_write(layer: str, source: str, rows: int, objects: int, started: float, **extra):
    import time

    return emit("layer_write", layer=layer, source=source, rows=rows, objects=objects,
                duration=round(time.time() - started, 2), **extra)
