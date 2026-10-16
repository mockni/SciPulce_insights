"""
SciPulse — Collecte de métriques du pipeline pour le dashboard de monitoring (TP 8, partie C)

Ce script interroge Elasticsearch pour collecter des métriques opérationnelles
(volumes, enrichissement, fraîcheur, santé) et les indexe dans `pipeline-logs`.
Lancé par Airflow, il rattache la mesure au DAG et au run en cours
(variables AIRFLOW_CTX_* injectées par le BashOperator).

Usage :
    python -m src.utils.pipeline_metrics
    python -m src.utils.pipeline_metrics --quiet
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

from src.config import (
    ES_HOST,
    INDEX_ARXIV,
    INDEX_ARXIV_RAW,
    INDEX_HN,
    INDEX_LINKS,
    INDEX_PIPELINE_LOGS,
)
from src.utils.mappings import PIPELINE_LOGS_SETTINGS

LOG_INDEX = INDEX_PIPELINE_LOGS
VOLUME_INDEXES = [INDEX_ARXIV, INDEX_ARXIV_RAW, INDEX_HN, INDEX_LINKS]


def _safe_count(es: Elasticsearch, index: str, query: dict | None = None) -> int:
    try:
        if query:
            return es.count(index=index, query=query)["count"]
        return es.count(index=index)["count"]
    except Exception:
        return 0


def previous_metrics(es: Elasticsearch) -> dict:
    """Dernière mesure enregistrée (pour calculer les volumes ingérés depuis)."""
    try:
        resp = es.search(
            index=LOG_INDEX,
            size=1,
            query={"term": {"type": "pipeline_metrics"}},
            sort=[{"@timestamp": "desc"}],
        )
        hits = resp["hits"]["hits"]
        return hits[0]["_source"] if hits else {}
    except Exception:
        return {}


def collect_metrics(es: Elasticsearch) -> dict:
    """Collecte les métriques du pipeline."""
    metrics = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "pipeline_metrics",
    }

    # Contexte Airflow (si lancé par un DAG)
    for env_var, field in [("AIRFLOW_CTX_DAG_ID", "dag_id"), ("AIRFLOW_CTX_DAG_RUN_ID", "run_id")]:
        if os.getenv(env_var):
            metrics[field] = os.getenv(env_var)

    # ── Volumes ────────────────────────────────────────────────────
    previous = previous_metrics(es)
    for index_name in VOLUME_INDEXES:
        field = f"volume_{index_name.replace('-', '_')}"
        metrics[field] = _safe_count(es, index_name)
        # documents ajoutés depuis la mesure précédente (« volume ingéré par run »)
        if field in previous:
            metrics[f"delta_{index_name.replace('-', '_')}"] = metrics[field] - previous[field]

    # ── Enrichissement ─────────────────────────────────────────────
    metrics["enriched_with_keywords"] = _safe_count(es, INDEX_ARXIV, {"exists": {"field": "keywords"}})
    metrics["enriched_with_hn_score"] = _safe_count(es, INDEX_ARXIV, {"exists": {"field": "hn_score"}})
    if metrics.get("volume_arxiv_papers"):
        metrics["enrichment_rate"] = round(metrics["enriched_with_keywords"] / metrics["volume_arxiv_papers"], 4)
    metrics["links_in_corpus"] = _safe_count(es, INDEX_LINKS, {"term": {"in_corpus": True}})

    # ── Fraîcheur ──────────────────────────────────────────────────
    try:
        latest = es.search(index=INDEX_HN, size=1, sort=[{"time": "desc"}], _source=["time"])
        if latest["hits"]["hits"]:
            latest_time = latest["hits"]["hits"][0]["_source"]["time"]
            metrics["hn_latest_timestamp"] = datetime.fromtimestamp(int(latest_time), timezone.utc).isoformat()
            metrics["hn_lag_minutes"] = round(
                (datetime.now(timezone.utc).timestamp() - int(latest_time)) / 60, 1
            )
    except Exception:
        pass

    # ── Santé ES ───────────────────────────────────────────────────
    try:
        health = es.cluster.health()
        metrics["es_status"] = health["status"]
        metrics["es_active_shards"] = health["active_shards"]
        metrics["es_pending_tasks"] = health["number_of_pending_tasks"]
    except Exception:
        metrics["es_status"] = "unknown"

    # ── Taille des index ───────────────────────────────────────────
    try:
        stats = es.indices.stats(index=INDEX_ARXIV)
        size_bytes = stats["indices"][INDEX_ARXIV]["total"]["store"]["size_in_bytes"]
        metrics["index_size_mb"] = round(size_bytes / (1024 * 1024), 1)
    except Exception:
        metrics["index_size_mb"] = 0

    return metrics


def index_metrics(es: Elasticsearch, metrics: dict):
    """Indexe les métriques dans l'index pipeline-logs (créé avec son mapping si besoin)."""
    if not es.indices.exists(index=LOG_INDEX):
        es.indices.create(
            index=LOG_INDEX,
            settings=PIPELINE_LOGS_SETTINGS["settings"],
            mappings=PIPELINE_LOGS_SETTINGS["mappings"],
        )
    es.index(index=LOG_INDEX, document=metrics)
    es.indices.refresh(index=LOG_INDEX)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — métriques du pipeline")
    parser.add_argument("--quiet", action="store_true", help="Affichage réduit (Airflow)")
    args = parser.parse_args()

    if not args.quiet:
        print()
        print("╔══════════════════════════════════════════════════════════╗")
        print("║  SciPulse — Collecte de métriques du pipeline           ║")
        print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST, request_timeout=30)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ ES non accessible : {e}")
        sys.exit(1)

    if not args.quiet:
        print("\n📊 Collecte des métriques...")
    metrics = collect_metrics(es)

    if not args.quiet:
        print("\n  Métriques collectées :")
        for k, v in sorted(metrics.items()):
            if k != "@timestamp":
                print(f"    {k:<35} {v}")

    index_metrics(es, metrics)
    total = es.count(index=LOG_INDEX, query={"term": {"type": "pipeline_metrics"}})["count"]
    print(f"✅ Métriques indexées dans '{LOG_INDEX}' ({total} mesures au total)")

    if not args.quiet:
        print("\n💡 Exécutez ce script régulièrement pour accumuler un historique.")
        print("   Les DAGs Airflow l'appellent à la fin de chaque run (tâche collect_pipeline_metrics).")
        print()


if __name__ == "__main__":
    main()
