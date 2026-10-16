"""
SciPulse — Instantané de l'état du pipeline (dashboard de monitoring)

Publie dans pipeline-logs :
  - un document `layer_snapshot` par bucket MinIO (Bronze / Silver) :
      objets, taille, fraîcheur (minutes depuis la dernière écriture)
  - un document `layer_snapshot` par index Gold : documents, taille, fraîcheur
  - un document `pipeline_metrics` : santé du cluster Elasticsearch

Les DAGs l'appellent à la fin de chaque run (trigger_rule=all_done).

Usage :
    python -m src.monitoring.pipeline_metrics
"""

import argparse
import sys
from datetime import datetime

from elasticsearch import Elasticsearch

from src.config import ALL_BUCKETS, ES_HOST, INDEX_ARXIV, INDEX_ENRICHED, LAYER_OF_BUCKET
from src.lake import s3
from src.monitoring import events


def bucket_snapshot(bucket: str) -> dict:
    layer, source = LAYER_OF_BUCKET[bucket]
    objects = s3.list_objects(bucket)
    snap = {"layer": layer, "source": source, "bucket": bucket, "objects": len(objects),
            "size_mb": round(sum(o["Size"] for o in objects) / 1e6, 2)}
    if objects:
        last = max(o["LastModified"] for o in objects)
        snap["last_write"] = last.isoformat()
        snap["freshness_minutes"] = round((s3.utc_now() - last).total_seconds() / 60, 1)
    return snap


def gold_snapshot(es: Elasticsearch, index: str, source: str, date_field: str | None) -> dict | None:
    if not es.indices.exists(index=index):
        return None
    stats = es.indices.stats(index=index)["indices"][index]["total"]
    snap = {"layer": "gold", "source": source, "bucket": index,
            "objects": es.count(index=index)["count"],
            "size_mb": round(stats["store"]["size_in_bytes"] / 1e6, 2)}
    if date_field is None:
        return snap
    agg = es.search(index=index, size=0, aggs={"last": {"max": {"field": date_field}}})
    last = agg["aggregations"]["last"].get("value_as_string")
    if last:
        snap["last_write"] = last
        last_ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
        snap["freshness_minutes"] = round((s3.utc_now() - last_ts).total_seconds() / 60, 1)
    return snap


def collect(es: Elasticsearch | None) -> list[dict]:
    snapshots = []
    for bucket in ALL_BUCKETS:
        try:
            snapshots.append(bucket_snapshot(bucket))
        except Exception as e:   # bucket absent ou MinIO injoignable
            snapshots.append({"layer": LAYER_OF_BUCKET[bucket][0], "source": LAYER_OF_BUCKET[bucket][1],
                              "bucket": bucket, "objects": 0, "error": str(e)[:200]})
    if es is not None:
        # Gold : la fraîcheur se mesure sur la date d'écriture (enriched_at, posée par Spark).
        # arxiv-papers n'a pas de date d'indexation : sa fraîcheur vient des événements layer_write.
        for index, source, field in [(INDEX_ARXIV, "arxiv", None), (INDEX_ENRICHED, "enriched", "enriched_at")]:
            snap = gold_snapshot(es, index, source, field)
            if snap:
                snapshots.append(snap)
    return snapshots


def main():
    argparse.ArgumentParser(description="SciPulse — métriques du pipeline").parse_args()
    es = Elasticsearch(ES_HOST, request_timeout=30)
    try:
        health = es.cluster.health()
    except Exception as e:
        print(f"❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    snapshots = collect(es)
    print(f"{'Couche':<8} {'Source':<10} {'Bucket / index':<24} {'Objets':>9} {'Mo':>9} {'Fraîcheur (min)':>16}")
    for snap in snapshots:
        events.emit("layer_snapshot", **snap)
        print(f"{snap['layer']:<8} {snap['source']:<10} {snap['bucket']:<24} {snap.get('objects', 0):>9,} "
              f"{snap.get('size_mb', 0):>9} {snap.get('freshness_minutes', '—'):>16}")
    events.emit("pipeline_metrics", es_status=health["status"], es_active_shards=health["active_shards"],
                es_pending_tasks=health["number_of_pending_tasks"])
    print(f"Elasticsearch : {health['status']}")


if __name__ == "__main__":
    main()
