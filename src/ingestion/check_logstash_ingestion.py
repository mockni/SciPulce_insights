"""
SciPulse — Attente / vérification de l'ingestion Logstash (tâche `ingest_arxiv_logstash`, TP 7)

Logstash tourne en continu dans son conteneur et surveille /data/arxiv-raw/.
Cette tâche attend que l'index arxiv-papers-raw contienne (presque) autant de
documents qu'il y a d'articles dans les fichiers, ou que le compteur se stabilise.

Si Logstash n'a rien indexé à l'expiration du délai (conteneur arrêté, fichier
déposé avant son démarrage…), l'option --fallback-python ingère les fichiers
avec le pipeline Python équivalent (src/ingestion/arxiv_bulk_ingest.py).

Usage :
    python -m src.ingestion.check_logstash_ingestion --timeout 900 --fallback-python
"""

import argparse
import glob
import sys
import time

from elasticsearch import Elasticsearch

from src.config import DATA_DIR, ES_HOST, INDEX_ARXIV_RAW


def count_lines(pattern: str) -> int:
    total = 0
    for path in glob.glob(pattern):
        with open(path, "rb") as f:
            total += sum(1 for line in f if line.strip())
    return total


def current_count(es: Elasticsearch) -> int:
    if not es.indices.exists(index=INDEX_ARXIV_RAW):
        return 0
    es.indices.refresh(index=INDEX_ARXIV_RAW)
    return es.count(index=INDEX_ARXIV_RAW)["count"]


def main():
    parser = argparse.ArgumentParser(description="SciPulse — vérification de l'ingestion Logstash")
    parser.add_argument("--timeout", type=int, default=900, help="Délai maximal en secondes")
    parser.add_argument("--interval", type=int, default=15)
    parser.add_argument("--min-ratio", type=float, default=0.99,
                        help="Part minimale des lignes à retrouver dans l'index")
    parser.add_argument("--fallback-python", action="store_true",
                        help="Ingérer en Python si Logstash n'a rien indexé")
    args = parser.parse_args()

    pattern = str(DATA_DIR / "arxiv-raw" / "*.json")
    expected = count_lines(pattern)
    print(f"📄 {expected:,} articles dans {pattern}")
    if expected == 0:
        print("❌ Aucun fichier à ingérer.")
        sys.exit(1)

    es = Elasticsearch(ES_HOST, request_timeout=60)
    start = time.time()
    previous, stable = -1, 0
    count = 0
    while time.time() - start < args.timeout:
        count = current_count(es)
        print(f"   {time.strftime('%H:%M:%S')} — {count:,} / {expected:,} documents dans {INDEX_ARXIV_RAW}")
        if count >= expected * args.min_ratio:
            print(f"✅ Ingestion Logstash confirmée : {count:,} documents")
            return
        stable = stable + 1 if count == previous and count > 0 else 0
        if stable >= 4:  # plus aucune progression depuis 4 relevés
            print(f"⚠️  Compteur stabilisé à {count:,} (< {args.min_ratio:.0%} de {expected:,}) — "
                  "des lignes ont probablement été rejetées, voir : docker compose logs logstash")
            return
        previous = count
        time.sleep(args.interval)

    if count == 0 and args.fallback_python:
        print("⚠️  Logstash n'a rien indexé — repli sur le pipeline Python (même transformation).")
        from src.ingestion.arxiv_bulk_ingest import ingest, iter_lines_from_file

        def all_lines():
            for path in sorted(glob.glob(pattern)):
                yield from iter_lines_from_file(path)

        ok, errors = ingest(all_lines(), index=INDEX_ARXIV_RAW, recreate=False, source_label=pattern)
        sys.exit(0 if ok > 0 and errors == 0 else 1)

    print("❌ Logstash n'a pas indexé suffisamment de documents dans le délai imparti.")
    print("   Vérifiez les logs : docker compose logs logstash")
    sys.exit(1)


if __name__ == "__main__":
    main()
