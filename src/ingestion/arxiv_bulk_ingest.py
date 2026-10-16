"""
SciPulse — Ingestion batch ArXiv via Python + elasticsearch-py (TP 2, partie B)
Alternative au pipeline Logstash pour comparer les approches.

Produit exactement les mêmes documents que docker/logstash/pipeline/arxiv.conf
(mêmes noms de champs, mêmes formats de date) dans l'index `arxiv-papers-raw-py`.

Usage :
    python -m src.ingestion.arxiv_bulk_ingest data/arxiv-raw/arxiv-cs-subset-100k.json
    python -m src.ingestion.arxiv_bulk_ingest --from-minio            # lit les chunks du bucket arxiv-raw
    python -m src.ingestion.arxiv_bulk_ingest <fichier> --index arxiv-papers-raw --keep-index
"""

import argparse
import json
import os
import sys
import time

from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST, INDEX_ARXIV_RAW_PY, LOG_DIR
from src.utils.arxiv_utils import (
    clean_text,
    first_version_date,
    parse_update_date,
    split_categories,
)

# ── Configuration ────────────────────────────────────────────────────────────

INDEX_NAME = INDEX_ARXIV_RAW_PY   # Index séparé pour comparer avec Logstash
BATCH_SIZE = 1000                 # Nombre de documents par requête bulk
DEAD_LETTER_FILE = str(LOG_DIR / "arxiv_bulk_ingest.dead-letter.jsonl")


def transform_article(raw: dict) -> dict:
    """
    Transforme un article ArXiv brut en document prêt à indexer.
    Équivalent Python des filtres Logstash (mutate, date, split, ruby).
    """
    doc = dict(raw)  # on conserve les champs non transformés (comme Logstash)

    # 0. Renommage (mutate rename)
    doc["arxiv_id"] = doc.pop("id", None)
    update_date = doc.pop("update_date", None)

    # 1. Éclatement des catégories (mutate split) + catégorie primaire (ruby)
    categories = split_categories(raw.get("categories"))
    doc["categories"] = categories
    if categories:
        doc["primary_category"] = categories[0]

    # 2. Nettoyage de l'abstract (mutate gsub + strip)
    if raw.get("abstract") is not None:
        doc["abstract"] = raw["abstract"].replace("\n", " ").strip()

    # 3. Nettoyage du titre (le titre ArXiv contient aussi des retours à la ligne)
    if raw.get("title") is not None:
        doc["title"] = clean_text(raw["title"])

    # 4. Dates (filtre date) : date_updated + date_published (v1 de l'article)
    doc["date_updated"] = parse_update_date(update_date)
    date_published = first_version_date(raw.get("versions"))
    doc["date_published"] = date_published or doc["date_updated"]

    # Comme Logstash : un champ calculé vide n'est pas créé
    return {k: v for k, v in doc.items() if v is not None or k in raw}


def iter_lines_from_file(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            yield f"{os.path.basename(filepath)}:{line_number}", line


def iter_lines_from_minio(prefix: str = "chunks/"):
    """Lit les chunks JSON lines déposés dans MinIO (landing zone) au TP 1."""
    from src.ingestion.upload_to_minio import BUCKET, s3

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
            print(f"  📦 s3://{BUCKET}/{obj['Key']}")
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"]
            for line_number, raw_line in enumerate(body.iter_lines(), 1):
                yield f"{obj['Key']}:{line_number}", raw_line.decode("utf-8")


def generate_actions(lines, index: str = INDEX_NAME, stats: dict | None = None):
    """
    Générateur qui lit des lignes JSON et produit des actions bulk ES.
    Utilise un générateur (yield) pour ne pas charger tout le fichier en mémoire.
    """
    stats = stats if stats is not None else {}
    os.makedirs(os.path.dirname(DEAD_LETTER_FILE), exist_ok=True)

    with open(DEAD_LETTER_FILE, "w", encoding="utf-8") as dead_letter:
        for location, line in lines:
            line = line.strip()
            if not line:
                continue

            # 1. Lire la ligne JSON, en traitant les erreurs
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                stats["json_errors"] = stats.get("json_errors", 0) + 1
                dead_letter.write(json.dumps({"location": location, "error": str(e), "line": line[:500]}) + "\n")
                continue

            # 2. Normaliser l'article
            doc = transform_article(raw)

            # 3. Vérification minimale : l'arxiv_id doit exister
            if not doc.get("arxiv_id"):
                stats["missing_id"] = stats.get("missing_id", 0) + 1
                continue

            # Action bulk au format ES — _id = arxiv_id → ingestion idempotente
            yield {
                "_index": index,
                "_id": doc["arxiv_id"],
                "_source": doc,
            }


def ingest(lines, index: str = INDEX_NAME, recreate: bool = True, source_label: str = ""):
    """Ingestion bulk dans Elasticsearch."""

    # 1. Connexion à Elasticsearch
    es = Elasticsearch(ES_HOST, request_timeout=120)

    # 2. Vérification de la connexion
    try:
        info = es.info()
        print(f"✅ Connecté à Elasticsearch ({info['cluster_name']})")
    except Exception as e:
        print(f"❌ Impossible de se connecter à Elasticsearch ({ES_HOST}) : {e}")
        sys.exit(1)

    # 3. Suppression de l'index existant (pour repartir de zéro)
    if recreate and es.indices.exists(index=index):
        es.indices.delete(index=index)
        print(f"🗑  Index '{index}' supprimé")

    # 4. Ingestion via l'API bulk
    print(f"\n📥 Ingestion de {source_label} → index '{index}'")
    print(f"   Taille des lots : {BATCH_SIZE} documents\n")

    # 4.1 Variables de traçage
    start_time = time.time()
    success_count = 0
    error_count = 0
    stats: dict = {}

    # 4.2 Ingestion — streaming_bulk découpe le générateur en lots de chunk_size
    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(lines, index=index, stats=stats),
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
        max_retries=3,
    ):
        if ok:
            success_count += 1
        else:
            error_count += 1
            if error_count <= 3:
                print(f"  ❌ {result}")

        processed = success_count + error_count
        if processed % 10_000 == 0:
            elapsed = time.time() - start_time
            print(f"  📊 {processed:>7,} documents traités ({processed / elapsed:,.0f} docs/sec)")

    # 5. Bilan
    elapsed = time.time() - start_time
    rate = success_count / elapsed if elapsed > 0 else 0

    print(f"\n{'═' * 50}")
    print(f"  ✅ Succès  : {success_count:>7,}")
    print(f"  ❌ Erreurs : {error_count:>7,}")
    if stats:
        print(f"  ⚠️  Ignorés : {stats}  (voir {DEAD_LETTER_FILE})")
    print(f"  ⏱  Durée   : {elapsed:.1f} secondes")
    print(f"  🚀 Débit   : {rate:,.0f} documents/seconde")
    print(f"{'═' * 50}")

    # 6. Rafraîchir l'index pour que les documents soient cherchables
    es.indices.refresh(index=index)

    # 7. Vérification finale
    count = es.count(index=index)["count"]
    print(f"\n🔍 Vérification : {count:,} documents dans l'index '{index}'")
    return success_count, error_count


def main():
    parser = argparse.ArgumentParser(description="SciPulse — ingestion bulk ArXiv")
    parser.add_argument("filepath", nargs="?", default="data/arxiv-raw/arxiv-cs-subset-100k.json")
    parser.add_argument("--from-minio", action="store_true", help="Lire les chunks depuis MinIO (bucket arxiv-raw)")
    parser.add_argument("--index", default=INDEX_NAME)
    parser.add_argument("--keep-index", action="store_true", help="Ne pas supprimer l'index avant ingestion")
    args = parser.parse_args()

    if args.from_minio:
        lines = iter_lines_from_minio()
        label = "s3://arxiv-raw/chunks/"
    else:
        if not os.path.exists(args.filepath):
            print(f"❌ Fichier non trouvé : {args.filepath}")
            print("   Usage : python -m src.ingestion.arxiv_bulk_ingest <chemin_fichier>")
            sys.exit(1)
        lines = iter_lines_from_file(args.filepath)
        label = args.filepath

    ingest(lines, index=args.index, recreate=not args.keep_index, source_label=label)


if __name__ == "__main__":
    main()
