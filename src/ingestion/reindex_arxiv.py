"""
SciPulse — Réindexation ArXiv : de arxiv-papers-raw vers arxiv-papers (TP 3, étape 3.2)
Transforme les documents pendant la copie (parsing des auteurs, dates, champs dérivés…).

Usage :
    python -m src.ingestion.reindex_arxiv
    python -m src.ingestion.reindex_arxiv --yes                 # sans confirmation (Airflow)
    python -m src.ingestion.reindex_arxiv --source arxiv-papers-raw-py
"""

import argparse
import sys
import time

from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST, INDEX_ARXIV, INDEX_ARXIV_RAW
from src.utils.arxiv_utils import (
    clean_text,
    domain_of,
    first_version_date,
    parse_authors,
    parse_update_date,
    split_categories,
)
from src.utils.mappings import ARXIV_SETTINGS, create_index

SOURCE_INDEX = INDEX_ARXIV_RAW
DEST_INDEX = INDEX_ARXIV
BATCH_SIZE = 500


def _as_es_date(value):
    """Accepte '2023-02-07' ou '2023-02-07T00:00:00.000Z' (déjà parsé par Logstash)."""
    if not value:
        return None
    value = str(value)
    return value if "T" in value else parse_update_date(value)


def transform_for_reindex(hit: dict) -> dict:
    """
    Transforme un document source (arxiv-papers-raw) en document destination (arxiv-papers).
    Seuls les champs déclarés dans le mapping sont conservés.
    """
    # 1. Récupérer la source du document
    src = hit.get("_source", {})

    # 2. Parser les auteurs : authors_parsed (fiable) sinon heuristique sur la chaîne
    authors, authors_flat = parse_authors(src.get("authors"), src.get("authors_parsed"))

    # 3. Reconstruire la source du document
    categories = split_categories(src.get("categories"))
    primary = src.get("primary_category") or (categories[0] if categories else None)
    abstract = clean_text(src.get("abstract")) or None
    title = clean_text(src.get("title")) or None
    date_updated = _as_es_date(src.get("date_updated") or src.get("update_date"))
    date_published = (
        _as_es_date(src.get("date_published"))
        or first_version_date(src.get("versions"))
        or date_updated
    )
    versions = src.get("versions")

    doc = {
        "arxiv_id": src.get("arxiv_id") or src.get("id"),
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "authors_flat": authors_flat,
        "submitter": src.get("submitter"),
        "categories": categories,
        "primary_category": primary,
        "domain": domain_of(primary),
        "date_published": date_published,
        "date_updated": date_updated,
        "doi": src.get("doi"),
        "journal_ref": src.get("journal-ref") or src.get("journal_ref"),
        "versions_count": len(versions) if isinstance(versions, list) else None,
        "abstract_length": len(abstract) if abstract else 0,
    }
    return {k: v for k, v in doc.items() if v is not None}


def scroll_source(es: Elasticsearch, index: str = SOURCE_INDEX) -> int:
    """Compte les documents dans l'index source."""
    return es.count(index=index)["count"]


def generate_actions(es: Elasticsearch, source: str = SOURCE_INDEX, dest: str = DEST_INDEX):
    """Générateur : lit l'index source (scroll) et produit des actions pour l'index destination."""
    # 1. Scanner l'index entier de manière efficace (helpers.scan = scroll API)
    for hit in helpers.scan(
        es,
        index=source,
        query={"query": {"match_all": {}}},
        size=1000,
        preserve_order=False,
    ):
        # 1.1 Transformer le document
        doc = transform_for_reindex(hit)

        # 1.2 Ignorer l'article s'il ne possède pas d'id ArXiv
        if not doc.get("arxiv_id"):
            continue

        # 1.3 Retourner une action bulk (générateur)
        yield {"_index": dest, "_id": doc["arxiv_id"], "_source": doc}


def main():
    parser = argparse.ArgumentParser(description="SciPulse — réindexation ArXiv")
    parser.add_argument("--source", default=SOURCE_INDEX)
    parser.add_argument("--dest", default=DEST_INDEX)
    parser.add_argument("--yes", "-y", action="store_true", help="Vider l'index destination sans confirmation")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Réindexation ArXiv (mapping optimisé)       ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    es = Elasticsearch(ES_HOST, request_timeout=120)

    # Vérifications
    try:
        es.info()
        print("✅ Connecté à Elasticsearch")
    except Exception as e:
        print(f"❌ Connexion impossible : {e}")
        sys.exit(1)

    # 1. Vérifier que l'index d'origine existe
    if not es.indices.exists(index=args.source):
        print(f"❌ L'index source '{args.source}' n'existe pas — lancez d'abord l'ingestion (TP 2).")
        sys.exit(1)

    # 2. Vérifier que l'index de destination existe (sinon le créer avec le bon mapping)
    if not es.indices.exists(index=args.dest):
        print(f"⚠️  L'index '{args.dest}' n'existe pas — création avec le mapping optimisé.")
        create_index(es, args.dest, ARXIV_SETTINGS)

    source_count = scroll_source(es, args.source)
    print(f"📂 Source : '{args.source}' — {source_count:,} documents")
    print(f"📂 Dest.  : '{args.dest}'")

    # 3. Si la destination contient déjà des articles : on repart d'un index propre
    dest_count = es.count(index=args.dest)["count"]
    if dest_count > 0:
        print(f"\n⚠️  L'index '{args.dest}' contient déjà {dest_count:,} documents.")
        if not args.yes:
            if not sys.stdin.isatty():
                print("   Mode non interactif : relancez avec --yes pour confirmer.")
                sys.exit(1)
            answer = input("   Le vider et le recréer (les enrichissements seront perdus) ? [o/N] ")
            if answer.strip().lower() not in ("o", "oui", "y", "yes"):
                print("   Abandon.")
                sys.exit(0)
        create_index(es, args.dest, ARXIV_SETTINGS, recreate=True)

    # Réindexation
    print("\n🔄 Réindexation en cours...\n")

    start_time = time.time()
    success = 0
    errors = 0

    for ok, result in helpers.streaming_bulk(
        es,
        generate_actions(es, args.source, args.dest),
        chunk_size=BATCH_SIZE,
        raise_on_error=False,
        max_retries=3,
    ):
        if ok:
            success += 1
        else:
            errors += 1
            if errors <= 3:
                print(f"   ❌ {result}")

        total = success + errors
        if total % 10_000 == 0:
            elapsed = time.time() - start_time
            rate = total / elapsed if elapsed > 0 else 0
            pct = total / source_count * 100 if source_count > 0 else 0
            print(f"   {pct:5.1f}%  {total:>7,} / {source_count:,}  ({rate:,.0f} docs/sec)")

    elapsed = time.time() - start_time
    es.indices.refresh(index=args.dest)
    final_count = es.count(index=args.dest)["count"]

    print(f"\n{'═' * 50}")
    print(f"  ✅ Réindexés  : {success:>10,}")
    print(f"  ❌ Erreurs    : {errors:>10,}")
    print(f"  ⏱  Durée      : {elapsed:>10.1f} sec")
    print(f"  🔍 Vérification : {final_count:,} documents dans '{args.dest}'")
    print(f"{'═' * 50}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
