"""
SciPulse — Chargement des données ArXiv depuis Elasticsearch vers PostgreSQL (TP 5, partie A)
Crée un miroir tabulaire (couche « raw ») pour les transformations dbt.

Usage :
    python -m src.transformation.load_to_postgres
    python -m src.transformation.load_to_postgres --max 50000
"""

import argparse
import sys
import time

import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import execute_values

from src.config import ES_HOST, INDEX_ARXIV, PG_DSN

TABLE = "public.arxiv_papers_raw"

# Couche brute : tout en TEXT (le typage est fait par dbt dans le staging — pattern ELT)
COLUMNS = [
    "arxiv_id", "title", "abstract", "authors_flat",
    "categories", "primary_category", "date_published",
    "date_updated", "doi", "keywords", "hn_score",
]

DDL = f"""
DROP TABLE IF EXISTS {TABLE} CASCADE;
CREATE TABLE {TABLE} (
    arxiv_id         TEXT PRIMARY KEY,
    title            TEXT,
    abstract         TEXT,
    authors_flat     TEXT,
    categories       TEXT,          -- "cs.AI cs.LG stat.ML" (format d'origine ArXiv)
    primary_category TEXT,
    date_published   TEXT,
    date_updated     TEXT,
    doi              TEXT,
    keywords         TEXT,          -- mots-clés TF-IDF séparés par des virgules
    hn_score         TEXT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);
"""


def _to_text(value):
    """Convertit une valeur ES en texte PostgreSQL (listes → chaîne)."""
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        if value.is_integer():  # pandas transforme les entiers + NaN en float (74 → 74.0)
            return str(int(value))
    return str(value)


def extract_from_es(es: Elasticsearch, index: str = INDEX_ARXIV, max_docs: int = 200_000) -> pd.DataFrame:
    """Extrait les articles ArXiv depuis Elasticsearch et construit un DataFrame."""
    print("📥 Extraction depuis Elasticsearch...")
    source_fields = [c for c in COLUMNS]
    rows = []
    for hit in helpers.scan(
        es, index=index, query={"query": {"match_all": {}}}, size=2000, _source=source_fields
    ):
        src = hit["_source"]
        categories = src.get("categories") or []
        keywords = src.get("keywords") or []
        rows.append({
            "arxiv_id": src.get("arxiv_id") or hit["_id"],
            "title": src.get("title"),
            "abstract": src.get("abstract"),
            "authors_flat": src.get("authors_flat"),
            "categories": " ".join(categories) if isinstance(categories, list) else categories,
            "primary_category": src.get("primary_category"),
            "date_published": src.get("date_published"),
            "date_updated": src.get("date_updated"),
            "doi": src.get("doi"),
            "keywords": ", ".join(keywords) if isinstance(keywords, list) else keywords,
            "hn_score": src.get("hn_score"),
        })
        if len(rows) % 25_000 == 0:
            print(f"   {len(rows):>7,} extraits...")
        if len(rows) >= max_docs:
            break

    df = pd.DataFrame(rows, columns=COLUMNS)
    print(f"   ✅ {len(df):,} articles extraits depuis Elasticsearch")
    return df


def load_to_postgres(df: pd.DataFrame, dsn: str = PG_DSN):
    """Charge le DataFrame dans PostgreSQL (DROP + CREATE + INSERT par lots → idempotent)."""
    print("\n📤 Chargement dans PostgreSQL...")

    # 1. Connexion à PostgreSQL
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    # 2. Créer la table (DROP + CREATE pour l'idempotence)
    cur.execute(DDL)
    conn.commit()
    print(f"   ✅ Table '{TABLE.split('.')[-1]}' créée")

    # 3. Insérer par batch
    start = time.time()
    values = [tuple(_to_text(v) for v in row) for row in df[COLUMNS].itertuples(index=False, name=None)]
    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE} ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (arxiv_id) DO NOTHING
        """,
        values,
        page_size=1000,
    )
    conn.commit()
    elapsed = time.time() - start

    # Vérification
    cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
    count = cur.fetchone()[0]
    print(f"   ✅ {count:,} lignes insérées en {elapsed:.1f}s")

    cur.close()
    conn.close()
    return count


def main():
    parser = argparse.ArgumentParser(description="SciPulse — chargement ES → PostgreSQL")
    parser.add_argument("--max", type=int, default=200_000)
    parser.add_argument("--index", default=INDEX_ARXIV)
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Chargement ArXiv → PostgreSQL               ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 1. Connexion à Elasticsearch
    es = Elasticsearch(ES_HOST, request_timeout=120)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    # 2. Extraction des données
    df = extract_from_es(es, index=args.index, max_docs=args.max)
    if df.empty:
        print("❌ Aucun article à charger — l'index est vide.")
        sys.exit(1)

    # 3. Chargement dans PostgreSQL
    try:
        load_to_postgres(df)
    except psycopg2.OperationalError as e:
        print(f"\n❌ PostgreSQL non accessible : {e}")
        sys.exit(1)

    print("\n🎉 Données prêtes pour dbt !")
    print('   Vérifiez : docker exec -it scipulse-postgres psql -U scipulse -d scipulse '
          '-c "SELECT COUNT(*) FROM arxiv_papers_raw;"')
    print()


if __name__ == "__main__":
    main()
