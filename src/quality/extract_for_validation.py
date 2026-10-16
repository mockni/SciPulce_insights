"""
SciPulse — Extraction de données depuis Elasticsearch vers Pandas
pour validation par Great Expectations.

Great Expectations n'a pas de connecteur natif Elasticsearch.
La stratégie est d'extraire un échantillon dans un DataFrame Pandas,
puis de valider le DataFrame.

Un échantillon de 10 000 documents est statistiquement suffisant
pour détecter les problèmes de qualité et beaucoup plus rapide
à traiter que l'intégralité du corpus.

Usage :
    python -m src.quality.extract_for_validation
    python -m src.quality.extract_for_validation --max 50000
"""

import argparse
import sys

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST

ARXIV_COLUMNS = [
    "arxiv_id", "title", "abstract", "authors_flat", "categories", "primary_category",
    "date_published", "date_updated", "doi", "keywords", "hn_score",
]
HN_COLUMNS = [
    "hn_id", "type", "by", "time", "title", "url", "text", "score", "descendants", "arxiv_id_linked",
]


def extract_arxiv(
    es: Elasticsearch,
    index: str = "arxiv-papers",
    max_docs: int = 10_000,
) -> pd.DataFrame:
    """
    Extrait un échantillon d'articles ArXiv depuis Elasticsearch.

    Paramètres
    ----------
    es : Elasticsearch
        Client Elasticsearch connecté.
    index : str
        Nom de l'index à interroger.
    max_docs : int
        Nombre maximum de documents à extraire.

    Retourne
    --------
    pd.DataFrame
        DataFrame avec les colonnes : arxiv_id, title, abstract,
        authors_flat, categories, primary_category, date_published,
        date_updated, doi, keywords, hn_score.
    """
    docs = []

    for hit in helpers.scan(
        es,
        index=index,
        query={"query": {"match_all": {}}},
        size=1000,
        preserve_order=False,
    ):
        source = hit["_source"]
        docs.append({
            "arxiv_id":         source.get("arxiv_id"),
            "title":            source.get("title"),
            "abstract":         source.get("abstract"),
            "authors_flat":     source.get("authors_flat"),
            "categories":       source.get("categories"),
            "primary_category": source.get("primary_category"),
            "date_published":   source.get("date_published"),
            "date_updated":     source.get("date_updated"),
            "doi":              source.get("doi"),
            "keywords":         source.get("keywords"),
            "hn_score":         source.get("hn_score"),
        })

        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs, columns=ARXIV_COLUMNS)
    print(f"✅ Extrait {len(df):,} articles ArXiv depuis '{index}'")
    print(f"   Colonnes : {list(df.columns)}")
    print(f"   Mémoire  : {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} Mo")
    return df


def extract_hn(
    es: Elasticsearch,
    index: str = "hn-items",
    max_docs: int = 5_000,
) -> pd.DataFrame:
    """
    Extrait les items Hacker News depuis Elasticsearch.

    Paramètres
    ----------
    es : Elasticsearch
        Client Elasticsearch connecté.
    index : str
        Nom de l'index HN.
    max_docs : int
        Nombre maximum de documents à extraire.

    Retourne
    --------
    pd.DataFrame
        DataFrame avec les colonnes : hn_id, type, by, time,
        title, url, text, score, descendants, arxiv_id_linked.
    """
    docs = []

    for hit in helpers.scan(
        es,
        index=index,
        query={"query": {"match_all": {}}},
        size=500,
    ):
        source = hit["_source"]
        docs.append({
            "hn_id":            source.get("hn_id"),
            "type":             source.get("type"),
            "by":               source.get("by"),
            "time":             source.get("time"),
            "title":            source.get("title"),
            "url":              source.get("url"),
            "text":             source.get("text"),
            "score":            source.get("score"),
            "descendants":      source.get("descendants"),
            "arxiv_id_linked":  source.get("arxiv_id_linked"),
        })

        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs, columns=HN_COLUMNS)
    print(f"✅ Extrait {len(df):,} items HN depuis '{index}'")
    return df


def extract_arxiv_from_files(pattern: str, max_docs: int = 10_000) -> pd.DataFrame:
    """
    Échantillon des fichiers JSON lines AVANT ingestion (validation « shift-left »,
    tâche validate_raw_arxiv du DAG). Les articles reçoivent la même transformation
    que dans Logstash ; les lignes illisibles sont comptées dans `json_errors`.
    """
    import glob
    import json

    from src.ingestion.arxiv_bulk_ingest import transform_article
    from src.utils.arxiv_utils import clean_text

    docs, json_errors = [], 0
    for path in sorted(glob.glob(pattern)):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    json_errors += 1
                    continue
                doc = transform_article(raw)
                docs.append({**{c: doc.get(c) for c in ARXIV_COLUMNS},
                             "authors_flat": clean_text(raw.get("authors"))})
                if len(docs) >= max_docs:
                    break
        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs, columns=ARXIV_COLUMNS)
    df.attrs["json_errors"] = json_errors
    print(f"✅ Extrait {len(df):,} articles depuis {pattern} ({json_errors} lignes JSON invalides)")
    return df


def print_summary(df: pd.DataFrame, name: str):
    """Affiche un résumé du DataFrame pour inspection rapide."""
    print(f"\n{'─' * 60}")
    print(f"  RÉSUMÉ — {name}")
    print(f"{'─' * 60}")
    print(f"  Lignes   : {len(df):,}")
    print(f"  Colonnes : {len(df.columns)}")
    print()

    # Nulls par colonne
    nulls = df.isnull().sum()
    if nulls.any():
        print("  Valeurs nulles :")
        for col in df.columns:
            n = nulls[col]
            pct = n / len(df) * 100
            if n > 0:
                print(f"    ⚠️  {col:<25} {n:>6,} nulls ({pct:.2f}%)")
            else:
                print(f"    ✅  {col:<25} {n:>6,} nulls")
    print()

    # Types
    print("  Types des colonnes :")
    for col in df.columns:
        print(f"    {col:<25} {df[col].dtype}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SciPulse — Extraction ES → Pandas")
    parser.add_argument("--max", type=int, default=10_000, help="Max documents ArXiv (défaut: 10000)")
    args = parser.parse_args()

    es = Elasticsearch(ES_HOST)

    try:
        es.info()
    except Exception as e:
        print(f"❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Extraction pour validation                  ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # ArXiv
    print("\n📥 Extraction ArXiv...\n")
    df_arxiv = extract_arxiv(es, max_docs=args.max)
    print_summary(df_arxiv, "ArXiv")

    # HN
    print("📥 Extraction Hacker News...\n")
    df_hn = extract_hn(es)
    print_summary(df_hn, "Hacker News")

    # Aperçu
    print("─" * 60)
    print("  APERÇU — 3 premiers articles ArXiv")
    print("─" * 60)
    print(df_arxiv[["arxiv_id", "title", "primary_category"]].head(3).to_string(index=False))
    print()
