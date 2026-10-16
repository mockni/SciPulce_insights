"""
SciPulse — Chargement de l'ArXiv Silver (Parquet MinIO) dans PostgreSQL pour dbt

Crée la table brute public.arxiv_papers_raw (toutes colonnes TEXT : le typage est
fait par dbt dans le modèle de staging — pattern ELT), source du projet dbt.

Usage :
    python -m src.transformation.load_to_postgres
"""

import argparse
import sys
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.config import BUCKET_ARXIV_CLEAN, PG_DSN, SILVER_ARXIV_PREFIX
from src.lake import s3

TABLE = "public.arxiv_papers_raw"
COLUMNS = [
    "arxiv_id", "title", "abstract", "authors_flat", "categories", "primary_category",
    "date_published", "date_updated", "doi",
]

DDL = f"""
DROP TABLE IF EXISTS {TABLE} CASCADE;
CREATE TABLE {TABLE} (
    arxiv_id         TEXT PRIMARY KEY,
    title            TEXT,
    abstract         TEXT,
    authors_flat     TEXT,
    categories       TEXT,          -- "cs.AI cs.LG stat.ML"
    primary_category TEXT,
    date_published   TEXT,          -- ISO 8601
    date_updated     TEXT,
    doi              TEXT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);
"""


def to_text(value):
    """Valeur Silver → texte PostgreSQL (listes jointes par des espaces, dates ISO)."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        return None if pd.isna(value) else (str(int(value)) if value.is_integer() else str(value))
    if hasattr(value, "tolist") and not isinstance(value, str):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value)
    return str(value)


def rows_from_silver():
    for key in s3.list_keys(BUCKET_ARXIV_CLEAN, SILVER_ARXIV_PREFIX, ".parquet"):
        df = s3.read_parquet(BUCKET_ARXIV_CLEAN, key)
        for row in df[COLUMNS].itertuples(index=False, name=None):
            yield tuple(to_text(v) for v in row)


def load(dsn: str = PG_DSN) -> int:
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(DDL)
    batch = []
    for row in rows_from_silver():
        batch.append(row)
        if len(batch) >= 5000:
            execute_values(cur, f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s "
                                "ON CONFLICT (arxiv_id) DO NOTHING", batch, page_size=1000)
            batch = []
    if batch:
        execute_values(cur, f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s "
                            "ON CONFLICT (arxiv_id) DO NOTHING", batch, page_size=1000)
    conn.commit()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def main():
    argparse.ArgumentParser(description="SciPulse — ArXiv Silver → PostgreSQL (dbt)").parse_args()
    started = time.time()
    print(f"📤 Chargement de s3://{BUCKET_ARXIV_CLEAN}/{SILVER_ARXIV_PREFIX} → {TABLE}")
    try:
        count = load()
    except psycopg2.OperationalError as e:
        print(f"❌ PostgreSQL non accessible : {e}")
        sys.exit(1)
    print(f"   ✅ {count:,} lignes chargées en {time.time() - started:.1f} s — prêt pour dbt")
    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
