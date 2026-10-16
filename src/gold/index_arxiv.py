"""
SciPulse — 🥇 Gold partielle (Module 1) : ArXiv Silver (Parquet) → index Elasticsearch arxiv-papers

Lit les fichiers Parquet de arxiv-clean/papers/ et les indexe en bulk.
_id = arxiv_id : relancer le job remplace les documents au lieu de les dupliquer.

Usage :
    python -m src.gold.index_arxiv
    python -m src.gold.index_arxiv --recreate     # recrée l'index (après un changement de mapping)
"""

import argparse
import logging
import math
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.config import BUCKET_ARXIV_CLEAN, ES_HOST, INDEX_ARXIV, SILVER_ARXIV_PREFIX
from src.lake import s3
from src.monitoring.events import emit_layer_write
from src.utils.mappings import ARXIV_PROPERTIES, ARXIV_SETTINGS, create_index

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.gold.arxiv")
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

BATCH_SIZE = 1000


def to_json_value(value):
    """Valeurs pandas / numpy / pyarrow → types JSON acceptés par Elasticsearch."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, float) and math.isnan(value):
        return None
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, dict):
        return {k: to_json_value(v) for k, v in value.items()}
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):   # numpy array / scalaire
        return to_json_value(value.tolist())
    if isinstance(value, (list, tuple)):
        return [to_json_value(v) for v in value]
    return value


def to_document(row: dict) -> dict:
    """Une ligne Silver → document Gold (seuls les champs du mapping sont envoyés)."""
    doc = {k: to_json_value(row.get(k)) for k in ARXIV_PROPERTIES if k in row}
    return {k: v for k, v in doc.items() if v is not None}


def generate_actions(index: str = INDEX_ARXIV):
    for key in s3.list_keys(BUCKET_ARXIV_CLEAN, SILVER_ARXIV_PREFIX, ".parquet"):
        df = s3.read_parquet(BUCKET_ARXIV_CLEAN, key)
        log.info(f"   📄 {key} — {len(df):,} articles")
        for row in df.to_dict(orient="records"):
            doc = to_document(row)
            yield {"_index": index, "_id": doc["arxiv_id"], "_source": doc}


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Gold ArXiv (Silver → Elasticsearch)")
    parser.add_argument("--recreate", action="store_true")
    args = parser.parse_args()

    started = time.time()
    es = Elasticsearch(ES_HOST, request_timeout=120)
    try:
        es.info()
    except Exception as e:
        log.error(f"❌ Elasticsearch non accessible : {e}")
        sys.exit(1)
    create_index(es, INDEX_ARXIV, ARXIV_SETTINGS, recreate=args.recreate)

    success, errors = 0, 0
    for ok, result in helpers.streaming_bulk(es, generate_actions(), chunk_size=BATCH_SIZE,
                                             raise_on_error=False, max_retries=3):
        if ok:
            success += 1
        else:
            errors += 1
            if errors <= 3:
                log.error(f"   ❌ {result}")
        if (success + errors) % 20_000 == 0:
            log.info(f"   {success + errors:,} documents envoyés")

    es.indices.refresh(index=INDEX_ARXIV)
    count = es.count(index=INDEX_ARXIV)["count"]
    emit_layer_write("gold", "arxiv", rows=success, objects=1, started=started, index=INDEX_ARXIV,
                     errors=errors)
    log.info(f"🏁 {success:,} documents indexés, {errors} erreurs — {count:,} dans '{INDEX_ARXIV}'")
    if errors or success == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
