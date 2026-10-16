"""
SciPulse — 🥈 Silver Hacker News : Bronze (JSON Lines) → nettoyage → Parquet (bucket hn-clean)

Traitement incrémental (exécuté toutes les 5 minutes par le DAG HN) :
  1. liste les fichiers Bronze pas encore traités (état : hn-clean/_state/processed.json) ;
  2. les nettoie (src/silver/cleaning.py) ;
  3. fusionne avec le snapshot Silver existant et déduplique (mesure la plus récente gardée) ;
  4. réécrit le snapshot : hn-clean/items/hn-items.parquet

Usage :
    python -m src.silver.hn
    python -m src.silver.hn --full-refresh      # retraite tout le Bronze
"""

import argparse
import logging
import time
from collections import Counter

import pandas as pd

from src.config import BUCKET_HN_CLEAN, BUCKET_HN_RAW, SILVER_HN_PREFIX
from src.lake import s3
from src.monitoring.events import emit, emit_layer_write
from src.silver.cleaning import HN_SILVER_COLUMNS, clean_hn, dedupe_hn, parse_jsonl, typed_hn
from src.silver.schemas import HN_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.silver.hn")

SNAPSHOT_KEY = f"{SILVER_HN_PREFIX}hn-items.parquet"
STATE_KEY = "_state/processed.json"


def build_silver(full_refresh: bool = False) -> dict:
    started = time.time()
    s3.ensure_buckets([BUCKET_HN_CLEAN])
    state = {"processed": []} if full_refresh else s3.get_json(BUCKET_HN_CLEAN, STATE_KEY, {"processed": []})
    processed = set(state["processed"])
    new_keys = [k for k in s3.list_keys(BUCKET_HN_RAW, "date=", ".jsonl") if k not in processed]
    log.info(f"📂 {len(new_keys)} fichier(s) Bronze à traiter ({len(processed)} déjà traités)")

    report = Counter()
    items = []
    for key in new_keys:
        items += parse_jsonl(s3.iter_jsonl_lines(BUCKET_HN_RAW, key), report)
    new_df = clean_hn(items, report)

    existing = pd.DataFrame(columns=HN_SILVER_COLUMNS)
    if not full_refresh and SNAPSHOT_KEY in s3.list_keys(BUCKET_HN_CLEAN, SILVER_HN_PREFIX):
        existing = s3.read_parquet(BUCKET_HN_CLEAN, SNAPSHOT_KEY)

    frames = [typed_hn(f[HN_SILVER_COLUMNS]) for f in (existing, new_df) if not f.empty]
    snapshot = dedupe_hn(pd.concat(frames, ignore_index=True)) if frames else typed_hn(existing)

    if new_keys or full_refresh:
        s3.put_parquet(BUCKET_HN_CLEAN, SNAPSHOT_KEY, snapshot, HN_SCHEMA)
        s3.put_json(BUCKET_HN_CLEAN, STATE_KEY,
                    {"processed": sorted(processed | set(new_keys)), "updated_at": s3.utc_now().isoformat()})

    summary = {
        "new_bronze_files": len(new_keys),
        "rows": len(snapshot),
        "new_rows": len(new_df),
        "stories": int((snapshot["type"] == "story").sum()) if len(snapshot) else 0,
        "arxiv_links": int(snapshot["arxiv_id_linked"].notna().sum()) if len(snapshot) else 0,
        "rules": dict(report),
    }
    emit_layer_write("silver", "hn", rows=summary["new_rows"], objects=1 if new_keys else 0,
                     started=started, snapshot_rows=summary["rows"])
    if report:
        emit("cleaning_report", layer="silver", source="hn", **{f"rule_{k}": v for k, v in report.items()})
    return summary


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Silver Hacker News")
    parser.add_argument("--full-refresh", action="store_true", help="Retraiter tout le Bronze")
    args = parser.parse_args()

    summary = build_silver(args.full_refresh)
    for rule, count in sorted(summary["rules"].items()):
        log.info(f"     {rule:<22} {count:>8,}")
    log.info(f"🏁 Snapshot Silver : {summary['rows']:,} items ({summary['stories']:,} stories, "
             f"{summary['arxiv_links']} liens ArXiv) — {summary['new_rows']} nouveaux")


if __name__ == "__main__":
    main()
