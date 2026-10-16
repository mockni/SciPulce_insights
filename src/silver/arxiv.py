"""
SciPulse — 🥈 Silver ArXiv : Bronze (JSON Lines) → nettoyage → Parquet (bucket arxiv-clean)

Lit la partition Bronze la plus récente (ou --ingest-date), applique les règles de
src/silver/cleaning.py fichier par fichier (mémoire bornée, même sur le dump complet)
et écrit :

    arxiv-clean/papers/part-00000.parquet …     (une part Silver par part Bronze)
    arxiv-clean/_state/last_run.json             (lignage : partition source, compteurs)

La sortie est réécrite entièrement à chaque exécution (idempotence).
Les validations Great Expectations sont des tâches séparées du DAG :
  - avant : src.quality.validate_arxiv --layer bronze (diagnostic, non bloquant)
  - après : src.quality.validate_arxiv --layer silver (bloquant pour la couche Gold)

Usage :
    python -m src.silver.arxiv
    python -m src.silver.arxiv --max 20000          # sous-ensemble pour le développement
"""

import argparse
import logging
import sys
import time
from collections import Counter

from src.config import ARXIV_SCOPE_PREFIXES, BUCKET_ARXIV_CLEAN, BUCKET_ARXIV_RAW, SILVER_ARXIV_PREFIX
from src.lake import s3
from src.monitoring.events import emit, emit_layer_write
from src.silver.cleaning import clean_arxiv, parse_jsonl
from src.silver.schemas import ARXIV_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.silver.arxiv")


def latest_ingest_date() -> str | None:
    partitions = s3.list_partitions(BUCKET_ARXIV_RAW, "ingest_date")
    return partitions[-1] if partitions else None


def build_silver(ingest_date: str, max_rows: int | None = None, scope=ARXIV_SCOPE_PREFIXES) -> dict:
    started = time.time()
    s3.ensure_buckets([BUCKET_ARXIV_CLEAN])
    bronze_keys = s3.list_keys(BUCKET_ARXIV_RAW, f"ingest_date={ingest_date}/", ".jsonl")
    if not bronze_keys:
        raise FileNotFoundError(f"Aucune donnée Bronze dans s3://{BUCKET_ARXIV_RAW}/ingest_date={ingest_date}/")

    s3.delete_prefix(BUCKET_ARXIV_CLEAN, SILVER_ARXIV_PREFIX)
    report = Counter()
    seen: set[str] = set()
    written, parts = 0, 0

    for key in bronze_keys:
        records = parse_jsonl(s3.iter_jsonl_lines(BUCKET_ARXIV_RAW, key), report)
        df = clean_arxiv(records, scope_prefixes=scope, report=report)
        # doublons répartis sur plusieurs fichiers Bronze : première occurrence conservée
        cross = df["arxiv_id"].isin(seen)
        report["duplicates_across_files"] += int(cross.sum())
        df = df[~cross]
        if max_rows is not None:
            df = df.head(max(0, max_rows - written))
        if df.empty:
            continue
        seen.update(df["arxiv_id"])
        s3.put_parquet(BUCKET_ARXIV_CLEAN, f"{SILVER_ARXIV_PREFIX}part-{parts:05d}.parquet", df, ARXIV_SCHEMA)
        written += len(df)
        parts += 1
        log.info(f"   🥈 {key} → part-{parts - 1:05d}.parquet ({len(df):,} articles, total {written:,})")
        if max_rows is not None and written >= max_rows:
            log.info(f"   Limite --max {max_rows:,} atteinte")
            break

    summary = {
        "source_partition": f"s3://{BUCKET_ARXIV_RAW}/ingest_date={ingest_date}/",
        "rows": written,
        "objects": parts,
        "rules": dict(report),
        "written_at": s3.utc_now().isoformat(),
        "duration_s": round(time.time() - started, 1),
    }
    s3.put_json(BUCKET_ARXIV_CLEAN, "_state/last_run.json", summary)
    emit_layer_write("silver", "arxiv", rows=written, objects=parts, started=started, partition=ingest_date)
    emit("cleaning_report", layer="silver", source="arxiv", **{f"rule_{k}": v for k, v in report.items()})
    return summary


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Silver ArXiv (Bronze → Parquet nettoyé)")
    parser.add_argument("--ingest-date", help="Partition Bronze (défaut : la plus récente)")
    parser.add_argument("--max", type=int, default=None, help="Nombre maximal d'articles (dev)")
    args = parser.parse_args()

    ingest_date = args.ingest_date or latest_ingest_date()
    if not ingest_date:
        log.error(f"❌ Aucune partition dans s3://{BUCKET_ARXIV_RAW}/ — lancez d'abord src.bronze.arxiv")
        sys.exit(1)

    log.info(f"🥈 Silver ArXiv depuis ingest_date={ingest_date} (périmètre : {', '.join(ARXIV_SCOPE_PREFIXES)})")
    summary = build_silver(ingest_date, args.max)

    log.info("📋 Règles de nettoyage appliquées :")
    for rule, count in sorted(summary["rules"].items()):
        log.info(f"     {rule:<28} {count:>10,}")
    log.info(f"🏁 {summary['rows']:,} articles Silver en {summary['objects']} fichiers Parquet "
             f"({summary['duration_s']} s)")
    if summary["rows"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
