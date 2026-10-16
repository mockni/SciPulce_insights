"""
SciPulse — 🥈 Silver citations : Bronze OpenAlex → normalisation → Parquet (bucket citations-clean)

Normalisation des clés de jointure (point de raccordement des trois sources) :
  - doi       : minuscules, sans préfixe https://doi.org/
  - arxiv_id  : déduit du DOI DataCite d'ArXiv (10.48550/arxiv.<id>), sans suffixe de version
  - match_key_type : « arxiv_doi » (prépublication) ou « journal_doi » (version éditeur)

Une même œuvre OpenAlex renvoyée par plusieurs lots est dédupliquée (relevé le plus récent).

Usage :
    python -m src.silver.citations
"""

import argparse
import logging
import sys
import time
from collections import Counter

import pandas as pd

from src.config import BUCKET_CITATIONS_CLEAN, BUCKET_CITATIONS_RAW, SILVER_CITATIONS_PREFIX
from src.lake import s3
from src.monitoring.events import emit, emit_layer_write
from src.silver.cleaning import parse_jsonl
from src.silver.schemas import CITATIONS_SCHEMA
from src.utils.arxiv_utils import arxiv_id_from_doi, clean_text, normalize_doi

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.silver.citations")

CITATIONS_COLUMNS = [f.name for f in CITATIONS_SCHEMA]


def _display(topic: dict | None, level: str) -> str | None:
    node = (topic or {}).get(level) or {}
    return node.get("display_name")


def clean_work(work: dict) -> dict:
    doi = normalize_doi(work.get("doi") or (work.get("ids") or {}).get("doi"))
    arxiv_id = arxiv_id_from_doi(doi)
    topic = work.get("primary_topic")
    return {
        "openalex_id": str(work.get("id") or "").rsplit("/", 1)[-1] or None,   # W2963403868
        "doi": doi,
        "arxiv_id": arxiv_id,
        "match_key_type": "arxiv_doi" if arxiv_id else ("journal_doi" if doi else None),
        "title": clean_text(work.get("title")) or None,
        "work_type": work.get("type"),
        "publication_year": work.get("publication_year"),
        "publication_date": work.get("publication_date"),
        "cited_by_count": work.get("cited_by_count"),
        "citations_by_year": [
            {"year": int(c["year"]), "cited_by_count": int(c.get("cited_by_count") or 0)}
            for c in (work.get("counts_by_year") or []) if c.get("year") is not None
        ],
        "referenced_works_count": work.get("referenced_works_count"),
        "field": _display(topic, "field"),
        "subfield": _display(topic, "subfield"),
        "oa_domain": _display(topic, "domain"),
        "fetched_at": pd.to_datetime(work.get("_fetched_at"), utc=True, errors="coerce"),
    }


def clean_citations(works: list[dict], report: Counter | None = None) -> pd.DataFrame:
    report = report if report is not None else Counter()
    rows = []
    for work in works:
        report["input"] += 1
        row = clean_work(work)
        if not row["openalex_id"]:
            report["missing_id"] += 1
            continue
        if not row["doi"]:
            report["missing_doi"] += 1        # inexploitable pour la jointure
            continue
        if row["cited_by_count"] is None or row["cited_by_count"] < 0:
            report["invalid_cited_by_count"] += 1
            row["cited_by_count"] = None
        rows.append(row)
    df = pd.DataFrame(rows, columns=CITATIONS_COLUMNS)
    before = len(df)
    df = (df.sort_values("fetched_at", na_position="first")
            .drop_duplicates(subset="openalex_id", keep="last")
            .sort_values("openalex_id").reset_index(drop=True))
    report["duplicates"] += before - len(df)
    report["arxiv_doi_rows"] += int((df["match_key_type"] == "arxiv_doi").sum())
    report["journal_doi_rows"] += int((df["match_key_type"] == "journal_doi").sum())
    report["output"] += len(df)
    df["publication_year"] = pd.to_numeric(df["publication_year"], errors="coerce").astype("Int32")
    for col in ("cited_by_count", "referenced_works_count"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    return df


def latest_ingest_date() -> str | None:
    values = s3.list_partitions(BUCKET_CITATIONS_RAW, "ingest_date")
    return values[-1] if values else None


def build_silver(ingest_date: str) -> dict:
    started = time.time()
    s3.ensure_buckets([BUCKET_CITATIONS_CLEAN])
    report = Counter()
    works = []
    for key in s3.list_keys(BUCKET_CITATIONS_RAW, f"ingest_date={ingest_date}/", ".jsonl"):
        works += parse_jsonl(s3.iter_jsonl_lines(BUCKET_CITATIONS_RAW, key), report)
    df = clean_citations(works, report)
    s3.delete_prefix(BUCKET_CITATIONS_CLEAN, SILVER_CITATIONS_PREFIX)
    s3.put_parquet(BUCKET_CITATIONS_CLEAN, f"{SILVER_CITATIONS_PREFIX}works.parquet", df, CITATIONS_SCHEMA)
    emit_layer_write("silver", "citations", rows=len(df), objects=1, started=started, partition=ingest_date)
    emit("cleaning_report", layer="silver", source="citations", **{f"rule_{k}": v for k, v in report.items()})
    return {"rows": len(df), "rules": dict(report)}


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Silver citations OpenAlex")
    parser.add_argument("--ingest-date")
    args = parser.parse_args()
    ingest_date = args.ingest_date or latest_ingest_date()
    if not ingest_date:
        log.error(f"❌ Aucune partition dans s3://{BUCKET_CITATIONS_RAW}/ — lancez src.bronze.citations")
        sys.exit(1)
    summary = build_silver(ingest_date)
    for rule, count in sorted(summary["rules"].items()):
        log.info(f"     {rule:<24} {count:>9,}")
    log.info(f"🏁 {summary['rows']:,} œuvres Silver")
    if summary["rows"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
