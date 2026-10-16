"""
SciPulse — Chargement d'échantillons Bronze / Silver depuis MinIO pour la validation et l'analyse.

Great Expectations n'a pas de connecteur MinIO : on charge un échantillon dans un
DataFrame pandas, puis on valide le DataFrame (mode programmatique de GX).
"""

from collections import Counter

import pandas as pd

from src.config import (
    BUCKET_ARXIV_CLEAN,
    BUCKET_ARXIV_RAW,
    BUCKET_CITATIONS_CLEAN,
    BUCKET_CITATIONS_RAW,
    BUCKET_HN_CLEAN,
    BUCKET_HN_RAW,
    SILVER_ARXIV_PREFIX,
    SILVER_CITATIONS_PREFIX,
    SILVER_HN_PREFIX,
)
from src.lake import s3
from src.silver.cleaning import arxiv_bronze_frame, hn_bronze_frame, parse_jsonl


def _bronze_records(bucket: str, prefix: str, max_rows: int, newest_first: bool = False,
                    report: Counter | None = None) -> list[dict]:
    report = report if report is not None else Counter()
    keys = s3.list_keys(bucket, prefix, ".jsonl")
    if newest_first:
        keys = list(reversed(keys))
    records = []
    for key in keys:
        records += parse_jsonl(s3.iter_jsonl_lines(bucket, key), report)
        if len(records) >= max_rows:
            break
    return records[:max_rows]


def latest_partition(bucket: str, name: str) -> str | None:
    values = s3.list_partitions(bucket, name)
    return values[-1] if values else None


def arxiv_bronze_sample(max_rows: int = 20_000) -> pd.DataFrame:
    """Échantillon de la partition Bronze ArXiv la plus récente (champs bruts)."""
    partition = latest_partition(BUCKET_ARXIV_RAW, "ingest_date")
    if not partition:
        return arxiv_bronze_frame([])
    report = Counter()
    df = arxiv_bronze_frame(_bronze_records(BUCKET_ARXIV_RAW, f"ingest_date={partition}/", max_rows,
                                            report=report))
    df.attrs["json_errors"] = report["json_errors"]
    df.attrs["partition"] = partition
    return df


def hn_bronze_sample(max_rows: int = 20_000) -> pd.DataFrame:
    """Items HN bruts des fichiers les plus récents."""
    report = Counter()
    df = hn_bronze_frame(_bronze_records(BUCKET_HN_RAW, "date=", max_rows, newest_first=True, report=report))
    df.attrs["json_errors"] = report["json_errors"]
    return df


def citations_bronze_records(max_rows: int = 20_000) -> list[dict]:
    partition = latest_partition(BUCKET_CITATIONS_RAW, "ingest_date")
    if not partition:
        return []
    return _bronze_records(BUCKET_CITATIONS_RAW, f"ingest_date={partition}/", max_rows)


def arxiv_silver_sample(max_rows: int = 20_000) -> pd.DataFrame:
    return s3.read_parquet_prefix(BUCKET_ARXIV_CLEAN, SILVER_ARXIV_PREFIX, max_rows)


def hn_silver_sample(max_rows: int = 50_000) -> pd.DataFrame:
    return s3.read_parquet_prefix(BUCKET_HN_CLEAN, SILVER_HN_PREFIX, max_rows)


def citations_silver_sample(max_rows: int = 50_000) -> pd.DataFrame:
    return s3.read_parquet_prefix(BUCKET_CITATIONS_CLEAN, SILVER_CITATIONS_PREFIX, max_rows)
