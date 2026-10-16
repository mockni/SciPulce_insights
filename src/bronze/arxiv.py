"""
SciPulse — 🥉 Bronze ArXiv : dépôt du dump brut dans MinIO (bucket arxiv-raw)

Aucune transformation : chaque ligne du dump Kaggle est écrite TELLE QUELLE.
Le dump est découpé en fichiers JSON Lines de N lignes (objets S3 de taille
raisonnable), dans une partition datée :

    arxiv-raw/ingest_date=2026-10-16/part-00000.jsonl
    arxiv-raw/ingest_date=2026-10-16/part-00001.jsonl
    arxiv-raw/ingest_date=2026-10-16/_manifest.json      (source, lignes, objets, durée)

Relancer le même jour réécrit la partition (idempotence).

Source, par ordre de priorité :
  1. --source-file / ARXIV_SOURCE_FILE : fichier local (dump complet ou sous-ensemble de dev)
  2. --download : téléchargement du zip Kaggle (identifiants KAGGLE_USERNAME / KAGGLE_KEY optionnels)

Usage :
    python -m src.bronze.arxiv
    python -m src.bronze.arxiv --source-file data/arxiv-metadata-oai-snapshot.json
    python -m src.bronze.arxiv --download
"""

import argparse
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterator

import requests

from src.config import ARXIV_KAGGLE_URL, ARXIV_SOURCE_FILE, BUCKET_ARXIV_RAW, DATA_DIR
from src.lake import s3
from src.monitoring.events import emit_layer_write

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.bronze.arxiv")

CHUNK_LINES = 50_000
DUMP_MEMBER = "arxiv-metadata-oai-snapshot.json"


def download_dump(dest_dir: Path = DATA_DIR) -> Path:
    """Télécharge le zip Kaggle en flux (≈ 1,5 Go) et retourne son chemin."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "arxiv-metadata.zip"
    auth = None
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        auth = (os.environ["KAGGLE_USERNAME"], os.environ["KAGGLE_KEY"])
    log.info(f"📥 Téléchargement de {ARXIV_KAGGLE_URL}")
    with requests.get(ARXIV_KAGGLE_URL, auth=auth, stream=True, timeout=60, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for block in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(block)
    log.info(f"   ✅ {dest} ({dest.stat().st_size / 1e9:.2f} Go)")
    return dest


def iter_source_lines(path: Path) -> Iterator[str]:
    """Lignes brutes d'un fichier JSON Lines, ou du dump contenu dans un zip Kaggle."""
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            member = next((n for n in z.namelist() if n.endswith(".json")), DUMP_MEMBER)
            with z.open(member) as f:
                for raw in f:
                    line = raw.decode("utf-8").rstrip("\n")
                    if line.strip():
                        yield line
    else:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if line.strip():
                    yield line


def write_bronze(lines: Iterator[str], ingest_date: str, source_label: str,
                 chunk_lines: int = CHUNK_LINES) -> dict:
    """Écrit les lignes dans arxiv-raw/ingest_date=…/part-XXXXX.jsonl et un manifeste."""
    started = time.time()
    s3.ensure_buckets([BUCKET_ARXIV_RAW])
    prefix = f"ingest_date={ingest_date}/"
    removed = s3.delete_prefix(BUCKET_ARXIV_RAW, prefix)
    if removed:
        log.info(f"♻️  Partition {prefix} réécrite ({removed} objets remplacés)")

    chunk, part, total = [], 0, 0
    for line in lines:
        chunk.append(line)
        if len(chunk) >= chunk_lines:
            s3.put_jsonl_lines(BUCKET_ARXIV_RAW, f"{prefix}part-{part:05d}.jsonl", chunk)
            total += len(chunk)
            part += 1
            log.info(f"   📦 part-{part - 1:05d}.jsonl — {total:,} lignes")
            chunk = []
    if chunk:
        s3.put_jsonl_lines(BUCKET_ARXIV_RAW, f"{prefix}part-{part:05d}.jsonl", chunk)
        total += len(chunk)
        part += 1

    manifest = {
        "source": source_label,
        "ingest_date": ingest_date,
        "rows": total,
        "objects": part,
        "chunk_lines": chunk_lines,
        "written_at": s3.utc_now().isoformat(),
        "duration_s": round(time.time() - started, 1),
    }
    s3.put_json(BUCKET_ARXIV_RAW, f"{prefix}_manifest.json", manifest)
    emit_layer_write("bronze", "arxiv", rows=total, objects=part, started=started,
                     partition=ingest_date)
    return manifest


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Bronze ArXiv (dump brut → MinIO)")
    parser.add_argument("--source-file", type=Path, default=None)
    parser.add_argument("--download", action="store_true", help="Télécharger le dump depuis Kaggle")
    parser.add_argument("--ingest-date", default=s3.utc_now().strftime("%Y-%m-%d"))
    parser.add_argument("--chunk-lines", type=int, default=CHUNK_LINES)
    args = parser.parse_args()

    source = args.source_file or ARXIV_SOURCE_FILE
    if args.download or not Path(source).exists():
        if not args.download:
            log.info(f"Fichier local absent ({source}) → téléchargement Kaggle")
        try:
            source = download_dump()
        except requests.RequestException as e:
            log.error(f"❌ Téléchargement impossible : {e}")
            log.error("   Placez le dump (ou un sous-ensemble) dans data/ et relancez avec --source-file")
            sys.exit(1)

    log.info(f"🥉 Bronze ArXiv : {source} → s3://{BUCKET_ARXIV_RAW}/ingest_date={args.ingest_date}/")
    manifest = write_bronze(iter_source_lines(Path(source)), args.ingest_date, str(source), args.chunk_lines)
    log.info(f"🏁 {manifest['rows']:,} lignes brutes en {manifest['objects']} objets "
             f"({manifest['duration_s']} s)")
    if manifest["rows"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
