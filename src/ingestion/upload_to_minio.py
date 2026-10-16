"""
SciPulse — Upload des données ArXiv vers MinIO (TP 1, étape 5)
Dépose le fichier complet puis le découpe en chunks dans le bucket arxiv-raw.

Usage :
    python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-cs-subset-100k.json
    python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-cs-subset-100k.json --chunk-size 5000
    python -m src.ingestion.upload_to_minio --list            # vérification (étape 5.5)
    python -m src.ingestion.upload_to_minio --download arxiv-cs-subset-100k.json --dest data/arxiv-raw
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from src.config import (
    BUCKET_ARXIV_RAW,
    LOG_DIR,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

BUCKET = BUCKET_ARXIV_RAW
LOG_FILE = LOG_DIR / "upload_minio.log"

# ── Logging : console + fichier (bonne pratique : garder une trace des chargements)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("scipulse.minio")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_fh)


# ── Connexion ────────────────────────────────────────────────────────────────

def get_s3_client():
    """Client S3 pointant sur MinIO (signature v4, path-style)."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


s3 = get_s3_client()


def ensure_bucket(bucket: str = BUCKET):
    """Crée le bucket s'il n'existe pas (normalement fait par minio-init)."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket {bucket} créé")


def upload_file(local_path: str, object_name: str | None = None, bucket: str = BUCKET) -> str:
    """Upload un fichier vers MinIO. Retourne la clé de l'objet."""
    object_name = object_name or Path(local_path).name
    s3.upload_file(str(local_path), bucket, object_name)  # multipart automatique pour les gros fichiers
    size_mb = os.path.getsize(local_path) / 1024 / 1024
    print(f"  ✅ {Path(local_path).name} → s3://{bucket}/{object_name} ({size_mb:.1f} Mo)")
    logger.info(f"upload {local_path} -> s3://{bucket}/{object_name} ({size_mb:.1f} Mo)")
    return object_name


def _upload_chunk(lines: list[str], object_name: str, bucket: str = BUCKET):
    """Upload un chunk en mémoire vers MinIO (JSON lines)."""
    body = ("\n".join(lines) + "\n").encode("utf-8")
    s3.put_object(Bucket=bucket, Key=object_name, Body=body, ContentType="application/x-ndjson")
    print(f"  ✅ {object_name} ({len(lines)} articles)")
    logger.info(f"chunk s3://{bucket}/{object_name} ({len(lines)} articles)")


def split_and_upload(
    source_file: str,
    chunk_size: int = 10_000,
    prefix: str = "chunks/",
    bucket: str = BUCKET,
) -> tuple[int, int]:
    """
    Découpe un fichier JSON lines en chunks et les upload vers MinIO.
    Chaque chunk contient `chunk_size` articles. Retourne (nb_chunks, nb_articles).
    """
    chunk: list[str] = []
    chunk_idx = 0
    total = 0

    with open(source_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            chunk.append(line)
            if len(chunk) >= chunk_size:
                _upload_chunk(chunk, f"{prefix}arxiv-chunk-{chunk_idx:04d}.json", bucket)
                total += len(chunk)
                chunk_idx += 1
                chunk = []

    if chunk:  # dernier chunk incomplet
        _upload_chunk(chunk, f"{prefix}arxiv-chunk-{chunk_idx:04d}.json", bucket)
        total += len(chunk)
        chunk_idx += 1

    return chunk_idx, total


def list_bucket(bucket: str = BUCKET, prefix: str = ""):
    """Liste le contenu du bucket (vérification programmatique, étape 5.5)."""
    paginator = s3.get_paginator("list_objects_v2")
    n, size = 0, 0
    print(f"📂 s3://{bucket}/{prefix}")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            n += 1
            size += obj["Size"]
            print(f"  {obj['Key']:<45} {obj['Size'] / 1024 / 1024:>8.2f} Mo")
    print(f"  → {n} objet(s), {size / 1024 / 1024:.1f} Mo au total")


def download_object(object_name: str, dest_dir: str, bucket: str = BUCKET) -> str:
    """Rapatrie un objet de la landing zone vers le disque (utilisé par le DAG Airflow)."""
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    dest = str(Path(dest_dir) / Path(object_name).name)
    s3.download_file(bucket, object_name, dest)
    print(f"  ✅ s3://{bucket}/{object_name} → {dest}")
    logger.info(f"download s3://{bucket}/{object_name} -> {dest}")
    return dest


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Upload ArXiv vers MinIO")
    parser.add_argument("source", nargs="?", default="data/arxiv-raw/arxiv-cs-subset-100k.json")
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--no-full", action="store_true", help="Ne pas uploader le fichier complet")
    parser.add_argument("--list", action="store_true", help="Lister le bucket et quitter")
    parser.add_argument("--download", metavar="OBJET", help="Télécharger un objet du bucket et quitter")
    parser.add_argument("--dest", default="data/arxiv-raw", help="Dossier de destination pour --download")
    args = parser.parse_args()

    try:
        if args.list:
            list_bucket()
            return
        if args.download:
            try:
                download_object(args.download, args.dest)
            except ClientError as e:
                print(f"❌ Objet introuvable : s3://{BUCKET}/{args.download} ({e})")
                sys.exit(1)
            return

        # 1) Erreur si la source n'existe pas
        if not os.path.exists(args.source):
            print(f"❌ Fichier non trouvé : {args.source}")
            print("   Usage : python -m src.ingestion.upload_to_minio <chemin_fichier>")
            sys.exit(1)

        # 2) Informations de chargement
        print(f"📤 Upload de {args.source} vers MinIO (bucket: {BUCKET})")
        print(f"   Endpoint: {MINIO_ENDPOINT}\n")
        logger.info(f"début upload {args.source} vers {MINIO_ENDPOINT}/{BUCKET}")
        ensure_bucket()

        # 3) Upload du fichier complet (copie brute immuable)
        if not args.no_full:
            upload_file(args.source)

        # 4) Découpage en chunks
        print(f"\n📦 Découpage en chunks de {args.chunk_size:,} articles...")
        n_chunks, n_articles = split_and_upload(args.source, chunk_size=args.chunk_size)
        print(f"\n🏁 {n_chunks} chunks uploadés ({n_articles} articles au total)")
        logger.info(f"fin upload : {n_chunks} chunks, {n_articles} articles")

    except EndpointConnectionError:
        print(f"❌ MinIO injoignable sur {MINIO_ENDPOINT} — la stack Docker tourne-t-elle ?")
        sys.exit(1)


if __name__ == "__main__":
    main()
