"""
SciPulse — Configuration centralisée

Toutes les adresses de services sont lues dans les variables d'environnement,
avec des valeurs par défaut valables depuis la machine hôte.

- Depuis l'hôte   : rien à définir → localhost:9200, localhost:5432, localhost:9000…
- Depuis Airflow  : docker-compose.yml définit ES_HOST=http://elasticsearch:9200,
                    PG_HOST=postgres, MINIO_ENDPOINT=http://minio:9000, SPARK_MASTER…

Le fichier `.env` à la racine est chargé s'il existe (sans écraser l'environnement).
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:  # python-dotenv est optionnel
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:  # pragma: no cover
    pass

# ═════════════════════════════════════════════════════════════════════════════
#  DATA LAKE MEDALLION (MinIO, compatible S3)
# ═════════════════════════════════════════════════════════════════════════════
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# 🥉 Bronze : données brutes, fidèles à la source (JSON Lines)
BUCKET_ARXIV_RAW = "arxiv-raw"
BUCKET_HN_RAW = "hn-raw"
BUCKET_CITATIONS_RAW = "citations-raw"
# 🥈 Silver : données nettoyées, validées, normalisées par source (Parquet)
BUCKET_ARXIV_CLEAN = "arxiv-clean"
BUCKET_HN_CLEAN = "hn-clean"
BUCKET_CITATIONS_CLEAN = "citations-clean"

ALL_BUCKETS = [
    BUCKET_ARXIV_RAW, BUCKET_HN_RAW, BUCKET_CITATIONS_RAW,
    BUCKET_ARXIV_CLEAN, BUCKET_HN_CLEAN, BUCKET_CITATIONS_CLEAN,
]
LAYER_OF_BUCKET = {
    BUCKET_ARXIV_RAW: ("bronze", "arxiv"), BUCKET_HN_RAW: ("bronze", "hn"),
    BUCKET_CITATIONS_RAW: ("bronze", "citations"),
    BUCKET_ARXIV_CLEAN: ("silver", "arxiv"), BUCKET_HN_CLEAN: ("silver", "hn"),
    BUCKET_CITATIONS_CLEAN: ("silver", "citations"),
}

# Préfixes des jeux Silver (un « dossier » Parquet par source, lu tel quel par Spark)
SILVER_ARXIV_PREFIX = "papers/"
SILVER_HN_PREFIX = "items/"
SILVER_CITATIONS_PREFIX = "works/"

# ═════════════════════════════════════════════════════════════════════════════
#  ELASTICSEARCH / KIBANA (couche Gold = serving uniquement)
# ═════════════════════════════════════════════════════════════════════════════
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")

INDEX_ARXIV = "arxiv-papers"                    # Module 1 : Gold partielle (ArXiv Silver)
INDEX_ENRICHED = "arxiv-papers-enriched"        # Module 2 : Gold finale (écrite par Spark)
INDEX_PIPELINE_LOGS = "pipeline-logs"           # observabilité (hors données métier)

# ═════════════════════════════════════════════════════════════════════════════
#  POSTGRESQL (métastore Airflow + entrepôt dbt)
# ═════════════════════════════════════════════════════════════════════════════
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scipulse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "scipulse")
PG_DATABASE = os.getenv("PG_DATABASE", "scipulse")
PG_DSN = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} "
    f"user={PG_USER} password={PG_PASSWORD}"
)

# ═════════════════════════════════════════════════════════════════════════════
#  SOURCES
# ═════════════════════════════════════════════════════════════════════════════
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
LOG_DIR = Path(os.getenv("SCIPULSE_LOG_DIR", PROJECT_ROOT / "logs"))

# ArXiv : fichier local (dump complet ou sous-ensemble de dev) ; sinon téléchargement Kaggle
ARXIV_SOURCE_FILE = Path(os.getenv("ARXIV_SOURCE_FILE", DATA_DIR / "arxiv-cs-subset-100k.json"))
ARXIV_KAGGLE_URL = os.getenv(
    "ARXIV_KAGGLE_URL", "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"
)
ARXIV_SCOPE_PREFIXES = tuple(os.getenv("ARXIV_SCOPE_PREFIXES", "cs.,stat.ML").split(","))

# Hacker News
HN_API_BASE = os.getenv("HN_API_BASE", "https://hacker-news.firebaseio.com/v0")
HN_BATCH_SIZE = int(os.getenv("HN_BATCH_SIZE", "50"))

# OpenAlex (source de citations, Module 2)
OPENALEX_API = os.getenv("OPENALEX_API", "https://api.openalex.org")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")  # clé gratuite : budget quotidien × 10
OPENALEX_MAILTO = os.getenv("OPENALEX_MAILTO", "")    # contact identifiant le client (bonne pratique)

# ═════════════════════════════════════════════════════════════════════════════
#  SPARK (Module 2)
# ═════════════════════════════════════════════════════════════════════════════
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
# Racine des chemins lus par Spark : s3a:// (MinIO) en production, file:// en test
LAKE_URI = os.getenv("LAKE_URI", "s3a://")


def get_es_client(timeout: int = 60):
    """Client Elasticsearch partagé par tous les scripts."""
    from elasticsearch import Elasticsearch

    return Elasticsearch(ES_HOST, request_timeout=timeout)
