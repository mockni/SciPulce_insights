"""
SciPulse — Configuration centralisée

Toutes les adresses de services sont lues dans les variables d'environnement,
avec des valeurs par défaut valables depuis la machine hôte (Jours 1-3).

- Depuis l'hôte      : rien à définir → localhost:9200, localhost:5432, ...
- Depuis Airflow     : docker-compose définit ES_HOST=http://elasticsearch:9200,
                       PG_HOST=postgres, MINIO_ENDPOINT=http://minio:9000

Le fichier `.env` à la racine du projet est chargé s'il existe
(les variables déjà présentes dans l'environnement ne sont pas écrasées).
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:  # python-dotenv est optionnel
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:  # pragma: no cover
    pass

# ── Elasticsearch ────────────────────────────────────────────────────────────
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

INDEX_ARXIV_RAW = "arxiv-papers-raw"        # ingéré par Logstash (mapping dynamique)
INDEX_ARXIV_RAW_PY = "arxiv-papers-raw-py"  # ingéré par Python (comparaison)
INDEX_ARXIV = "arxiv-papers"                # mapping optimisé
INDEX_HN = "hn-items"
INDEX_LINKS = "arxiv-hn-links"
INDEX_PIPELINE_LOGS = "pipeline-logs"

# ── PostgreSQL (entrepôt dbt) ────────────────────────────────────────────────
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scipulse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "scipulse")
PG_DATABASE = os.getenv("PG_DATABASE", "scipulse")
PG_DSN = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} "
    f"user={PG_USER} password={PG_PASSWORD}"
)

# ── MinIO (S3) ───────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_ARXIV_RAW = "arxiv-raw"
BUCKET_HN_RAW = "hn-raw"
BUCKET_PROCESSED = "processed"

# ── Kibana ───────────────────────────────────────────────────────────────────
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")

# ── Hacker News ──────────────────────────────────────────────────────────────
HN_API_BASE = os.getenv("HN_API_BASE", "https://hacker-news.firebaseio.com/v0")
HN_POLL_INTERVAL = int(os.getenv("HN_POLL_INTERVAL", "60"))
HN_BATCH_SIZE = int(os.getenv("HN_BATCH_SIZE", "50"))

# ── Données et logs locaux ───────────────────────────────────────────────────
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
LOG_DIR = Path(os.getenv("SCIPULSE_LOG_DIR", PROJECT_ROOT / "logs"))   # lu par Filebeat (TP 8)
ARXIV_SUBSET_FILE = DATA_DIR / "arxiv-raw" / "arxiv-cs-subset-100k.json"


def get_es_client(timeout: int = 60):
    """Client Elasticsearch partagé par tous les scripts."""
    from elasticsearch import Elasticsearch

    return Elasticsearch(ES_HOST, request_timeout=timeout)
