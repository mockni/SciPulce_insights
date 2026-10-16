"""
SciPulse — 🥉 Bronze citations : API OpenAlex → MinIO (bucket citations-raw)

Pourquoi OpenAlex (plutôt que Semantic Scholar) ?
  - filtre par lot : jusqu'à 50 DOI par requête (`filter=doi:A|B|…`) → ~2 000 requêtes pour 100 000 articles ;
  - chaque article ArXiv possède un DOI DataCite `10.48550/arxiv.<id>` indexé par OpenAlex :
    on retrouve aussi les articles sans DOI d'éditeur ;
  - `counts_by_year` fournit directement l'historique des citations → vélocité de citation ;
  - données sous licence CC0 (réutilisation libre) ;
  - accès gratuit : budget quotidien sans clé, multiplié par 10 avec une clé API gratuite
    (variable OPENALEX_API_KEY, envoyée en paramètre `api_key`) ; la clé est recommandée pour
    le corpus complet, le budget sans clé suffit pour quelques milliers d'articles.
  Semantic Scholar sans clé est limité à un débit partagé très bas : trop lent pour 100 000 articles.

Pour chaque article Silver, deux DOI sont interrogés :
  - le DOI ArXiv (10.48550/arxiv.<id>)  → la prépublication ;
  - le DOI d'éditeur s'il existe        → la version publiée (souvent la plus citée).

Les réponses de l'API sont écrites TELLES QUELLES (JSON Lines), avec deux métadonnées
techniques : `_fetched_at` et `_query_dois` (lot de DOI interrogé).

    citations-raw/ingest_date=2026-10-16/part-00000.jsonl
    citations-raw/ingest_date=2026-10-16/_manifest.json

Usage :
    python -m src.bronze.citations
    python -m src.bronze.citations --max-articles 5000
"""

import argparse
import logging
import sys
import time

import requests

from src.config import (
    BUCKET_ARXIV_CLEAN,
    BUCKET_CITATIONS_RAW,
    OPENALEX_API,
    OPENALEX_API_KEY,
    OPENALEX_MAILTO,
    SILVER_ARXIV_PREFIX,
)
from src.lake import s3
from src.monitoring.events import emit_layer_write

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.bronze.citations")

DOIS_PER_REQUEST = 50             # limite des filtres OR d'OpenAlex
PER_PAGE = 200                    # maximum autorisé par l'API
MIN_INTERVAL_S = 0.12             # ≤ ~8 requêtes / s (bien sous la limite de l'API)
RECORDS_PER_FILE = 20_000
SELECT_FIELDS = ",".join([
    "id", "doi", "title", "type", "publication_year", "publication_date", "cited_by_count",
    "counts_by_year", "referenced_works_count", "primary_topic", "ids",
])

_session = requests.Session()
_last_call = 0.0


def _get(params: dict, retries: int = 5) -> dict:
    """GET /works avec limitation de débit et reprise exponentielle (429, 5xx)."""
    global _last_call
    if OPENALEX_API_KEY:
        params = {**params, "api_key": OPENALEX_API_KEY}
    if OPENALEX_MAILTO:
        params = {**params, "mailto": OPENALEX_MAILTO}
    for attempt in range(retries):
        wait = MIN_INTERVAL_S - (time.time() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.time()
        try:
            resp = _session.get(f"{OPENALEX_API}/works", params=params, timeout=30)
        except requests.RequestException as e:
            log.warning(f"   réseau ({e}) — nouvel essai")
        else:
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in (429, 500, 502, 503, 504):
                resp.raise_for_status()
            log.warning(f"   HTTP {resp.status_code} — nouvel essai")
        time.sleep(2 ** attempt)
    raise RuntimeError("OpenAlex indisponible après plusieurs essais")


def fetch_works_by_dois(dois: list[str]) -> list[dict]:
    """Toutes les œuvres correspondant au lot de DOI (pagination par curseur)."""
    works, cursor = [], "*"
    while cursor:
        page = _get({"filter": "doi:" + "|".join(dois), "per-page": PER_PAGE,
                     "select": SELECT_FIELDS, "cursor": cursor})
        results = page.get("results", [])
        works += results
        cursor = page.get("meta", {}).get("next_cursor")
        if len(results) < PER_PAGE:      # dernière page : inutile de demander la suivante (requête facturée)
            break
    return works


def dois_to_query(max_articles: int | None = None) -> list[str]:
    """DOI ArXiv + DOI d'éditeur des articles Silver (dédupliqués, ordre stable)."""
    dois, seen, articles = [], set(), 0
    for key in s3.list_keys(BUCKET_ARXIV_CLEAN, SILVER_ARXIV_PREFIX, ".parquet"):
        df = s3.read_parquet(BUCKET_ARXIV_CLEAN, key)[["arxiv_doi", "doi"]]
        for arxiv_doi, doi in df.itertuples(index=False, name=None):
            for value in (arxiv_doi, doi):
                if value and value not in seen:
                    seen.add(value)
                    dois.append(value)
            articles += 1
            if max_articles and articles >= max_articles:
                return dois
    return dois


def ingest(dois: list[str], ingest_date: str) -> dict:
    started = time.time()
    s3.ensure_buckets([BUCKET_CITATIONS_RAW])
    prefix = f"ingest_date={ingest_date}/"
    s3.delete_prefix(BUCKET_CITATIONS_RAW, prefix)

    buffer, part, total, requests_done = [], 0, 0, 0
    for i in range(0, len(dois), DOIS_PER_REQUEST):
        batch = dois[i : i + DOIS_PER_REQUEST]
        fetched_at = s3.utc_now().isoformat()
        for work in fetch_works_by_dois(batch):
            buffer.append({**work, "_fetched_at": fetched_at, "_query_dois": batch})
        requests_done += 1
        if len(buffer) >= RECORDS_PER_FILE:
            s3.put_jsonl_records(BUCKET_CITATIONS_RAW, f"{prefix}part-{part:05d}.jsonl", buffer)
            total, part, buffer = total + len(buffer), part + 1, []
        if requests_done % 100 == 0:
            log.info(f"   {i + len(batch):,} / {len(dois):,} DOI interrogés — {total + len(buffer):,} œuvres")
    if buffer:
        s3.put_jsonl_records(BUCKET_CITATIONS_RAW, f"{prefix}part-{part:05d}.jsonl", buffer)
        total, part = total + len(buffer), part + 1

    manifest = {"source": f"{OPENALEX_API}/works", "ingest_date": ingest_date, "dois_queried": len(dois),
                "rows": total, "objects": part, "requests": requests_done,
                "duration_s": round(time.time() - started, 1)}
    s3.put_json(BUCKET_CITATIONS_RAW, f"{prefix}_manifest.json", manifest)
    emit_layer_write("bronze", "citations", rows=total, objects=part, started=started, partition=ingest_date)
    return manifest


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Bronze citations (OpenAlex → MinIO)")
    parser.add_argument("--max-articles", type=int, default=None, help="Limiter le nombre d'articles (dev)")
    parser.add_argument("--ingest-date", default=s3.utc_now().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    dois = dois_to_query(args.max_articles)
    if not dois:
        log.error("❌ Aucun article Silver : lancez d'abord le pipeline ArXiv (Bronze → Silver)")
        sys.exit(1)
    log.info(f"🥉 Bronze citations : {len(dois):,} DOI à interroger sur OpenAlex")
    try:
        manifest = ingest(dois, args.ingest_date)
    except (RuntimeError, requests.RequestException) as e:
        log.error(f"❌ {e}")
        sys.exit(1)
    log.info(f"🏁 {manifest['rows']:,} œuvres OpenAlex en {manifest['objects']} fichiers "
             f"({manifest['requests']} requêtes, {manifest['duration_s']} s)")


if __name__ == "__main__":
    main()
