"""
SciPulse — Hacker News Poller (TP 3, partie B)
Polling de l'API Firebase HN et indexation dans Elasticsearch.

Usage :
    python -m src.ingestion.hn_poller                          # un seul cycle (100 dernières stories)
    python -m src.ingestion.hn_poller --continuous --interval 30
    python -m src.ingestion.hn_poller --feed top --limit 200 --with-comments 5
    python -m src.ingestion.hn_poller --archive                # copie brute des items dans MinIO (hn-raw)
"""

import argparse
import html
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from elasticsearch import Elasticsearch, helpers

from src.config import (
    ES_HOST,
    HN_API_BASE,
    HN_BATCH_SIZE,
    HN_POLL_INTERVAL,
    INDEX_ARXIV,
    INDEX_HN,
    INDEX_LINKS,
    LOG_DIR,
)
from src.utils.arxiv_utils import ARXIV_URL_PATTERN, extract_arxiv_ids

LOG_FILE = LOG_DIR / "hn_poller.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger("scipulse.hn_poller")
logging.getLogger("elastic_transport").setLevel(logging.WARNING)

# ── Configuration ────────────────────────────────────────────────────────────

ES_INDEX_HN = INDEX_HN
ES_INDEX_LINKS = INDEX_LINKS
BATCH_SIZE = HN_BATCH_SIZE
POLL_INTERVAL = HN_POLL_INTERVAL  # secondes
FEEDS = {"new": "newstories", "top": "topstories", "best": "beststories"}
HTTP_WORKERS = 8                  # requêtes HN en parallèle (l'API tolère ~30 req/s)

_session = requests.Session()


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST, request_timeout=60)


def fetch_new_story_ids(feed: str = "new") -> list[int]:
    """Récupère les 500 derniers IDs de stories (ou top/best) depuis l'API HN."""
    resp = _session.get(f"{HN_API_BASE}/{FEEDS[feed]}.json", timeout=10)
    resp.raise_for_status()
    return resp.json() or []


def fetch_item(item_id: int) -> dict | None:
    """Récupère le détail d'un item HN."""
    try:
        resp = _session.get(f"{HN_API_BASE}/item/{item_id}.json", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Erreur fetch item {item_id}: {e}")
        return None


def fetch_items(ids: list[int]) -> list[dict]:
    """Récupère plusieurs items en parallèle (les items supprimés/vides sont ignorés)."""
    with ThreadPoolExecutor(max_workers=HTTP_WORKERS) as pool:
        items = list(pool.map(fetch_item, ids))
    return [it for it in items if it and it.get("id") and not it.get("deleted") and not it.get("dead")]


def extract_arxiv_id(url: str | None) -> str | None:
    """Extrait l'ID ArXiv (sans suffixe de version) depuis une URL si elle pointe vers arxiv.org."""
    if not url:
        return None
    match = ARXIV_URL_PATTERN.search(url)
    return match.group("id") if match else None


def classify_post(item: dict) -> str:
    """Type éditorial d'un post : show_hn, ask_hn, launch_hn, article, text, job, comment…"""
    item_type = item.get("type", "story")
    if item_type != "story":
        return item_type
    title = (item.get("title") or "").lower()
    if title.startswith("show hn"):
        return "show_hn"
    if title.startswith("ask hn"):
        return "ask_hn"
    if title.startswith("launch hn"):
        return "launch_hn"
    return "article" if item.get("url") else "text"


def url_domain(url: str | None) -> str | None:
    if not url:
        return None
    netloc = urlparse(url).netloc.lower()
    return netloc[4:] if netloc.startswith("www.") else netloc or None


def item_exists(es: Elasticsearch, item_id: int) -> bool:
    """Vérifie si un item HN est déjà indexé."""
    return bool(es.exists(index=ES_INDEX_HN, id=str(item_id)))


def existing_ids(es: Elasticsearch, ids: list[int]) -> set[str]:
    """Version « batch » de item_exists : un seul appel _mget au lieu de N requêtes."""
    if not ids or not es.indices.exists(index=ES_INDEX_HN):
        return set()
    resp = es.mget(index=ES_INDEX_HN, ids=[str(i) for i in ids], source=False)
    return {d["_id"] for d in resp["docs"] if d.get("found")}


def prepare_hn_doc(item: dict, story_id: str | None = None) -> dict:
    """Transforme un item HN brut en document ES."""
    doc = {
        "hn_id": str(item["id"]),
        "type": item.get("type", "story"),
        "post_type": classify_post(item),
        "by": item.get("by"),
        "time": item.get("time"),
        "title": item.get("title"),
        "url": item.get("url"),
        "domain": url_domain(item.get("url")),
        "text": item.get("text"),
        "score": item.get("score", 0) if item.get("type") != "comment" else None,
        "descendants": item.get("descendants", 0) if item.get("type") != "comment" else None,
        "parent": str(item["parent"]) if item.get("parent") else None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Relation parent-child pour les commentaires (le parent du join = la story racine)
    if item.get("type") == "comment" and item.get("parent"):
        root = story_id or str(item["parent"])
        doc["story_id"] = root
        doc["relation"] = {"name": "comment", "parent": root}
    else:
        doc["story_id"] = doc["hn_id"]
        doc["relation"] = {"name": "story"}

    # Détection de lien ArXiv (URL du post, sinon texte d'un Ask HN / commentaire)
    arxiv_id = extract_arxiv_id(item.get("url"))
    link_source = "url"
    if not arxiv_id:
        # le texte HN est en HTML échappé (« https:&#x2F;&#x2F;arxiv.org… ») → on le décode
        ids_in_text = extract_arxiv_ids(html.unescape(item.get("text") or ""))
        arxiv_id = ids_in_text[0] if ids_in_text else None
        link_source = "text"
    if arxiv_id:
        doc["arxiv_id_linked"] = arxiv_id
        doc["_link_source"] = link_source

    return {k: v for k, v in doc.items() if v is not None}


def build_link_doc(doc: dict, in_corpus: bool | None = None) -> dict:
    """Document de l'index arxiv-hn-links (un lien = un couple arxiv_id × item HN)."""
    link = {
        "arxiv_id": doc["arxiv_id_linked"],
        "hn_item_id": doc["hn_id"],
        "hn_score": doc.get("score", 0),
        "hn_comments": doc.get("descendants", 0),
        "hn_title": doc.get("title"),
        "hn_url": doc.get("url"),
        "hn_time": doc.get("time"),
        "link_source": doc.get("_link_source", "url"),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    if in_corpus is not None:
        link["in_corpus"] = in_corpus
    return link


def arxiv_ids_in_corpus(es: Elasticsearch, arxiv_ids: list[str]) -> set[str]:
    """Parmi ces identifiants, lesquels existent dans l'index arxiv-papers ?"""
    if not arxiv_ids or not es.indices.exists(index=INDEX_ARXIV):
        return set()
    resp = es.mget(index=INDEX_ARXIV, ids=list(set(arxiv_ids)), source=False)
    return {d["_id"] for d in resp["docs"] if d.get("found")}


def index_batch(es: Elasticsearch, items: list[dict], story_ids: dict | None = None) -> int:
    """Indexe un batch de documents HN dans Elasticsearch (+ documents de liaison ArXiv)."""
    story_ids = story_ids or {}
    actions = []
    linked_docs = []

    for item in items:
        if not item or not item.get("id"):
            continue

        # 1. Préparer le document
        doc = prepare_hn_doc(item, story_id=story_ids.get(item["id"]))
        link_source = doc.pop("_link_source", None)

        # 2. Action d'indexation HN
        action = {"_index": ES_INDEX_HN, "_id": doc["hn_id"], "_source": doc}

        # 3. Routing pour les commentaires : même shard que la story parente (obligatoire pour le join)
        if doc["type"] == "comment":
            action["_routing"] = doc["story_id"]
        actions.append(action)

        # 4. Lien ArXiv détecté → document de liaison
        if doc.get("arxiv_id_linked"):
            linked_docs.append({**doc, "_link_source": link_source})

    if not actions:
        return 0

    links = []
    if linked_docs:
        in_corpus = arxiv_ids_in_corpus(es, [d["arxiv_id_linked"] for d in linked_docs])
        for d in linked_docs:
            links.append({
                "_index": ES_INDEX_LINKS,
                "_id": f"{d['arxiv_id_linked']}_{d['hn_id']}",
                "_source": build_link_doc(d, in_corpus=d["arxiv_id_linked"] in in_corpus),
            })

    # 5. Indexation
    _, errors = helpers.bulk(es, actions + links, raise_on_error=False, stats_only=False)

    hn_errors = 0
    if errors:
        logger.warning(f"{len(errors)} erreurs lors de l'indexation — ex. : {errors[0]}")
        hn_errors = sum(1 for e in errors if next(iter(e.values()), {}).get("_index") == ES_INDEX_HN)

    if links:
        logger.info(f"  🔗 {len(links)} lien(s) ArXiv détecté(s)")

    return len(actions) - hn_errors


def archive_raw_items(items: list[dict]):
    """Copie brute des items dans MinIO (bucket hn-raw) : landing zone du flux streaming."""
    from src.config import BUCKET_HN_RAW
    from src.ingestion.upload_to_minio import s3

    if not items:
        return
    now = datetime.now(timezone.utc)
    key = f"{now:%Y/%m/%d}/hn-items-{now:%H%M%S}.json"
    body = "\n".join(json.dumps(it, ensure_ascii=False) for it in items).encode("utf-8")
    try:
        s3.put_object(Bucket=BUCKET_HN_RAW, Key=key, Body=body)
        logger.info(f"  🗄  {len(items)} items archivés dans s3://{BUCKET_HN_RAW}/{key}")
    except Exception as e:  # l'archivage ne doit pas bloquer l'ingestion
        logger.warning(f"Archivage MinIO impossible : {e}")


def poll_once(
    es: Elasticsearch,
    feed: str = "new",
    limit: int = 100,
    with_comments: int = 0,
    archive: bool = False,
) -> int:
    """Exécute un cycle de polling : récupère les nouvelles stories et les indexe."""

    # 1. Récupération des IDs depuis l'API Hacker News
    story_ids = fetch_new_story_ids(feed)
    logger.info(f"📡 {len(story_ids)} story IDs récupérés depuis l'API HN ({feed})")

    # 2. Filtrer les items déjà indexés (idempotence)
    candidates = story_ids[:limit]
    already = existing_ids(es, candidates)
    new_ids = [sid for sid in candidates if str(sid) not in already]
    logger.info(f"  → {len(new_ids)} nouveaux items à indexer")

    if not new_ids:
        return 0

    total_indexed = 0
    for i in range(0, len(new_ids), BATCH_SIZE):
        # 3. Indexation des nouveaux billets
        batch_ids = new_ids[i : i + BATCH_SIZE]
        items = fetch_items(batch_ids)

        # 3b. Optionnel : commentaires de premier niveau (pour les requêtes has_child)
        comment_parent = {}
        if with_comments > 0:
            kid_ids = []
            for story in items:
                for kid in (story.get("kids") or [])[:with_comments]:
                    kid_ids.append(kid)
                    comment_parent[kid] = str(story["id"])
            comments = fetch_items(kid_ids)
            items = items + comments

        if archive:
            archive_raw_items(items)

        count = index_batch(es, items, story_ids=comment_parent)
        total_indexed += count
        logger.info(f"  ✅ Batch {i // BATCH_SIZE + 1}: {count} documents indexés")

    es.indices.refresh(index=ES_INDEX_HN)
    return total_indexed


def main():
    parser = argparse.ArgumentParser(description="SciPulse — HN Poller")
    parser.add_argument("--continuous", action="store_true", help="Boucle de polling continue")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL, help="Intervalle en secondes")
    parser.add_argument("--feed", choices=list(FEEDS), default="new", help="Flux HN (new, top, best)")
    parser.add_argument("--limit", type=int, default=100, help="Nombre de stories examinées par cycle")
    parser.add_argument("--with-comments", type=int, default=0, metavar="N",
                        help="Indexer aussi les N premiers commentaires de chaque nouvelle story")
    parser.add_argument("--archive", action="store_true", help="Archiver les items bruts dans MinIO (hn-raw)")
    args = parser.parse_args()

    es = get_es_client()
    logger.info(f"Connexion ES: {es.info()['cluster_name']}")

    if not es.indices.exists(index=ES_INDEX_HN):
        # sans mapping explicite, le join field et la date epoch_second seraient mal typés
        from src.utils.mappings import HN_SETTINGS, LINKS_SETTINGS, create_index

        logger.info("Index hn-items absent → création avec le mapping explicite")
        create_index(es, ES_INDEX_HN, HN_SETTINGS)
        if not es.indices.exists(index=ES_INDEX_LINKS):
            create_index(es, ES_INDEX_LINKS, LINKS_SETTINGS)

    kwargs = dict(feed=args.feed, limit=args.limit, with_comments=args.with_comments, archive=args.archive)

    if args.continuous:
        logger.info(f"🔄 Mode continu — polling toutes les {args.interval}s (Ctrl+C pour arrêter)")
        while True:
            try:
                poll_once(es, **kwargs)
                time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("⏹  Arrêt du poller.")
                break
            except requests.RequestException as e:
                logger.warning(f"API HN indisponible ({e}) — nouvel essai dans {args.interval}s")
                time.sleep(args.interval)
    else:
        total = poll_once(es, **kwargs)
        logger.info(f"🏁 Cycle terminé — {total} documents indexés au total.")


if __name__ == "__main__":
    main()
