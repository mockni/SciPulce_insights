"""
SciPulse — 🥉 Bronze Hacker News : polling micro-batch de l'API Firebase → MinIO (bucket hn-raw)

Chaque cycle (DAG toutes les 5 minutes) :
  1. récupère les IDs de /newstories (stories non encore vues) ;
  2. récupère aussi /topstories : ces stories sont relues à chaque cycle, car leur
     score et leur nombre de commentaires évoluent (le Silver garde la mesure la plus récente) ;
  3. optionnellement, les N premiers commentaires de chaque nouvelle story ;
  4. écrit les items TELS QUE RENVOYÉS PAR L'API dans un fichier JSON Lines :

        hn-raw/date=2026-10-16/hn-20261016T143000Z.jsonl

Seules deux métadonnées techniques, préfixées par « _ », sont ajoutées :
`_fetched_at` (horodatage de la collecte) et `_feed` (newstories / topstories / comment).
L'état des IDs déjà vus est conservé dans hn-raw/_state/seen_ids.json.

Usage :
    python -m src.bronze.hn
    python -m src.bronze.hn --limit 200 --top 30 --with-comments 3
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from src.config import BUCKET_HN_RAW, HN_API_BASE
from src.lake import s3
from src.monitoring.events import emit_layer_write

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scipulse.bronze.hn")

STATE_KEY = "_state/seen_ids.json"
MAX_STATE_IDS = 50_000        # fenêtre glissante : HN publie ~1 500 stories / jour
HTTP_WORKERS = 8              # l'API tolère ~30 requêtes / s

_session = requests.Session()


def fetch_ids(feed: str) -> list[int]:
    resp = _session.get(f"{HN_API_BASE}/{feed}.json", timeout=10)
    resp.raise_for_status()
    return resp.json() or []


def fetch_item(item_id: int) -> dict | None:
    try:
        resp = _session.get(f"{HN_API_BASE}/item/{item_id}.json", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.warning(f"item {item_id} non récupéré : {e}")
        return None


def fetch_items(ids: list[int]) -> list[dict]:
    """Items récupérés en parallèle ; les réponses vides (null) sont ignorées."""
    with ThreadPoolExecutor(max_workers=HTTP_WORKERS) as pool:
        return [it for it in pool.map(fetch_item, ids) if it]


def load_state() -> list[int]:
    return s3.get_json(BUCKET_HN_RAW, STATE_KEY, default={"seen": []})["seen"]


def save_state(seen: list[int]):
    s3.put_json(BUCKET_HN_RAW, STATE_KEY, {"seen": seen[-MAX_STATE_IDS:], "updated_at": s3.utc_now().isoformat()})


def tag(items: list[dict], feed: str, fetched_at: str, story_of: dict | None = None) -> list[dict]:
    """Ajoute les métadonnées techniques _fetched_at / _feed (le contenu reste intact)."""
    tagged = []
    for item in items:
        record = dict(item)
        record["_fetched_at"] = fetched_at
        record["_feed"] = feed
        if story_of and item.get("id") in story_of:
            record["_story_id"] = story_of[item["id"]]  # story racine d'un commentaire
        tagged.append(record)
    return tagged


def poll_once(limit: int = 100, top: int = 30, with_comments: int = 0) -> dict:
    started = time.time()
    s3.ensure_buckets([BUCKET_HN_RAW])
    now = s3.utc_now()
    fetched_at = now.isoformat()

    seen = load_state()
    seen_set = set(seen)

    new_ids = [i for i in fetch_ids("newstories")[:limit] if i not in seen_set]
    top_ids = [i for i in fetch_ids("topstories")[:top] if i not in set(new_ids)]
    log.info(f"📡 {len(new_ids)} nouvelles stories, {len(top_ids)} top stories à rafraîchir")

    new_items = fetch_items(new_ids)
    top_items = fetch_items(top_ids)
    records = tag(new_items, "newstories", fetched_at) + tag(top_items, "topstories", fetched_at)

    if with_comments > 0:
        story_of = {}
        for story in new_items:
            for kid in (story.get("kids") or [])[:with_comments]:
                story_of[kid] = story["id"]
        comments = fetch_items(list(story_of))
        records += tag(comments, "comment", fetched_at, story_of)

    key = f"date={now:%Y-%m-%d}/hn-{now:%Y%m%dT%H%M%SZ}.jsonl"
    if records:
        s3.put_jsonl_records(BUCKET_HN_RAW, key, records)
        save_state(seen + [it["id"] for it in new_items if it.get("id") is not None])
        log.info(f"   ✅ {len(records)} items bruts → s3://{BUCKET_HN_RAW}/{key}")
    else:
        log.info("   (rien de nouveau)")

    emit_layer_write("bronze", "hn", rows=len(records), objects=1 if records else 0, started=started)
    return {"key": key if records else None, "rows": len(records)}


def main():
    parser = argparse.ArgumentParser(description="SciPulse — Bronze Hacker News (API → MinIO)")
    parser.add_argument("--limit", type=int, default=100, help="Nouvelles stories examinées par cycle")
    parser.add_argument("--top", type=int, default=30, help="Top stories rafraîchies à chaque cycle")
    parser.add_argument("--with-comments", type=int, default=3, metavar="N",
                        help="N premiers commentaires de chaque nouvelle story")
    args = parser.parse_args()
    try:
        result = poll_once(args.limit, args.top, args.with_comments)
    except requests.RequestException as e:
        log.error(f"❌ API Hacker News indisponible : {e}")
        sys.exit(1)
    log.info(f"🏁 Cycle terminé — {result['rows']} items")


if __name__ == "__main__":
    main()
