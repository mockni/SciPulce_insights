"""
SciPulse — Mise à jour des scores croisés ArXiv ↔ Hacker News
Tâche `update_arxiv_hn_scores` du DAG scipulse_hn_poller (TP 7).

Le score et le nombre de commentaires d'un post HN évoluent pendant ~48 h.
Ce script :
  1. rafraîchit (via l'API HN) les items liés à un article ArXiv,
  2. met à jour hn-items et arxiv-hn-links,
  3. réagrège les scores par article et les écrit dans arxiv-papers
     (hn_score, hn_comments_count, hn_item_ids).

Usage :
    python -m src.enrichment.update_hn_scores
    python -m src.enrichment.update_hn_scores --no-refresh     # sans appel à l'API HN
"""

import argparse
import sys

from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST, INDEX_ARXIV, INDEX_HN, INDEX_LINKS
from src.enrichment.enrich_arxiv import load_hn_scores, update_index
from src.ingestion.hn_poller import fetch_items


def refresh_linked_items(es: Elasticsearch) -> int:
    """Relit sur l'API HN les items qui pointent vers ArXiv et met à jour leurs compteurs."""
    links = list(helpers.scan(es, index=INDEX_LINKS, query={"query": {"match_all": {}}}))
    item_ids = sorted({int(l["_source"]["hn_item_id"]) for l in links if l["_source"].get("hn_item_id")})
    if not item_ids:
        return 0

    fresh = {str(it["id"]): it for it in fetch_items(item_ids)}
    actions = []
    for item_id, item in fresh.items():
        if item.get("type") == "comment":
            continue  # les commentaires n'ont ni score ni descendants
        counters = {"score": item.get("score", 0) or 0, "descendants": item.get("descendants", 0) or 0}
        actions.append({"_op_type": "update", "_index": INDEX_HN, "_id": item_id, "doc": counters})
    for link in links:
        item = fresh.get(str(link["_source"].get("hn_item_id")))
        if item and item.get("type") != "comment":
            actions.append({
                "_op_type": "update", "_index": INDEX_LINKS, "_id": link["_id"],
                "doc": {"hn_score": item.get("score", 0) or 0, "hn_comments": item.get("descendants", 0) or 0},
            })

    ok, errors = helpers.bulk(es, actions, raise_on_error=False)
    es.indices.refresh(index=[INDEX_HN, INDEX_LINKS])
    if errors:
        print(f"   ⚠️  {len(errors)} mise(s) à jour en échec : {errors[0]}")
    return len(fresh)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — scores croisés ArXiv × HN")
    parser.add_argument("--no-refresh", action="store_true", help="Ne pas interroger l'API HN")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Mise à jour des scores croisés ArXiv × HN   ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    es = Elasticsearch(ES_HOST, request_timeout=60)
    try:
        es.info()
    except Exception as e:
        print(f"❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    if not es.indices.exists(index=INDEX_LINKS) or es.count(index=INDEX_LINKS)["count"] == 0:
        print("ℹ️  Aucun lien ArXiv ↔ HN pour l'instant — rien à mettre à jour.")
        return

    if not args.no_refresh:
        print("📡 Rafraîchissement des items HN liés à ArXiv...")
        n = refresh_linked_items(es)
        print(f"   ✅ {n} item(s) relu(s) sur l'API HN\n")

    hn_scores = load_hn_scores(es)
    if not es.indices.exists(index=INDEX_ARXIV):
        print(f"⚠️  Index '{INDEX_ARXIV}' absent — scores non reportés sur les articles.")
        return
    _, _, errors = update_index(es, keywords_map={}, hn_scores=hn_scores)

    linked = es.count(index=INDEX_ARXIV, query={"range": {"hn_score": {"gt": 0}}})["count"]
    print(f"\n📊 {linked} article(s) ArXiv avec un score HN > 0")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
