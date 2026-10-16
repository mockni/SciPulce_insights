"""
SciPulse — Détection (second passage) des liens ArXiv dans les items Hacker News
Tâche `detect_arxiv_links` du DAG scipulse_hn_poller (TP 7).

Le poller détecte déjà les liens à l'indexation ; ce second passage :
  - rattrape les items indexés avant une évolution de la regex (anciens IDs, /html/, texte),
  - recroise chaque lien avec le corpus (champ in_corpus = l'article est-il dans arxiv-papers ?),
  - garantit la cohérence entre hn-items.arxiv_id_linked et l'index arxiv-hn-links.

Usage :
    python -m src.enrichment.detect_arxiv_links
"""

import argparse
import html
import sys
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST, INDEX_ARXIV, INDEX_HN, INDEX_LINKS
from src.ingestion.hn_poller import arxiv_ids_in_corpus
from src.utils.arxiv_utils import extract_arxiv_ids

SOURCE_FIELDS = ["hn_id", "type", "title", "url", "text", "score", "descendants", "time", "story_id", "arxiv_id_linked"]


def find_links(src: dict) -> list[tuple[str, str]]:
    """Retourne [(arxiv_id, source)] trouvés dans l'URL puis dans le texte d'un item HN."""
    links = [(a, "url") for a in extract_arxiv_ids(src.get("url"))]
    for a in extract_arxiv_ids(html.unescape(src.get("text") or "")):
        if a not in {x for x, _ in links}:
            links.append((a, "text"))
    return links


def main():
    argparse.ArgumentParser(description="SciPulse — détection des liens ArXiv ↔ HN (second passage)").parse_args()
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Détection des liens ArXiv ↔ Hacker News     ║")
    print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST, request_timeout=60)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(1)
    if not es.indices.exists(index=INDEX_HN):
        print(f"\n⚠️  Index '{INDEX_HN}' absent — rien à analyser.")
        return

    # 1. Items HN qui mentionnent arxiv.org (URL ou texte)
    query = {
        "query": {
            "bool": {
                "should": [
                    {"wildcard": {"url": {"value": "*arxiv.org*", "case_insensitive": True}}},
                    {"match": {"text": "arxiv.org"}},
                    {"exists": {"field": "arxiv_id_linked"}},
                ],
                "minimum_should_match": 1,
            }
        }
    }
    hn_updates, link_docs = [], []
    scanned = 0
    for hit in helpers.scan(es, index=INDEX_HN, query=query, _source=SOURCE_FIELDS):
        scanned += 1
        src = hit["_source"]
        links = find_links(src)
        if not links:
            continue
        primary_id = links[0][0]
        if src.get("arxiv_id_linked") != primary_id:
            update = {"_op_type": "update", "_index": INDEX_HN, "_id": hit["_id"],
                      "doc": {"arxiv_id_linked": primary_id}}
            if hit.get("_routing"):
                update["_routing"] = hit["_routing"]
            hn_updates.append(update)
        for arxiv_id, source in links:
            link_docs.append((arxiv_id, source, src))

    print(f"\n🔎 {scanned} item(s) HN mentionnant ArXiv, {len(link_docs)} lien(s) trouvé(s)")
    if not link_docs:
        print("   (normal en début de collecte : les liens ArXiv sur HN sont rares)")
        return

    # 2. Croisement avec le corpus ArXiv
    in_corpus = arxiv_ids_in_corpus(es, [a for a, _, _ in link_docs]) if es.indices.exists(index=INDEX_ARXIV) else set()

    now = datetime.now(timezone.utc).isoformat()
    actions = list(hn_updates)
    for arxiv_id, source, src in link_docs:
        actions.append({
            "_index": INDEX_LINKS,
            "_id": f"{arxiv_id}_{src['hn_id']}",
            "_source": {
                "arxiv_id": arxiv_id,
                "hn_item_id": src["hn_id"],
                "hn_score": src.get("score", 0) or 0,
                "hn_comments": src.get("descendants", 0) or 0,
                "hn_title": src.get("title"),
                "hn_url": src.get("url"),
                "hn_time": src.get("time"),
                "link_source": source,
                "in_corpus": arxiv_id in in_corpus,
                "detected_at": now,
            },
        })

    ok, errors = helpers.bulk(es, actions, raise_on_error=False)
    es.indices.refresh(index=INDEX_LINKS)

    total_links = es.count(index=INDEX_LINKS)["count"]
    print(f"   ✅ {ok} opération(s) ({len(hn_updates)} item(s) HN corrigé(s))")
    print(f"   📚 {len(set(a for a, _, _ in link_docs) & in_corpus)} article(s) cité(s) présent(s) dans le corpus")
    print(f"   🔗 {total_links} lien(s) dans '{INDEX_LINKS}'")
    if errors:
        print(f"   ❌ {len(errors)} erreur(s) : {errors[0]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
