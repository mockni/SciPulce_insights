"""
SciPulse — Charge le jeu de test (tests/fixtures/) dans la couche Bronze de MinIO.

Sert à la procédure de validation sur la stack réelle (docs/validation.md) : les données
passent ensuite par les MÊMES tâches que la production (Silver, Great Expectations, Gold,
Spark, Kibana), ce qui permet de comparer les dashboards aux résultats attendus
(tests/fixtures/expected.json).

Les API externes sont remplacées par les fichiers de fixtures :
  - ArXiv      : tests/fixtures/arxiv_bronze.jsonl  → arxiv-raw/ingest_date=<date>/
  - Hacker News: tests/fixtures/hn_items.json       → hn-raw/date=<date>/  (cycle de polling simulé)
  - OpenAlex   : tests/fixtures/openalex_works.json → citations-raw/ingest_date=<date>/
                 (réponses filtrées par les DOI réellement demandés, comme l'API)

Les citations sont demandées à partir du Silver ArXiv : le script construit donc aussi
arxiv-clean (même fonction que la tâche silver_arxiv).

⚠️ À lancer sur une stack vide (`make clean && make up`) : la partition du jour est remplacée
et le jeu de test se mélangerait aux données réelles déjà présentes.

Usage :
    python -m src.utils.seed_test_set
    python -m src.utils.seed_test_set --ingest-date 2026-10-16
"""

import argparse
import json
import logging
from pathlib import Path
from unittest.mock import patch

from src.config import ALL_BUCKETS, PROJECT_ROOT
from src.lake import s3

log = logging.getLogger("scipulse.seed")

FIXTURES = Path(PROJECT_ROOT) / "tests" / "fixtures"


def seed(ingest_date: str) -> dict:
    from src.bronze import arxiv as bronze_arxiv
    from src.bronze import citations as bronze_citations
    from src.bronze import hn as bronze_hn
    from src.silver import arxiv as silver_arxiv
    from src.utils.arxiv_utils import normalize_doi

    s3.ensure_buckets(ALL_BUCKETS)

    # 🥉 ArXiv : lignes brutes, intactes (dont les anomalies volontaires)
    lines = (FIXTURES / "arxiv_bronze.jsonl").read_text(encoding="utf-8").splitlines()
    arxiv_manifest = bronze_arxiv.write_bronze(iter(lines), ingest_date, "test-set")

    # 🥉 Hacker News : un cycle de polling dont l'API renvoie les items des fixtures
    items = json.loads((FIXTURES / "hn_items.json").read_text(encoding="utf-8"))
    by_id = {it["id"]: it for it in items}
    stories = [it["id"] for it in items if it.get("type") == "story"]
    with patch.object(bronze_hn, "fetch_ids", side_effect=lambda feed: stories if feed == "newstories" else []), \
         patch.object(bronze_hn, "fetch_items", side_effect=lambda ids: [by_id[i] for i in ids if i in by_id]):
        hn_summary = bronze_hn.poll_once(limit=len(stories), top=0, with_comments=10)

    # 🥈 ArXiv (nécessaire pour savoir quels DOI demander à OpenAlex)
    silver_summary = silver_arxiv.build_silver(ingest_date)

    # 🥉 Citations : l'« API » renvoie les œuvres dont le DOI fait partie du lot demandé
    works = json.loads((FIXTURES / "openalex_works.json").read_text(encoding="utf-8"))

    def fake_fetch(dois):
        wanted = set(dois)
        return [w for w in works if normalize_doi(w.get("doi")) in wanted]

    with patch.object(bronze_citations, "fetch_works_by_dois", side_effect=fake_fetch):
        citations_manifest = bronze_citations.ingest(bronze_citations.dois_to_query(), ingest_date)

    return {"arxiv_bronze": arxiv_manifest, "hn_bronze": hn_summary,
            "arxiv_silver": silver_summary, "citations_bronze": citations_manifest}


def main():
    parser = argparse.ArgumentParser(description="SciPulse — jeu de test → couche Bronze (MinIO)")
    parser.add_argument("--ingest-date", default=s3.utc_now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    summary = seed(args.ingest_date)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    print("\n✅ Jeu de test chargé. Étapes suivantes : voir docs/validation.md (make validate-test-set).")


if __name__ == "__main__":
    main()
