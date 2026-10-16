"""
Génère le jeu de test déterministe (graine fixe) utilisé par la procédure de validation.

    python tests/fixtures/make_fixtures.py

Fichiers produits (versionnés) :
  arxiv_bronze.jsonl     lignes au format du dump Kaggle, avec anomalies volontaires
  hn_items.json          items au format de l'API Hacker News
  openalex_works.json    œuvres au format de l'API OpenAlex (/works)
  expected.json          résultats attendus vérifiés par tests/integration/test_pipeline_e2e.py

Cas analytiques construits :
  - 1706.03762  très cité (top de sa cohorte), absent de HN          → segment academic_only
  - 2303.08774  très populaire sur HN (900 points), peu cité         → segment viral_low_citations
  - 2305.10001  très cité ET populaire sur HN                         → segment academic_and_viral
  - cs.LG : volume 2022 → 2023 en forte croissance                    → category_yoy_growth > 0
Anomalies Bronze (corrigées ou rejetées au Silver) :
  ligne JSON invalide, doublon d'identifiant, abstract manquant, abstract trop court,
  article hors périmètre (hep-th), DOI avec préfixe https et majuscules, date mal formée,
  ancien format d'identifiant (cs/0112017), titre multi-lignes.
"""

import json
import random
from pathlib import Path

HERE = Path(__file__).parent
rng = random.Random(2026)

WORDS = {
    "cs.CL": "language translation token transformer attention sentence semantic parsing dialogue corpus".split(),
    "cs.LG": "learning gradient optimization neural training generalization regularization kernel policy".split(),
    "cs.CV": "image segmentation detection pixel convolutional video recognition visual depth scene".split(),
}
MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
NAMES = [("Martin", "Alice"), ("Chen", "Bob"), ("Kebaili", "Yakoub"), ("Rossi", "Lea"),
         ("Tanaka", "Jun"), ("Silva", "Ines"), ("Dubois", "Hugo"), ("Nguyen", "Mai")]


def article(arxiv_id, category, year, month=6, title=None, abstract=None, doi=None, extra_cats=()):
    words = WORDS[category]
    authors = rng.sample(NAMES, rng.randint(1, 3))
    return {
        "id": arxiv_id,
        "submitter": f"{authors[0][1]} {authors[0][0]}",
        "authors": ", ".join(f"{p} {n}" for n, p in authors),
        "title": title or " ".join(rng.choice(words) for _ in range(6)).capitalize(),
        "comments": None,
        "journal-ref": None,
        "doi": doi,
        "report-no": None,
        "categories": " ".join((category, *extra_cats)),
        "license": "http://arxiv.org/licenses/nonexclusive-distrib/1.0/",
        "abstract": abstract if abstract is not None else
        "  " + " ".join(rng.choice(words) for _ in range(rng.randint(40, 90))) + ".\n",
        "versions": [{"version": "v1", "created": f"Mon, {rng.randint(1, 28)} {MONTHS[month - 1]} {year} 10:00:00 GMT"}],
        "update_date": f"{min(year + 1, 2025)}-0{rng.randint(1, 9)}-1{rng.randint(0, 9)}",
        "authors_parsed": [[n, p, ""] for n, p in authors],
    }


def main():
    records, works, citations = [], [], {}

    # Cohortes régulières (pour des percentiles significatifs)
    seq = 20000
    cohorts = [("cs.CL", 2017, 12), ("cs.CL", 2023, 15), ("cs.LG", 2022, 6), ("cs.LG", 2023, 18),
               ("cs.CV", 2021, 12)]
    for category, year, n in cohorts:
        for _ in range(n):
            seq += 1
            arxiv_id = f"{year % 100:02d}{rng.randint(1, 12):02d}.{seq:05d}"
            records.append(article(arxiv_id, category, year, rng.randint(1, 12)))
            citations[arxiv_id] = rng.randint(0, 40)

    # Cas analytiques
    records.append(article("1706.03762", "cs.CL", 2017, 6, title="Attention Is All You Need",
                           extra_cats=("cs.LG",)))
    citations["1706.03762"] = 120_000
    records.append(article("2303.08774", "cs.CL", 2023, 3, title="GPT-4 Technical Report"))
    citations["2303.08774"] = 2
    records.append(article("2305.10001", "cs.LG", 2023, 5, title="Scaling laws for sparse mixture of experts",
                           doi="10.5555/Example.2023.001"))
    citations["2305.10001"] = 5_000            # via le DOI d'éditeur (journal_doi)

    # Anomalies
    records.append(article("2101.00001", "cs.CV", 2021, abstract=""))                         # abstract vide
    records.append(article("2101.00002", "cs.CV", 2021, abstract="Too short."))               # abstract court
    records.append(article("2101.00003", "cs.CV", 2021, doi="https://doi.org/10.1109/CVPR.2021.ABC"))
    records.append({**article("2101.00004", "cs.CV", 2021), "categories": "hep-th gr-qc"})     # hors périmètre
    records.append({**article("2101.00005", "cs.CV", 2021), "update_date": "15/03/2022"})      # date mal formée
    records.append(article("cs/0112017", "cs.CL", 2001, 12, title="An old-style\n  identifier paper"))
    dup = article("2101.00006", "cs.CV", 2021)
    records.append(dup)
    records.append({**dup, "update_date": "2024-01-01", "title": dup["title"] + " (v2)"})      # doublon

    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    lines.insert(len(lines) // 2, "{ ligne json invalide")
    (HERE / "arxiv_bronze.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # OpenAlex : une œuvre « prépublication » par article (DOI 10.48550/arxiv.<id>)
    for i, (arxiv_id, cited) in enumerate(sorted(citations.items())):
        doi = f"https://doi.org/10.48550/arXiv.{arxiv_id}"
        if arxiv_id == "2305.10001":
            doi = "https://doi.org/10.5555/example.2023.001"     # version éditeur
        works.append({
            "id": f"https://openalex.org/W{3000000000 + i}",
            "doi": doi,
            "title": f"Work for {arxiv_id}",
            "type": "preprint" if "48550" in doi else "article",
            "publication_year": 2017 if arxiv_id == "1706.03762" else 2023,
            "publication_date": "2023-01-01",
            "cited_by_count": cited,
            "counts_by_year": [{"year": 2025, "cited_by_count": cited // 4}, {"year": 2024, "cited_by_count": cited // 4}],
            "referenced_works_count": 30,
            "primary_topic": {"field": {"display_name": "Computer Science"},
                              "subfield": {"display_name": "Artificial Intelligence"},
                              "domain": {"display_name": "Physical Sciences"}},
            "ids": {"openalex": f"https://openalex.org/W{3000000000 + i}", "doi": doi},
        })
    (HERE / "openalex_works.json").write_text(json.dumps(works, indent=1), encoding="utf-8")

    # Hacker News
    items = [
        {"id": 41000001, "type": "story", "by": "alice", "time": 1717000000, "score": 900, "descendants": 450,
         "title": "GPT-4 Technical Report", "url": "https://arxiv.org/abs/2303.08774v3", "kids": [41000101]},
        {"id": 41000002, "type": "story", "by": "bob", "time": 1717003600, "score": 300, "descendants": 120,
         "title": "Scaling laws for sparse MoE", "url": "https://arxiv.org/pdf/2305.10001.pdf"},
        {"id": 41000003, "type": "story", "by": "carol", "time": 1717007200, "score": 40, "descendants": 5,
         "title": "Show HN: my side project", "url": "https://example.com/project"},
        {"id": 41000004, "type": "story", "by": "dan", "time": 1717010800, "score": 12, "descendants": 3,
         "title": "Ask HN: which papers do you read?",
         "text": "I liked https:&#x2F;&#x2F;arxiv.org&#x2F;abs&#x2F;2303.08774 a lot"},
        {"id": 41000005, "type": "story", "deleted": True, "time": 1717014400},
        {"id": 41000101, "type": "comment", "by": "erin", "time": 1717000500, "parent": 41000001,
         "text": "Interesting, compare with <a href=\"https:&#x2F;&#x2F;arxiv.org&#x2F;abs&#x2F;1706.03762\">this</a>"},
    ]
    (HERE / "hn_items.json").write_text(json.dumps(items, indent=1), encoding="utf-8")

    expected = {
        "bronze_lines": len(lines),
        "json_errors": 1,
        "out_of_scope": 1,
        "duplicates": 1,
        "missing_abstract": 1,
        "silver_arxiv_rows": len(records) - 1 - 1 - 1,     # hors périmètre, doublon, abstract vide
        "segments": {"1706.03762": "academic_only", "2303.08774": "viral_low_citations",
                     "2305.10001": "academic_and_viral"},
        "hn_score_total": {"2303.08774": 912, "2305.10001": 300},
    }
    (HERE / "expected.json").write_text(json.dumps(expected, indent=1), encoding="utf-8")
    print(f"{len(lines)} lignes ArXiv, {len(works)} œuvres OpenAlex, {len(items)} items HN")


if __name__ == "__main__":
    main()
