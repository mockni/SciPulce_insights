"""
Procédure de validation sur jeu de test : pipeline Bronze → Silver → Gold de bout en bout.

Aucun service n'est nécessaire :
  - MinIO est remplacé par moto (faux S3 en mémoire) ;
  - les API Hacker News et OpenAlex sont remplacées par les fichiers de tests/fixtures/ ;
  - Great Expectations tourne avec un contexte éphémère ;
  - Spark tourne en local (local[2]) et lit une copie locale de la couche Silver.

Le jeu de test (tests/fixtures/make_fixtures.py) contient des anomalies volontaires et
trois articles construits pour les cas d'usage du dashboard ; expected.json fixe les
résultats attendus. Lancement :  pytest tests/integration -v
"""

import importlib.util
import shutil
import sys
from unittest.mock import patch

import pytest

from src import config


@pytest.fixture
def ephemeral_gx(monkeypatch):
    pytest.importorskip("great_expectations")
    import src.quality.validate_arxiv as va
    import src.quality.validate_citations as vc
    import src.quality.validate_hn as vh
    from src.quality.gx_utils import get_context

    for module in (va, vh, vc):
        monkeypatch.setattr(module, "get_context", lambda: get_context(ephemeral=True))


def run_bronze_and_silver(s3_lake, arxiv_lines, hn_items, openalex_works):
    """Exécute toutes les étapes Python du pipeline sur le jeu de test."""
    from src.bronze import arxiv as bronze_arxiv
    from src.bronze import citations as bronze_citations
    from src.bronze import hn as bronze_hn
    from src.silver import arxiv as silver_arxiv
    from src.silver import citations as silver_citations
    from src.silver import hn as silver_hn

    # 🥉 Bronze ArXiv (petits fichiers pour tester le découpage)
    bronze_arxiv.write_bronze(iter(arxiv_lines), "2026-10-16", "fixture", chunk_lines=30)
    # 🥉 Bronze HN (API simulée)
    by_id = {it["id"]: it for it in hn_items}
    stories = [it["id"] for it in hn_items if it.get("type") == "story"]
    with patch.object(bronze_hn, "fetch_ids", side_effect=lambda feed: stories if feed == "newstories" else []), \
         patch.object(bronze_hn, "fetch_items", side_effect=lambda ids: [by_id[i] for i in ids if i in by_id]):
        first = bronze_hn.poll_once(limit=100, top=0, with_comments=2)
        second = bronze_hn.poll_once(limit=100, top=0, with_comments=2)   # rien de nouveau
    # 🥈 Silver ArXiv et HN
    arxiv_summary = silver_arxiv.build_silver("2026-10-16")
    hn_summary = silver_hn.build_silver()
    # 🥉 Bronze citations (API OpenAlex simulée : filtre par DOI)
    from src.utils.arxiv_utils import normalize_doi

    def fake_fetch(dois):
        wanted = set(dois)
        return [w for w in openalex_works if normalize_doi(w["doi"]) in wanted]

    with patch.object(bronze_citations, "fetch_works_by_dois", side_effect=fake_fetch):
        citations_manifest = bronze_citations.ingest(bronze_citations.dois_to_query(), "2026-10-16")
    # 🥈 Silver citations
    citations_summary = silver_citations.build_silver("2026-10-16")
    return {"hn_first": first, "hn_second": second, "arxiv": arxiv_summary, "hn": hn_summary,
            "citations_bronze": citations_manifest, "citations": citations_summary}


def test_bronze_is_faithful_and_silver_applies_rules(s3_lake, arxiv_lines, hn_items, openalex_works, expected):
    out = run_bronze_and_silver(s3_lake, arxiv_lines, hn_items, openalex_works)

    # Bronze : lignes intactes, découpées en fichiers de 30 lignes + manifeste
    keys = s3_lake.list_keys(config.BUCKET_ARXIV_RAW, "ingest_date=2026-10-16/", ".jsonl")
    assert len(keys) == 3
    stored = [l for k in keys for l in s3_lake.iter_jsonl_lines(config.BUCKET_ARXIV_RAW, k)]
    assert stored == arxiv_lines
    manifest = s3_lake.get_json(config.BUCKET_ARXIV_RAW, "ingest_date=2026-10-16/_manifest.json")
    assert manifest["rows"] == expected["bronze_lines"]

    # Silver ArXiv : chaque règle a été appliquée et comptée
    rules = out["arxiv"]["rules"]
    assert rules["json_errors"] == expected["json_errors"]
    assert rules["out_of_scope"] == expected["out_of_scope"]
    assert rules["duplicates"] == expected["duplicates"]
    assert rules["missing_abstract"] == expected["missing_abstract"]
    assert rules["invalid_dates"] == 1 and rules["short_abstracts_flagged"] == 1
    assert out["arxiv"]["rows"] == expected["silver_arxiv_rows"]

    silver = s3_lake.read_parquet_prefix(config.BUCKET_ARXIV_CLEAN, config.SILVER_ARXIV_PREFIX).set_index("arxiv_id")
    assert silver.index.is_unique
    assert silver.loc["2101.00003", "doi"] == "10.1109/cvpr.2021.abc"            # DOI normalisé
    assert silver.loc["2101.00006", "title"].endswith("(v2)")                    # version la plus récente
    assert silver.loc["cs/0112017", "title"] == "An old-style identifier paper"  # titre multi-lignes nettoyé
    assert silver.loc["1706.03762", "arxiv_doi"] == "10.48550/arxiv.1706.03762"
    assert str(silver["date_published"].dtype) == "datetime64[us, UTC]"

    # Bronze / Silver HN : métadonnées techniques, item supprimé rejeté, liens ArXiv détectés
    assert out["hn_first"]["rows"] == 6 and out["hn_second"]["rows"] == 0
    hn = s3_lake.read_parquet_prefix(config.BUCKET_HN_CLEAN, config.SILVER_HN_PREFIX).set_index("hn_id")
    assert "41000005" not in hn.index                                            # deleted
    assert hn.loc["41000001", "arxiv_id_linked"] == "2303.08774"                 # version retirée
    assert hn.loc["41000004", "arxiv_link_source"] == "text"                     # lien dans le texte HTML
    assert hn.loc["41000101", "story_id"] == "41000001"
    assert hn.loc["41000003", "post_type"] == "show_hn"

    # Citations : clés normalisées, rattachement par DOI ArXiv et par DOI d'éditeur
    cit = s3_lake.read_parquet_prefix(config.BUCKET_CITATIONS_CLEAN, config.SILVER_CITATIONS_PREFIX)
    assert len(cit) == len(openalex_works)
    assert set(cit["match_key_type"]) == {"arxiv_doi", "journal_doi"}
    assert (cit["doi"] == cit["doi"].str.lower()).all()


def test_great_expectations_suites(s3_lake, arxiv_lines, hn_items, openalex_works, ephemeral_gx):
    run_bronze_and_silver(s3_lake, arxiv_lines, hn_items, openalex_works)
    from src.quality import samples, validate_arxiv, validate_citations, validate_hn
    from src.quality.gx_utils import summarize

    from src.quality.gx_utils import validation_results

    def failed_of(result):
        return {r["expectation_config"]["expectation_type"] + ":" + str(r["expectation_config"]["kwargs"].get("column"))
                for r in validation_results(result) if not r["success"]}

    # Bronze : les anomalies du jeu de test DOIVENT être détectées
    _, bronze = validate_arxiv.validate(samples.arxiv_bronze_sample(), "bronze")
    assert not bronze.success
    assert "expect_column_values_to_be_unique:id" in failed_of(bronze)
    assert "expect_column_values_to_match_regex:update_date" in failed_of(bronze)
    _, bronze_hn = validate_hn.validate(samples.hn_bronze_sample(), "bronze")
    assert failed_of(bronze_hn) == {"expect_column_values_to_be_null:deleted",       # item supprimé
                                    "expect_column_values_to_not_be_null:title"}      # … donc sans titre

    # Silver : le contrat est respecté après nettoyage
    for module, loader, layer in [(validate_arxiv, samples.arxiv_silver_sample, "silver"),
                                  (validate_hn, samples.hn_silver_sample, "silver")]:
        _, result = module.validate(loader(), layer)
        assert result.success, (module.__name__, layer, summarize(result))
    _, cit = validate_citations.validate(samples.citations_silver_sample())
    assert cit.success
    assert summarize(cit)["expectations_evaluated"] >= 3


spark_available = importlib.util.find_spec("pyspark") is not None and shutil.which("java") is not None


@pytest.mark.skipif(not spark_available, reason="PySpark + Java requis")
def test_spark_gold_answers_analytical_questions(s3_lake, arxiv_lines, hn_items, openalex_works, expected, tmp_path):
    run_bronze_and_silver(s3_lake, arxiv_lines, hn_items, openalex_works)

    # Copie locale de la couche Silver : Spark lit file:// au lieu de s3a://
    lake = tmp_path / "lake"
    for bucket, prefix in [(config.BUCKET_ARXIV_CLEAN, config.SILVER_ARXIV_PREFIX),
                           (config.BUCKET_HN_CLEAN, config.SILVER_HN_PREFIX),
                           (config.BUCKET_CITATIONS_CLEAN, config.SILVER_CITATIONS_PREFIX)]:
        for key in s3_lake.list_keys(bucket, prefix, ".parquet"):
            target = lake / bucket / key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(s3_lake.get_bytes(bucket, key))

    import os
    from datetime import date

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from src.spark import insights_job as job

    spark = job.build_spark(master="local[2]")
    try:
        arxiv, hn, citations = job.read_silver(spark, f"file://{lake}/")
        gold, stats = job.build_gold(arxiv, hn, citations, date(2026, 10, 16), tfidf_min_df=1.0)
        rows = {r["arxiv_id"]: r.asDict() for r in gold.collect()}
        plans = job.explain_strategies(arxiv, job.match_citations(arxiv, citations, 2026))
    finally:
        spark.stop()

    assert len(rows) == expected["silver_arxiv_rows"]
    # Cas d'usage du dashboard augmenté
    for arxiv_id, segment in expected["segments"].items():
        assert rows[arxiv_id]["segment"] == segment, (arxiv_id, rows[arxiv_id]["segment"])
    for arxiv_id, total in expected["hn_score_total"].items():
        assert rows[arxiv_id]["hn_score_total"] == total
    assert rows["2305.10001"]["citation_match_key"] == "journal_doi"
    assert rows["1706.03762"]["citation_match_key"] == "arxiv_doi"
    assert rows["1706.03762"]["has_hn"] is False      # cité seulement dans un commentaire HN

    # Score : borné, maximal pour l'article à la fois très cité et viral
    scores = [r["impact_score"] for r in rows.values()]
    assert all(0 <= s <= 100 for s in scores)
    assert rows["2305.10001"]["impact_score"] == max(scores)
    assert rows["2305.10001"]["impact_rank_in_category"] == 1

    # Window functions : croissance de cs.LG entre 2022 et 2023
    assert rows["2305.10001"]["category_yoy_growth"] > 0
    # … mais pas de « croissance » entre deux années non consécutives (cs.CL : 2001 → 2017)
    assert rows["1706.03762"]["category_yoy_growth"] is None
    # TF-IDF : mots-clés présents et issus du texte
    assert rows["1706.03762"]["keywords"]
    # Taux de correspondance
    assert 0 < stats["pct_with_citations"] < 1 and stats["with_hn"] == 2
    # Plans : sort-merge forcé vs broadcast
    assert "SortMergeJoin" in plans["sort_merge"] and "BroadcastHashJoin" in plans["broadcast"]
