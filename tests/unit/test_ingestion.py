"""Tests des scripts d'ingestion ArXiv (TP 1, 2, 3)."""

import json

from src.ingestion.arxiv_bulk_ingest import generate_actions, transform_article
from src.ingestion.prepare_subset import matches, select
from src.ingestion.reindex_arxiv import transform_for_reindex


class TestTransformArticle:
    """transform_article doit reproduire les filtres Logstash de arxiv.conf."""

    def test_renames_and_splits(self, raw_article):
        doc = transform_article(raw_article)
        assert doc["arxiv_id"] == "1706.03762"
        assert "id" not in doc and "update_date" not in doc
        assert doc["categories"] == ["cs.CL", "cs.LG"]
        assert doc["primary_category"] == "cs.CL"

    def test_cleans_text(self, raw_article):
        doc = transform_article(raw_article)
        assert "\n" not in doc["abstract"]
        assert doc["abstract"].startswith("The dominant")
        assert doc["title"] == "Attention Is All You Need"

    def test_dates(self, raw_article):
        doc = transform_article(raw_article)
        assert doc["date_updated"] == "2023-08-03T00:00:00.000Z"
        assert doc["date_published"] == "2017-06-12T17:57:34.000Z"

    def test_keeps_other_fields_like_logstash(self, raw_article):
        doc = transform_article(raw_article)
        assert doc["submitter"] == "Ashish Vaswani"
        assert doc["versions"] == raw_article["versions"]
        assert "doi" in doc and doc["doi"] is None  # champ présent dans la source (null)

    def test_missing_versions_falls_back_to_update_date(self, raw_article):
        raw_article.pop("versions")
        assert transform_article(raw_article)["date_published"] == "2023-08-03T00:00:00.000Z"


class TestGenerateActions:
    def test_skips_invalid_json_and_missing_id(self, raw_article, tmp_path, monkeypatch):
        monkeypatch.setattr("src.ingestion.arxiv_bulk_ingest.DEAD_LETTER_FILE", str(tmp_path / "dead.jsonl"))
        lines = [
            ("f:1", json.dumps(raw_article)),
            ("f:2", "{ ceci n'est pas du json"),
            ("f:3", json.dumps({"title": "sans identifiant"})),
            ("f:4", "   "),
        ]
        stats = {}
        actions = list(generate_actions(lines, index="test-index", stats=stats))
        assert len(actions) == 1
        assert actions[0]["_id"] == "1706.03762"
        assert actions[0]["_index"] == "test-index"
        assert stats == {"json_errors": 1, "missing_id": 1}
        assert "f:2" in (tmp_path / "dead.jsonl").read_text()


class TestReindexTransform:
    def test_from_logstash_document(self, raw_article):
        logstash_doc = transform_article(raw_article)   # équivalent du document arxiv-papers-raw
        doc = transform_for_reindex({"_id": "1706.03762", "_source": logstash_doc})
        assert doc["arxiv_id"] == "1706.03762"
        assert len(doc["authors"]) == 8
        assert doc["authors"][0]["name"] == "Ashish Vaswani"
        assert doc["authors_flat"].startswith("Ashish Vaswani, Noam Shazeer")
        assert doc["domain"] == "cs"
        assert doc["versions_count"] == 2
        assert doc["abstract_length"] == len(doc["abstract"])
        # seuls les champs du mapping sont conservés
        assert "authors_parsed" not in doc and "license" not in doc

    def test_from_raw_string_authors(self):
        hit = {"_source": {"arxiv_id": "2301.00001", "title": "T", "authors": "A Martin and B Chen",
                           "categories": "cs.AI", "date_updated": "2023-01-05"}}
        doc = transform_for_reindex(hit)
        assert doc["authors_flat"] == "A Martin, B Chen"
        assert doc["categories"] == ["cs.AI"]
        assert doc["date_updated"] == "2023-01-05T00:00:00.000Z"
        assert doc["date_published"] == doc["date_updated"]


class TestPrepareSubset:
    def test_matches(self):
        assert matches("cs.AI cs.LG")
        assert matches("math.OC stat.ML")
        assert matches("eess.IV cs.CV")
        assert not matches("hep-th gr-qc")
        assert not matches("stat.ME")

    def test_select_modes(self):
        lines = [f"l{i}" for i in range(100)]
        assert select(iter(lines), 5, "first") == ["l0", "l1", "l2", "l3", "l4"]
        assert select(iter(lines), 3, "last") == ["l97", "l98", "l99"]
        sample = select(iter(lines), 10, "random", seed=1)
        assert len(sample) == 10 and len(set(sample)) == 10
        assert sample == select(iter(lines), 10, "random", seed=1)  # reproductible
