"""Règles de nettoyage Silver (src/silver/cleaning.py et src/silver/citations.py)."""

from collections import Counter

import pandas as pd

from src.silver.citations import clean_citations, clean_work
from src.silver.cleaning import (
    arxiv_bronze_frame,
    classify_post,
    clean_arxiv,
    clean_hn,
    detect_arxiv_link,
    diagnose_arxiv_bronze,
    parse_jsonl,
    url_domain,
)


def test_parse_jsonl_counts_errors():
    report = Counter()
    records = parse_jsonl(['{"id": "1"}', "{ cassé", "[1, 2]", '{"id": "2"}'], report)
    assert [r["id"] for r in records] == ["1", "2"]
    assert report["json_errors"] == 2


def test_clean_arxiv_record_fields(raw_article):
    df = clean_arxiv([raw_article])
    row = df.iloc[0]
    assert row["arxiv_id"] == "1706.03762"
    assert row["abstract"].startswith("The dominant") and "\n" not in row["abstract"]
    assert list(row["categories"]) == ["cs.CL", "cs.LG"]
    assert row["primary_category"] == "cs.CL" and row["domain"] == "cs"
    assert row["date_published"] == pd.Timestamp("2017-06-12T17:57:34", tz="UTC")
    assert row["year_published"] == 2017 and row["versions_count"] == 2
    assert row["arxiv_doi"] == "10.48550/arxiv.1706.03762"
    assert row["n_authors"] == 8 and row["authors"][0]["name"] == "Ashish Vaswani"


def test_clean_arxiv_rules(raw_article):
    newer = {**raw_article, "update_date": "2024-01-01", "title": "Attention v2"}
    out_of_scope = {**raw_article, "id": "1111.11111", "categories": "hep-th"}
    no_abstract = {**raw_article, "id": "2222.22222", "abstract": "   "}
    no_id = {**raw_article, "id": None}
    bad_doi = {**raw_article, "id": "3333.33333", "doi": "pas-un-doi"}
    report = Counter()
    df = clean_arxiv([raw_article, newer, out_of_scope, no_abstract, no_id, bad_doi],
                     scope_prefixes=("cs.", "stat.ML"), report=report)
    assert report["out_of_scope"] == 1 and report["missing_id"] == 1
    assert report["duplicates"] == 1 and report["missing_abstract"] == 1 and report["invalid_doi"] == 1
    assert set(df["arxiv_id"]) == {"1706.03762", "3333.33333"}
    assert df.set_index("arxiv_id").loc["1706.03762", "title"] == "Attention v2"    # plus récente gardée
    assert df.set_index("arxiv_id").loc["3333.33333", "doi"] is None


def test_clean_arxiv_empty_input_keeps_schema():
    df = clean_arxiv([])
    assert df.empty and "arxiv_id" in df.columns


def test_diagnose_bronze(raw_article):
    df = arxiv_bronze_frame([raw_article, raw_article, {**raw_article, "update_date": "03/2020", "abstract": None}])
    report = diagnose_arxiv_bronze(df, scope_prefixes=("cs.",))
    assert report["duplicate_ids"] == 2 and report["bad_date_format"] == 1 and report["null_abstract"] == 1
    assert report["multiline_titles"] == 0 and report["out_of_scope"] == 0


def test_hn_helpers():
    assert classify_post({"type": "story", "title": "Show HN: x", "url": "u"}) == "show_hn"
    assert classify_post({"type": "story", "title": "Ask HN: y"}) == "ask_hn"
    assert classify_post({"type": "story", "title": "Paper", "url": "u"}) == "article"
    assert classify_post({"type": "comment"}) == "comment"
    assert url_domain("https://www.arxiv.org/abs/1") == "arxiv.org" and url_domain("pas une url") is None
    assert detect_arxiv_link({"url": "https://arxiv.org/abs/2301.07041v2"}) == ("2301.07041", "url")
    assert detect_arxiv_link({"text": "voir https:&#x2F;&#x2F;arxiv.org&#x2F;abs&#x2F;2005.14165"}) == ("2005.14165", "text")


def test_clean_hn_dedupe_keeps_latest_measure(hn_items):
    first = [{**it, "_fetched_at": "2026-10-16T10:00:00+00:00", "_feed": "newstories"} for it in hn_items]
    refreshed = {**hn_items[0], "score": 1200, "_fetched_at": "2026-10-16T11:00:00+00:00", "_feed": "topstories"}
    report = Counter()
    df = clean_hn(first + [refreshed], report).set_index("hn_id")
    assert report["deleted_or_dead"] == 1 and report["duplicates"] == 1
    assert df.loc["41000001", "score"] == 1200 and df.loc["41000001", "feed"] == "topstories"
    assert pd.isna(df.loc["41000101", "score"])                  # un commentaire n'a pas de score
    assert df.loc["41000101", "text"].startswith("Interesting, compare with this")
    assert df.loc["41000001", "published_at"] == pd.Timestamp(1717000000, unit="s", tz="UTC")


def test_clean_citations(openalex_works):
    work = clean_work({**openalex_works[0], "_fetched_at": "2026-10-16T10:00:00Z"})
    assert work["openalex_id"].startswith("W") and work["doi"] == work["doi"].lower()
    assert work["match_key_type"] == "arxiv_doi" and work["arxiv_id"]
    assert work["field"] == "Computer Science"
    report = Counter()
    df = clean_citations(openalex_works + [openalex_works[0], {"id": None}], report)
    assert report["duplicates"] == 1 and report["missing_id"] == 1
    assert len(df) == len(openalex_works)
    journal = df[df["match_key_type"] == "journal_doi"]
    assert list(journal["doi"]) == ["10.5555/example.2023.001"] and journal["arxiv_id"].isna().all()
