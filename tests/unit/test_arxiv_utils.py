"""Tests des fonctions pures de src/utils/arxiv_utils.py."""

import pytest

from src.utils.arxiv_utils import (
    clean_text,
    domain_of,
    extract_arxiv_ids,
    first_version_date,
    normalize_arxiv_id,
    parse_authors,
    parse_update_date,
    parse_version_date,
    primary_category,
    split_categories,
)


def test_split_categories_string_and_list():
    assert split_categories("cs.AI cs.LG stat.ML") == ["cs.AI", "cs.LG", "stat.ML"]
    assert split_categories(["cs.AI"]) == ["cs.AI"]
    assert split_categories(None) == []
    assert primary_category("cs.CL cs.LG") == "cs.CL"


@pytest.mark.parametrize("category, domain", [("cs.AI", "cs"), ("stat.ML", "stat"), ("quant-ph", "quant-ph"), (None, None)])
def test_domain_of(category, domain):
    assert domain_of(category) == domain


def test_clean_text():
    assert clean_text("  Deep\n  learning   models \n") == "Deep learning models"
    assert clean_text(None) is None


def test_dates_match_logstash_format():
    assert parse_update_date("2023-02-07") == "2023-02-07T00:00:00.000Z"
    assert parse_update_date("not a date") is None
    assert parse_version_date("Mon, 2 Apr 2007 19:18:42 GMT") == "2007-04-02T19:18:42.000Z"
    versions = [{"version": "v1", "created": "Mon, 12 Jun 2017 17:57:34 GMT"}]
    assert first_version_date(versions) == "2017-06-12T17:57:34.000Z"
    assert first_version_date([]) is None


@pytest.mark.parametrize("raw, expected", [
    ("2301.07041v3", "2301.07041"),
    ("2301.07041", "2301.07041"),
    ("cs/0112017v1", "cs/0112017"),
    ("math.GT/0309136", "math.GT/0309136"),
])
def test_normalize_arxiv_id(raw, expected):
    assert normalize_arxiv_id(raw) == expected


@pytest.mark.parametrize("text, expected", [
    ("https://arxiv.org/abs/2301.07041", ["2301.07041"]),
    ("https://arxiv.org/abs/2301.07041v2", ["2301.07041"]),
    ("http://www.arxiv.org/pdf/1706.03762.pdf", ["1706.03762"]),
    ("https://arxiv.org/html/2402.12345v1", ["2402.12345"]),
    ("https://arxiv.org/abs/cs/0112017", ["cs/0112017"]),
    ("Voir arxiv.org/abs/2005.14165 et arxiv.org/abs/2005.14165v4", ["2005.14165"]),
    ("https://github.com/foo/bar", []),
    (None, []),
])
def test_extract_arxiv_ids(text, expected):
    assert extract_arxiv_ids(text) == expected


def test_parse_authors_from_authors_parsed(raw_article):
    authors, flat = parse_authors(raw_article["authors"], raw_article["authors_parsed"])
    assert len(authors) == 8
    assert authors[0] == {"name": "Ashish Vaswani", "affiliation": None}
    assert flat.startswith("Ashish Vaswani, Noam Shazeer")


def test_parse_authors_parsed_with_affiliation():
    authors, _ = parse_authors(None, [["Balázs", "C.", "", "Michigan State University"]])
    assert authors == [{"name": "C. Balázs", "affiliation": "Michigan State University"}]


def test_parse_authors_from_raw_string():
    authors, flat = parse_authors("Alice Martin (MIT, Cambridge), Bob Chen and Carl Dupont", None)
    assert [a["name"] for a in authors] == ["Alice Martin", "Bob Chen", "Carl Dupont"]
    assert authors[0]["affiliation"] == "MIT, Cambridge"
    assert flat == "Alice Martin, Bob Chen, Carl Dupont"


def test_parse_authors_empty():
    assert parse_authors("", None) == ([], "")
