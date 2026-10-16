"""Tests de la qualité des données (TP 4) : nettoyage et suites Great Expectations."""

import pandas as pd
import pytest

from src.quality.clean_arxiv import _json_safe, clean, diagnose


@pytest.fixture
def dirty_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"_id": "1", "arxiv_id": "2301.00001", "title": "A  title   with spaces",
         "abstract": "Line one\nline two  with   spaces and enough characters to be long.",
         "authors_flat": " Alice  Martin , Bob Chen ", "categories": ["cs.AI"], "primary_category": "cs.AI"},
        {"_id": "2", "arxiv_id": "2301.00001", "title": "Duplicate", "abstract": "x" * 80,
         "authors_flat": "Carl", "categories": ["cs.AI"], "primary_category": "cs.AI"},
        {"_id": "3", "arxiv_id": "2301.00002", "title": "Short abstract paper", "abstract": "Too short",
         "authors_flat": None, "categories": ["cs.LG", "stat.ML"], "primary_category": None},
        {"_id": "4", "arxiv_id": "2301.00003", "title": "No abstract at all", "abstract": None,
         "authors_flat": "Dana", "categories": ["hep-th"], "primary_category": "weird-cat"},
    ])


def test_diagnose_counts_problems(dirty_df):
    report = diagnose(dirty_df)
    assert report["duplicates"] == 1
    assert report["null_abstracts"] == 1
    assert report["short_abstracts"] == 1
    assert report["multi_space_titles"] == 1
    assert report["null_categories"] == 1
    assert report["unknown_categories"] == 1
    assert report["messy_authors"] == 1


def test_clean_fixes_what_can_be_fixed(dirty_df):
    cleaned = clean(dirty_df)
    after = diagnose(cleaned)
    assert len(cleaned) == 3
    assert after["duplicates"] == 0
    assert after["multi_space_titles"] == 0
    assert after["null_categories"] == 0
    assert after["messy_authors"] == 0
    row = cleaned.set_index("arxiv_id").loc["2301.00002"]
    assert row["primary_category"] == "cs.LG" and row["domain"] == "cs"
    assert bool(row["abstract_too_short"]) is True
    # un abstract absent reste null (on ne maquille pas la donnée)
    assert cleaned.set_index("arxiv_id").loc["2301.00003", "abstract"] is None
    assert cleaned.set_index("arxiv_id").loc["2301.00001", "authors_flat"] == "Alice Martin, Bob Chen"


def test_json_safe():
    import numpy as np

    assert _json_safe(float("nan")) is None
    assert _json_safe(np.int64(3)) == 3 and isinstance(_json_safe(np.int64(3)), int)
    assert _json_safe(np.bool_(True)) is True
    assert _json_safe(["a"]) == ["a"]


# ── Great Expectations (contexte éphémère : rien n'est écrit dans gx/) ─────────

@pytest.fixture
def ephemeral_gx(monkeypatch):
    gx = pytest.importorskip("great_expectations")
    monkeypatch.setattr("src.quality.validate_arxiv.get_context", lambda: gx.get_context())
    monkeypatch.setattr("src.quality.validate_hn.get_context", lambda: gx.get_context())


def _arxiv_df(n=20):
    return pd.DataFrame([{
        "arxiv_id": f"2301.{i:05d}", "title": f"A sufficiently long title {i}",
        "abstract": "word " * 30, "authors_flat": "A B", "categories": ["cs.AI"],
        "primary_category": "cs.AI", "date_published": "2023-01-05T00:00:00.000Z",
        "date_updated": "2023-02-01T00:00:00.000Z", "doi": None, "keywords": ["word"], "hn_score": None,
    } for i in range(n)])


def test_gx_arxiv_suite_passes_on_clean_data(ephemeral_gx):
    from src.quality.validate_arxiv import validate_arxiv_data

    _, result = validate_arxiv_data(_arxiv_df(), stage="enriched", min_rows=10)
    assert result.success


def test_gx_arxiv_suite_detects_problems(ephemeral_gx):
    from src.quality.validate_arxiv import validate_arxiv_data

    df = _arxiv_df()
    df.loc[0, "arxiv_id"] = None
    df.loc[1, "arxiv_id"] = df.loc[2, "arxiv_id"]               # doublon
    df.loc[3, "date_published"] = "1985-01-01T00:00:00.000Z"    # avant la création d'ArXiv
    _, result = validate_arxiv_data(df, stage="clean", min_rows=10)
    assert not result.success
    validation = list(result.run_results.values())[0]["validation_result"]
    failed = {(r["expectation_config"]["expectation_type"], r["expectation_config"]["kwargs"].get("column"))
              for r in validation["results"] if not r["success"]}
    assert ("expect_column_values_to_not_be_null", "arxiv_id") in failed
    assert ("expect_column_values_to_be_unique", "arxiv_id") in failed
    assert ("expect_column_values_to_be_between", "year_published") in failed


def test_gx_hn_suite(ephemeral_gx):
    from src.quality.validate_hn import validate_hn_data

    df = pd.DataFrame([
        {"hn_id": "1", "type": "story", "by": "a", "time": 1717000000, "title": "T", "url": None,
         "text": None, "score": 10, "descendants": 2, "arxiv_id_linked": "2301.07041"},
        {"hn_id": "2", "type": "comment", "by": "b", "time": 1717000100, "title": None, "url": None,
         "text": "hi", "score": None, "descendants": None, "arxiv_id_linked": None},
    ])
    _, result = validate_hn_data(df)
    assert result.success

    df.loc[1, "type"] = "unknown"
    _, result = validate_hn_data(df)
    assert not result.success
