"""Fixtures partagées par les tests SciPulse."""

import json
import os
from pathlib import Path

import pytest

# Les tests ne publient pas d'événements dans Elasticsearch
os.environ.setdefault("SCIPULSE_MONITORING", "0")

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_article() -> dict:
    """Une ligne du dump Kaggle « arxiv-metadata-oai-snapshot.json » (format d'origine)."""
    return {
        "id": "1706.03762",
        "submitter": "Ashish Vaswani",
        "authors": "Ashish Vaswani, Noam Shazeer, Niki Parmar, Jakob Uszkoreit, Llion Jones,\n  Aidan N. Gomez, Lukasz Kaiser, Illia Polosukhin",
        "title": "Attention Is All You Need",
        "comments": "15 pages, 5 figures",
        "journal-ref": None,
        "doi": None,
        "report-no": None,
        "categories": "cs.CL cs.LG",
        "license": "http://arxiv.org/licenses/nonexclusive-distrib/1.0/",
        "abstract": "  The dominant sequence transduction models are based on complex recurrent or\nconvolutional neural networks in an encoder-decoder configuration. We propose a new\nsimple network architecture, the Transformer.\n",
        "versions": [
            {"version": "v1", "created": "Mon, 12 Jun 2017 17:57:34 GMT"},
            {"version": "v2", "created": "Mon, 19 Jun 2017 16:49:45 GMT"},
        ],
        "update_date": "2023-08-03",
        "authors_parsed": [
            ["Vaswani", "Ashish", ""], ["Shazeer", "Noam", ""], ["Parmar", "Niki", ""],
            ["Uszkoreit", "Jakob", ""], ["Jones", "Llion", ""], ["Gomez", "Aidan N.", ""],
            ["Kaiser", "Lukasz", ""], ["Polosukhin", "Illia", ""],
        ],
    }


@pytest.fixture
def hn_items() -> list[dict]:
    return json.loads((FIXTURES / "hn_items.json").read_text())


@pytest.fixture
def openalex_works() -> list[dict]:
    return json.loads((FIXTURES / "openalex_works.json").read_text())


@pytest.fixture
def arxiv_lines() -> list[str]:
    return [l for l in (FIXTURES / "arxiv_bronze.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]


@pytest.fixture
def expected() -> dict:
    return json.loads((FIXTURES / "expected.json").read_text())


@pytest.fixture
def s3_lake(monkeypatch):
    """Faux MinIO (moto) : les buckets du data lake, sans serveur."""
    moto = pytest.importorskip("moto")
    for var, value in [("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"),
                       ("AWS_DEFAULT_REGION", "us-east-1")]:
        monkeypatch.setenv(var, value)
    from src.lake import s3

    with moto.mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        monkeypatch.setattr(s3, "get_s3", lambda: client)   # restauré automatiquement après le test
        s3.ensure_buckets()
        yield s3
