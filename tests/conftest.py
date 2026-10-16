"""Fixtures partagées par les tests SciPulse."""

import pytest


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
def hn_story() -> dict:
    return {
        "id": 40000001, "type": "story", "by": "alice", "time": 1717000000,
        "title": "Attention Is All You Need (2017)", "url": "https://arxiv.org/abs/1706.03762v7",
        "score": 120, "descendants": 45, "kids": [40000002, 40000003],
    }


@pytest.fixture
def hn_comment() -> dict:
    return {
        "id": 40000002, "type": "comment", "by": "bob", "time": 1717000300, "parent": 40000001,
        "text": "See also https:&#x2F;&#x2F;arxiv.org&#x2F;pdf&#x2F;2005.14165 for GPT-3, really interesting.",
    }
