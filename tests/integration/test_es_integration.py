"""
Tests d'intégration contre un vrai Elasticsearch (stack Docker lancée).

    docker compose up -d elasticsearch
    pytest -m integration -v

Ils créent un index temporaire `scipulse-test-arxiv` avec le mapping de production,
y indexent quelques articles, puis vérifient l'analyseur et le service de recherche.
Ils sont ignorés automatiquement si Elasticsearch n'est pas joignable.
"""

import pytest
from elasticsearch import Elasticsearch

from src.config import ES_HOST
from src.ingestion.arxiv_bulk_ingest import transform_article
from src.ingestion.reindex_arxiv import transform_for_reindex
from src.utils.mappings import ARXIV_SETTINGS, create_index

pytestmark = pytest.mark.integration
TEST_INDEX = "scipulse-test-arxiv"

ARTICLES = [
    ("2101.00001", "Convolutional neural networks for image classification",
     "We study CNN architectures for large scale image classification tasks and analyse their robustness.", "cs.CV"),
    ("2101.00002", "A survey of convolutional neural network pruning",
     "Pruning convolutional neural networks reduces the cost of inference on embedded devices.", "cs.CV"),
    ("2101.00003", "Deep reinforcement learning for robot manipulation",
     "We train RL agents for robotic grasping with sparse rewards in simulation and on real robots.", "cs.RO"),
    ("2101.00004", "Large language models as zero shot reasoners",
     "LLMs can reason step by step when prompted, without task specific fine tuning.", "cs.CL"),
]


@pytest.fixture(scope="module")
def es():
    client = Elasticsearch(ES_HOST, request_timeout=10)
    try:
        client.info()
    except Exception:
        pytest.skip(f"Elasticsearch non joignable sur {ES_HOST}")
    create_index(client, TEST_INDEX, ARXIV_SETTINGS, recreate=True)
    for arxiv_id, title, abstract, cat in ARTICLES:
        raw = {"id": arxiv_id, "title": title, "abstract": abstract, "categories": cat,
               "authors": "Alice Martin and Bob Chen", "update_date": "2021-01-15",
               "versions": [{"version": "v1", "created": "Fri, 1 Jan 2021 10:00:00 GMT"}]}
        doc = transform_for_reindex({"_source": transform_article(raw)})
        client.index(index=TEST_INDEX, id=arxiv_id, document=doc)
    client.indices.refresh(index=TEST_INDEX)
    yield client
    client.indices.delete(index=TEST_INDEX, ignore_unavailable=True)


def tokens(es, text, analyzer="scientific_english"):
    return [t["token"] for t in es.indices.analyze(index=TEST_INDEX, analyzer=analyzer, text=text)["tokens"]]


def test_analyzer_synonyms_stopwords_stemming(es):
    toks = tokens(es, "A new CNN-based approach to reinforcement learning for NLP tasks")
    assert "convolut" in toks and "neural" in toks          # CNN étendu + stemmé
    assert "rl" in toks                                      # synonyme inverse
    assert "learn" in toks and "task" in toks                # stemming
    assert "a" not in toks and "to" not in toks              # mots vides


def test_stemming_network_networks(es):
    count = lambda q: es.count(index=TEST_INDEX, query={"match": {"abstract": q}})["count"]
    assert count("network") == count("networks") > 0


def test_synonym_search(es):
    hits = es.search(index=TEST_INDEX, query={"match": {"abstract": "convolutional neural network"}})["hits"]["hits"]
    assert "2101.00001" in {h["_id"] for h in hits}          # l'abstract dit « CNN »


def test_nested_authors(es):
    q = {"nested": {"path": "authors", "query": {"match": {"authors.name": "Martin"}}}}
    assert es.count(index=TEST_INDEX, query=q)["count"] == len(ARTICLES)


def test_search_service_end_to_end(es):
    from src.search.search_service import SciPulseSearch

    s = SciPulseSearch(index=TEST_INDEX)
    r = s.full_text("convolutional network pruning", boost_recent=False)
    assert r["hits"]["hits"][0]["_id"] == "2101.00002"
    assert "highlight" in r["hits"]["hits"][0]

    assert s.more_like_this("2101.00001", size=3)["hits"]["total"]["value"] >= 0
    assert "title_suggest" in s.suggest("convlutional nueral network")["suggest"]
    auto = s.autocomplete("deep rein")["suggest"]["title_autocomplete"][0]["options"]
    assert auto and auto[0]["text"].lower().startswith("deep rein")
    trends = s.significant_terms(category="cs.CV", min_doc_count=1)
    assert "echantillon" in trends["aggregations"]
    combined = s.combined_search("reinforcement learning robotics", categories=["cs.RO"])
    assert combined["hits"]["total"]["value"] == 1
