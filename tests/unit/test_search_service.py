"""
Tests unitaires du module src/search/search_service.py (TP 6, étape 10).

Elasticsearch est remplacé par un mock : on vérifie la REQUÊTE construite,
pas la réponse du moteur (les tests d'intégration sont dans tests/integration/).
"""

import json
from unittest.mock import patch

import pytest

EMPTY_RESPONSE = {"hits": {"hits": [], "total": {"value": 0}}, "aggregations": {}, "suggest": {}}


@pytest.fixture
def mock_es():
    with patch("src.search.search_service.Elasticsearch") as MockES:
        instance = MockES.return_value
        instance.search.return_value = EMPTY_RESPONSE
        yield instance


@pytest.fixture
def search(mock_es):
    from src.search.search_service import SciPulseSearch

    s = SciPulseSearch()
    s.es = mock_es
    return s


def sent_body(mock_es) -> dict:
    return mock_es.search.call_args[1]["body"]


class TestSciPulseSearch:
    """Tests des méthodes de base du module."""

    def test_full_text_builds_multi_match(self, search, mock_es):
        search.full_text("transformer attention mechanism")
        body = sent_body(mock_es)
        multi_match = body["query"]["function_score"]["query"]["bool"]["must"][0]["multi_match"]
        assert multi_match["query"] == "transformer attention mechanism"
        assert "title^3" in multi_match["fields"]
        assert multi_match["type"] == "cross_fields"

    def test_full_text_includes_highlight(self, search, mock_es):
        search.full_text("graph neural network")
        body = sent_body(mock_es)
        assert "abstract" in body["highlight"]["fields"]

    def test_full_text_with_hn_filter(self, search, mock_es):
        search.full_text("llm", min_hn_score=100)
        filters = sent_body(mock_es)["query"]["function_score"]["query"]["bool"]["filter"]
        assert {"range": {"hn_score": {"gte": 100}}} in filters

    def test_full_text_recency_decay(self, search, mock_es):
        search.full_text("diffusion", boost_recent=True)
        functions = sent_body(mock_es)["query"]["function_score"]["functions"]
        assert any("gauss" in f for f in functions)
        search.full_text("diffusion", boost_recent=False)
        functions = sent_body(mock_es)["query"]["function_score"]["functions"]
        assert not any("gauss" in f for f in functions)

    def test_mlt_uses_correct_id(self, search, mock_es):
        search.more_like_this("1706.03762")
        mlt = sent_body(mock_es)["query"]["more_like_this"]
        assert mlt["like"] == [{"_index": "arxiv-papers", "_id": "1706.03762"}]

    def test_significant_terms_with_category(self, search, mock_es):
        search.significant_terms(category="cs.CL")
        body = sent_body(mock_es)
        assert {"term": {"primary_category": "cs.CL"}} in body["query"]["bool"]["filter"]
        # champ texte → significant_text (pas de fielddata nécessaire)
        assert "significant_text" in body["aggs"]["echantillon"]["aggs"]["termes"]

    def test_significant_terms_on_keyword_field(self, search, mock_es):
        search.significant_terms(field="keywords", date_from="2023-01-01")
        body = sent_body(mock_es)
        assert "significant_terms" in body["aggs"]["echantillon"]["aggs"]["termes"]
        assert "2023-01-01" in json.dumps(body["query"])

    def test_suggest_sends_phrase_suggest(self, search, mock_es):
        search.suggest("convlutional nueral network")
        suggest = sent_body(mock_es)["suggest"]["title_suggest"]
        assert suggest["text"] == "convlutional nueral network"
        assert "phrase" in suggest

    def test_phrase_search_with_slop(self, search, mock_es):
        search.phrase_search("neural network architecture", slop=2)
        match_phrase = sent_body(mock_es)["query"]["match_phrase"]["abstract"]
        assert match_phrase == {"query": "neural network architecture", "slop": 2}

    def test_hn_discussions_uses_has_child(self, search, mock_es):
        search.hn_discussions("breakthrough")
        assert mock_es.search.call_args[1]["index"] == "hn-items"
        assert sent_body(mock_es)["query"]["has_child"]["type"] == "comment"


class TestCombinedSearch:
    """Tests de la méthode combined_search."""

    def test_combined_sends_multi_match(self, search, mock_es):
        """combined_search doit envoyer une requête multi_match."""
        search.combined_search("transformer attention")
        body_str = json.dumps(sent_body(mock_es))
        assert "multi_match" in body_str
        assert "transformer attention" in body_str

    def test_combined_includes_highlight(self, search, mock_es):
        """combined_search doit inclure le highlighting."""
        search.combined_search("test query")
        call_body = sent_body(mock_es)
        assert "highlight" in call_body
        assert "abstract" in call_body["highlight"]["fields"]

    def test_combined_includes_aggregations(self, search, mock_es):
        """combined_search doit inclure les agrégations."""
        search.combined_search("test query")
        call_body = sent_body(mock_es)
        assert "aggs" in call_body
        assert "termes_emergents" in call_body["aggs"]
        assert "par_categorie" in call_body["aggs"]

    def test_combined_includes_suggestion(self, search, mock_es):
        """combined_search doit inclure le suggest."""
        search.combined_search("some query")
        call_body = sent_body(mock_es)
        assert "suggest" in call_body
        assert call_body["suggest"]["correction"]["text"] == "some query"

    def test_combined_with_category_filter(self, search, mock_es):
        """combined_search avec catégories doit ajouter un filtre terms."""
        search.combined_search("test", categories=["cs.AI", "cs.CL"])
        body_str = json.dumps(sent_body(mock_es))
        assert "cs.AI" in body_str
        assert "cs.CL" in body_str

    def test_combined_with_date_filter(self, search, mock_es):
        """combined_search avec date_from doit ajouter un filtre range."""
        search.combined_search("test", date_from="2023-01-01")
        body_str = json.dumps(sent_body(mock_es))
        assert "2023-01-01" in body_str

    def test_combined_without_recency_boost(self, search, mock_es):
        search.combined_search("test", boost_recent=False)
        assert "bool" in sent_body(mock_es)["query"]


class TestFormatResults:
    """Le formatage terminal doit supporter les agrégations directes et sous sampler."""

    def test_format_full_response(self):
        from src.search.search_service import SciPulseSearch

        response = {
            "hits": {
                "total": {"value": 1},
                "hits": [{
                    "_score": 12.5,
                    "_source": {"title": "Attention Is All You Need", "primary_category": "cs.CL",
                                "date_published": "2017-06-12T17:57:34.000Z", "hn_score": 120},
                    "highlight": {"abstract": ["the <mark>Transformer</mark> architecture"]},
                }],
            },
            "aggregations": {
                "par_categorie": {"buckets": [{"key": "cs.CL", "doc_count": 1}]},
                "termes_emergents": {"doc_count": 1, "termes": {"buckets": [
                    {"key": "transformer", "doc_count": 1, "score": 0.9}]}},
            },
            "suggest": {"correction": [{"options": [{"text": "transformer attention"}]}]},
        }
        text = SciPulseSearch.format_results(response)
        assert "Attention Is All You Need" in text
        assert "HN:120" in text
        assert "cs.CL" in text
        assert "transformer" in text
        assert "Vouliez-vous dire : transformer attention" in text

    def test_format_empty_response(self):
        from src.search.search_service import SciPulseSearch

        assert "0 résultats" in SciPulseSearch.format_results(EMPTY_RESPONSE)
