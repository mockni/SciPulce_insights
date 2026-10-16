"""Cohérence des mappings (TP 3) avec le code qui les utilise."""

from src.utils.mappings import (
    ARXIV_SETTINGS,
    HN_SETTINGS,
    INDEX_DEFINITIONS,
    SCIENTIFIC_SYNONYMS,
)

# Liste _english_ de Lucene : un mot vide dans une règle de synonyme ferait échouer la création de l'index
LUCENE_ENGLISH_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
}


def test_synonyms_contain_no_stopwords():
    for rule in SCIENTIFIC_SYNONYMS:
        tokens = {t for term in rule.split(",") for t in term.split()}
        assert not tokens & LUCENE_ENGLISH_STOPWORDS, rule


def test_custom_analyzers_reference_declared_filters():
    analysis = ARXIV_SETTINGS["settings"]["analysis"]
    builtin = {"lowercase", "stop", "porter_stem", "asciifolding"}
    for name, analyzer in analysis["analyzer"].items():
        for f in analyzer["filter"]:
            assert f in analysis["filter"] or f in builtin, f"{name} → filtre inconnu {f}"


def test_synonyms_before_stemmer():
    chain = ARXIV_SETTINGS["settings"]["analysis"]["analyzer"]["scientific_english"]["filter"]
    assert chain.index("scientific_synonyms") < chain.index("english_stemmer")


def test_fields_used_by_search_service_exist():
    props = ARXIV_SETTINGS["mappings"]["properties"]
    assert props["title"]["fields"]["suggest"]["type"] == "completion"
    assert props["title"]["fields"]["trigram"]["analyzer"] == "trigram"
    assert props["abstract"]["term_vector"] == "with_positions_offsets"
    assert props["authors"]["type"] == "nested"
    for field in ["keywords", "hn_score", "hn_comments_count", "date_published", "primary_category"]:
        assert field in props


def test_hn_mapping_join_and_epoch():
    props = HN_SETTINGS["mappings"]["properties"]
    assert props["relation"] == {"type": "join", "relations": {"story": "comment"}}
    assert props["time"]["format"] == "epoch_second"


def test_all_indexes_have_single_shard_no_replica():
    for name, definition in INDEX_DEFINITIONS.items():
        assert definition["settings"]["number_of_shards"] == 1, name
        assert definition["settings"]["number_of_replicas"] == 0, name
