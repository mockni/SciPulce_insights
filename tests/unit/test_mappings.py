"""Cohérence des mappings Elasticsearch avec le code qui les alimente."""

from src.silver.schemas import ARXIV_SCHEMA
from src.spark.insights_job import GOLD_COLUMNS
from src.utils.mappings import (
    ARXIV_PROPERTIES,
    ARXIV_SETTINGS,
    ENRICHED_PROPERTIES,
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
    for name, analyzer in analysis["analyzer"].items():
        for f in analyzer["filter"]:
            assert f in analysis["filter"] or f in {"lowercase"}, f"{name} → filtre inconnu {f}"


def test_synonyms_before_stemmer():
    chain = ARXIV_SETTINGS["settings"]["analysis"]["analyzer"]["scientific_english"]["filter"]
    assert chain.index("scientific_synonyms") < chain.index("english_stemmer")


def test_gold_partial_index_covers_silver_schema():
    """Chaque champ du mapping arxiv-papers existe dans le schéma Silver (index_arxiv les copie)."""
    silver_fields = {f.name for f in ARXIV_SCHEMA}
    assert set(ARXIV_PROPERTIES) <= silver_fields


def test_enriched_mapping_matches_spark_output():
    """Le DataFrame Gold écrit par Spark et le mapping arxiv-papers-enriched sont alignés."""
    assert set(GOLD_COLUMNS) <= set(ENRICHED_PROPERTIES), set(GOLD_COLUMNS) - set(ENRICHED_PROPERTIES)
    for field in ("impact_score", "segment", "cited_by_count", "hn_score_total", "keywords"):
        assert field in GOLD_COLUMNS


def test_search_fields():
    props = ARXIV_PROPERTIES
    assert props["title"]["fields"]["suggest"]["type"] == "completion"
    assert props["title"]["fields"]["trigram"]["analyzer"] == "trigram"
    assert props["abstract"]["term_vector"] == "with_positions_offsets"
    assert props["authors"]["type"] == "nested"


def test_all_indexes_have_single_shard_no_replica():
    for name, definition in INDEX_DEFINITIONS.items():
        assert definition["settings"]["number_of_shards"] == 1, name
        assert definition["settings"]["number_of_replicas"] == 0, name
