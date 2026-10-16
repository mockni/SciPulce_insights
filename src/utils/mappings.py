"""
SciPulse — Mappings des index Elasticsearch (couche Gold = serving)

  - arxiv-papers           : Module 1, articles ArXiv Silver (exploration plein texte)
  - arxiv-papers-enriched  : Module 2, Gold finale écrite par Spark (ArXiv × HN × citations)
  - pipeline-logs          : observabilité (défini dans src/monitoring/events.py)

Usage :
    python -m src.utils.mappings                 # crée les index absents
    python -m src.utils.mappings --recreate      # ⚠️ supprime et recrée (perte des données)
    python -m src.utils.mappings --only arxiv-papers --recreate
"""

import argparse
import copy
import sys

from elasticsearch import Elasticsearch

from src.config import ES_HOST, INDEX_ARXIV, INDEX_ENRICHED, INDEX_PIPELINE_LOGS
from src.monitoring.events import PIPELINE_LOGS_SETTINGS

DATE_FORMAT = "strict_date_optional_time||yyyy-MM-dd||epoch_millis"

# Synonymes scientifiques (format Solr : termes équivalents séparés par des virgules).
# ⚠️ Aucun mot vide (of, the, for…) dans les règles : le filtre de synonymes analyse
# ses règles avec les filtres qui le précèdent (dont english_stop) et échouerait.
SCIENTIFIC_SYNONYMS = [
    "cnn, convolutional neural network",
    "rnn, recurrent neural network",
    "lstm, long short term memory",
    "gnn, graph neural network",
    "gan, generative adversarial network",
    "vae, variational autoencoder",
    "llm, large language model",
    "nlp, natural language processing",
    "nmt, neural machine translation",
    "ner, named entity recognition",
    "rl, reinforcement learning",
    "rlhf, reinforcement learning from human feedback",
    "rag, retrieval augmented generation",
    "ml, machine learning",
    "dl, deep learning",
    "ai, artificial intelligence",
    "bert, bidirectional encoder representations from transformers",
    "gpt, generative pre trained transformer",
    "vit, vision transformer",
    "mlp, multilayer perceptron",
    "svm, support vector machine",
    "sgd, stochastic gradient descent",
    "pca, principal component analysis",
    "asr, automatic speech recognition",
    "qa, question answering",
    "kg, knowledge graph",
]

ANALYSIS = {
    "filter": {
        "english_stemmer": {"type": "stemmer", "language": "english"},
        "english_stop": {"type": "stop", "stopwords": "_english_"},
        # Indexation : `synonym` (expansion stockée, visible avec _analyze)
        "scientific_synonyms": {"type": "synonym", "synonyms": SCIENTIFIC_SYNONYMS, "lenient": True},
        # Recherche : `synonym_graph` gère les synonymes multi-mots
        "scientific_synonyms_graph": {"type": "synonym_graph", "synonyms": SCIENTIFIC_SYNONYMS,
                                      "lenient": True},
        "shingle_filter": {"type": "shingle", "min_shingle_size": 2, "max_shingle_size": 3},
    },
    "analyzer": {
        # lowercase → stopwords → synonymes → stemming (les synonymes AVANT le stemmer)
        "scientific_english": {
            "type": "custom", "tokenizer": "standard",
            "filter": ["lowercase", "english_stop", "scientific_synonyms", "english_stemmer"],
        },
        "scientific_english_search": {
            "type": "custom", "tokenizer": "standard",
            "filter": ["lowercase", "english_stop", "scientific_synonyms_graph", "english_stemmer"],
        },
        # Phrase suggester (« convlutional nueral network ») : pas de stemming
        "trigram": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "shingle_filter"]},
    },
}

ARXIV_PROPERTIES = {
    "arxiv_id": {"type": "keyword"},
    "title": {
        "type": "text",
        "analyzer": "scientific_english",
        "search_analyzer": "scientific_english_search",
        "fields": {
            "keyword": {"type": "keyword", "ignore_above": 512},
            "suggest": {"type": "completion", "analyzer": "simple"},
            "trigram": {"type": "text", "analyzer": "trigram"},
        },
    },
    "abstract": {
        "type": "text",
        "analyzer": "scientific_english",
        "search_analyzer": "scientific_english_search",
        "term_vector": "with_positions_offsets",   # highlighting rapide + more_like_this
    },
    "authors": {
        "type": "nested",
        "properties": {
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "affiliation": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        },
    },
    "authors_flat": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}}},
    "n_authors": {"type": "integer"},
    "submitter": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "categories": {"type": "keyword"},
    "primary_category": {"type": "keyword"},
    "domain": {"type": "keyword"},
    "n_categories": {"type": "integer"},
    "date_published": {"type": "date", "format": DATE_FORMAT},
    "date_updated": {"type": "date", "format": DATE_FORMAT},
    "year_published": {"type": "integer"},
    "doi": {"type": "keyword"},
    "arxiv_doi": {"type": "keyword"},
    "journal_ref": {"type": "text"},
    "versions_count": {"type": "integer"},
    "abstract_length": {"type": "integer"},
    "abstract_too_short": {"type": "boolean"},
    "license": {"type": "keyword"},
}

ARXIV_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "analysis": ANALYSIS},
    # dynamic: false → un champ imprévu reste dans _source mais n'est pas indexé
    "mappings": {"dynamic": False, "properties": ARXIV_PROPERTIES},
}

# ── Gold finale (Module 2) : mêmes champs ArXiv + enrichissements Spark ──────
ENRICHED_PROPERTIES = {
    **{k: v for k, v in copy.deepcopy(ARXIV_PROPERTIES).items() if k != "authors"},
    # Les auteurs sont écrits par Spark en simple tableau de noms
    "author_names": {"type": "keyword"},
    "keywords": {"type": "keyword"},                        # TF-IDF Spark MLlib
    # Signal Hacker News
    "hn_posts": {"type": "integer"},
    "hn_score_total": {"type": "integer"},
    "hn_score_max": {"type": "integer"},
    "hn_comments_total": {"type": "integer"},
    "hn_first_seen": {"type": "date"},
    "has_hn": {"type": "boolean"},
    # Citations (OpenAlex)
    "openalex_id": {"type": "keyword"},
    "cited_by_count": {"type": "integer"},
    "citations_last_2y": {"type": "integer"},
    "citation_velocity": {"type": "float"},
    "citation_percentile": {"type": "float"},
    "openalex_field": {"type": "keyword"},
    "openalex_subfield": {"type": "keyword"},
    "has_citations_data": {"type": "boolean"},
    "citation_match_key": {"type": "keyword"},              # arxiv_doi | journal_doi
    # Indicateurs de catégorie (window functions)
    "category_publications_year": {"type": "integer"},
    "category_yoy_growth": {"type": "float"},
    "category_avg_velocity": {"type": "float"},
    # Score d'impact composite
    "score_citations": {"type": "float"},
    "score_hn": {"type": "float"},
    "score_recency": {"type": "float"},
    "impact_score": {"type": "float"},
    "impact_rank_in_category": {"type": "integer"},
    "segment": {"type": "keyword"},
    "enriched_at": {"type": "date"},
}

ENRICHED_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "analysis": ANALYSIS},
    "mappings": {"dynamic": False, "properties": ENRICHED_PROPERTIES},
}

# Gabarit des index pipeline-logs-airflow-* (alimentés par Logstash) : chaînes en keyword,
# pour éviter des conflits de type avec pipeline-logs dans le data view « pipeline-logs* »
AIRFLOW_LOGS_TEMPLATE = {
    "index_patterns": [f"{INDEX_PIPELINE_LOGS}-airflow-*"],
    "priority": 200,
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic_templates": [{"strings_as_keywords": {
                "match_mapping_type": "string",
                "mapping": {"type": "keyword", "ignore_above": 1024}}}],
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "log_message": {"type": "text"},
            },
        },
    },
}

INDEX_DEFINITIONS = {
    INDEX_ARXIV: ARXIV_SETTINGS,
    INDEX_ENRICHED: ENRICHED_SETTINGS,
    INDEX_PIPELINE_LOGS: PIPELINE_LOGS_SETTINGS,
}


def create_index(es: Elasticsearch, name: str, definition: dict, recreate: bool = False) -> bool:
    """Crée un index. Retourne True si l'index a été créé."""
    if es.indices.exists(index=name):
        if not recreate:
            print(f"  ⏭  Index '{name}' existe déjà (utilisez --recreate pour le recréer).")
            return False
        es.indices.delete(index=name)
        print(f"  🗑  Index '{name}' supprimé.")
    es.indices.create(index=name, settings=definition["settings"], mappings=definition["mappings"])
    print(f"  ✅ Index '{name}' créé.")
    return True


def deploy_all(es: Elasticsearch, recreate: bool = False, only: list[str] | None = None):
    for name, definition in INDEX_DEFINITIONS.items():
        if only and name not in only:
            continue
        create_index(es, name, definition, recreate=recreate)
    if not only:
        es.indices.put_index_template(name="pipeline-logs-airflow", **AIRFLOW_LOGS_TEMPLATE)
        print("  ✅ Gabarit d'index 'pipeline-logs-airflow-*' déployé.")


def main():
    parser = argparse.ArgumentParser(description="SciPulse — création des index ES")
    parser.add_argument("--recreate", action="store_true", help="Supprimer puis recréer les index")
    parser.add_argument("--only", nargs="*", help="Limiter à certains index")
    args = parser.parse_args()

    es = Elasticsearch(ES_HOST)
    try:
        es.info()
    except Exception as e:
        print(f"❌ Elasticsearch non accessible ({ES_HOST}) : {e}")
        sys.exit(1)
    print()
    deploy_all(es, recreate=args.recreate, only=args.only)
    print("\nMappings déployés avec succès.")


if __name__ == "__main__":
    main()
