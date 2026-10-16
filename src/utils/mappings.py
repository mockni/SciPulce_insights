"""
SciPulse — Mappings et settings des index Elasticsearch (TP 3)

Index créés :
  - arxiv-papers    : articles ArXiv, mapping optimisé pour la fouille de texte
  - hn-items        : posts et commentaires Hacker News (join field story → comment)
  - arxiv-hn-links  : liens croisés ArXiv ↔ HN (application-side join)
  - pipeline-logs   : métriques d'observabilité du pipeline (TP 8)

Usage :
    python -m src.utils.mappings                 # crée les index absents
    python -m src.utils.mappings --recreate      # ⚠️ supprime et recrée (perte des données)
    python -m src.utils.mappings --only arxiv-papers --recreate
"""

import argparse
import sys

from elasticsearch import Elasticsearch

from src.config import (
    ES_HOST,
    INDEX_ARXIV,
    INDEX_HN,
    INDEX_LINKS,
    INDEX_PIPELINE_LOGS,
)

DATE_FORMAT = "strict_date_optional_time||yyyy-MM-dd||epoch_millis"

# Synonymes scientifiques (format Solr : termes équivalents séparés par des virgules).
# ⚠️ Pas de mots vides (of, the, for…) dans les règles : le filtre de synonymes
# analyse ses règles avec les filtres qui le précèdent (dont english_stop).
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

# ═════════════════════════════════════════════════════════════════════════════
#  ARXIV
# ═════════════════════════════════════════════════════════════════════════════

ARXIV_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "english_stemmer": {"type": "stemmer", "language": "english"},
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                # Indexation : `synonym` (expansion stockée dans l'index → visible via _analyze)
                "scientific_synonyms": {
                    "type": "synonym",
                    "synonyms": SCIENTIFIC_SYNONYMS,
                    "lenient": True,
                },
                # Recherche : `synonym_graph` gère correctement les synonymes multi-mots
                "scientific_synonyms_graph": {
                    "type": "synonym_graph",
                    "synonyms": SCIENTIFIC_SYNONYMS,
                    "lenient": True,
                },
                "shingle_filter": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 3,
                },
            },
            "analyzer": {
                # lowercase → stopwords → synonymes → stemming (les synonymes AVANT le stemmer)
                "scientific_english": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "scientific_synonyms", "english_stemmer"],
                },
                "scientific_english_search": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "scientific_synonyms_graph", "english_stemmer"],
                },
                # Pour le phrase suggester (correction « convlutional nueral network »)
                "trigram": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "shingle_filter"],
                },
            },
        },
    },
    "mappings": {
        "dynamic": False,  # les champs non déclarés restent dans _source mais ne sont pas indexés
        "properties": {
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
                "term_vector": "with_positions_offsets",
            },
            "authors": {
                "type": "nested",
                "properties": {
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "affiliation": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                },
            },
            "authors_flat": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            },
            "submitter": {"type": "keyword"},
            "categories": {"type": "keyword"},
            "primary_category": {"type": "keyword"},
            "domain": {"type": "keyword"},
            "date_published": {"type": "date", "format": DATE_FORMAT},
            "date_updated": {"type": "date", "format": DATE_FORMAT},
            "doi": {"type": "keyword"},
            "journal_ref": {"type": "text"},
            "versions_count": {"type": "integer"},
            "abstract_length": {"type": "integer"},
            "abstract_too_short": {"type": "boolean"},
            # Enrichissement (Jour 3)
            "keywords": {"type": "keyword"},
            "hn_score": {"type": "integer"},
            "hn_comments_count": {"type": "integer"},
            "hn_item_ids": {"type": "keyword"},
        },
    },
}

# ═════════════════════════════════════════════════════════════════════════════
#  HACKER NEWS
# ═════════════════════════════════════════════════════════════════════════════

HN_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                # Les commentaires HN contiennent du HTML (<p>, <a href=...>)
                "hn_text": {
                    "type": "custom",
                    "char_filter": ["html_strip"],
                    "tokenizer": "standard",
                    "filter": ["lowercase", "stop", "porter_stem"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "hn_id": {"type": "keyword"},
            "type": {"type": "keyword"},                 # story, comment, job, poll, pollopt
            "post_type": {"type": "keyword"},            # show_hn, ask_hn, launch_hn, article, text, job…
            "by": {"type": "keyword"},
            "time": {"type": "date", "format": "epoch_second"},
            "title": {
                "type": "text",
                "analyzer": "english",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "url": {"type": "keyword", "ignore_above": 2048},
            "domain": {"type": "keyword"},
            "text": {"type": "text", "analyzer": "hn_text"},
            "score": {"type": "integer"},
            "descendants": {"type": "integer"},
            "parent": {"type": "keyword"},               # parent direct (story ou commentaire)
            "story_id": {"type": "keyword"},             # story racine du fil
            "relation": {"type": "join", "relations": {"story": "comment"}},
            "arxiv_id_linked": {"type": "keyword"},
            "fetched_at": {"type": "date"},
        }
    },
}

LINKS_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "arxiv_id": {"type": "keyword"},
            "hn_item_id": {"type": "keyword"},
            "hn_score": {"type": "integer"},
            "hn_comments": {"type": "integer"},
            "hn_title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
            "hn_url": {"type": "keyword", "ignore_above": 2048},
            "hn_time": {"type": "date", "format": "epoch_second"},
            "link_source": {"type": "keyword"},          # url | text
            "in_corpus": {"type": "boolean"},            # l'article existe-t-il dans arxiv-papers ?
            "detected_at": {"type": "date"},
        }
    },
}

PIPELINE_LOGS_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic_templates": [
            {"strings_as_keywords": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}}
        ],
        "properties": {
            "@timestamp": {"type": "date"},
            "type": {"type": "keyword"},                 # pipeline_metrics | airflow_task | airflow_dag_run
            "es_status": {"type": "keyword"},
            "dag_id": {"type": "keyword"},
            "task_id": {"type": "keyword"},
            "run_id": {"type": "keyword"},
            "state": {"type": "keyword"},
            "duration": {"type": "float"},
            "index_size_mb": {"type": "float"},
        },
    },
}

INDEX_DEFINITIONS = {
    INDEX_ARXIV: ARXIV_SETTINGS,
    INDEX_HN: HN_SETTINGS,
    INDEX_LINKS: LINKS_SETTINGS,
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

    es.indices.create(
        index=name,
        settings=definition["settings"],
        mappings=definition["mappings"],
    )
    print(f"  ✅ Index '{name}' créé.")
    return True


def deploy_all(es: Elasticsearch, recreate: bool = False, only: list[str] | None = None):
    for name, definition in INDEX_DEFINITIONS.items():
        if only and name not in only:
            continue
        create_index(es, name, definition, recreate=recreate)


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
