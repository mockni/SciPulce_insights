"""
SciPulse — Enrichissement des articles ArXiv (TP 5, partie C)

Ce script :
  1. Extrait les abstracts depuis Elasticsearch
  2. Calcule les mots-clés par TF-IDF (scikit-learn)
  3. Charge les liens ArXiv ↔ Hacker News (index arxiv-hn-links)
  4. Injecte les scores HN croisés dans les articles ArXiv
  5. Met à jour l'index Elasticsearch (mises à jour partielles)

Usage :
    python -m src.enrichment.enrich_arxiv
    python -m src.enrichment.enrich_arxiv --max 10000          # Limiter l'échantillon
    python -m src.enrichment.enrich_arxiv --top-keywords 10
    python -m src.enrichment.enrich_arxiv --skip-tfidf          # seulement les scores HN
"""

import argparse
import sys
import time

import numpy as np
from elasticsearch import Elasticsearch, helpers
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer

from src.config import ES_HOST, INDEX_ARXIV, INDEX_LINKS

# Termes génériques du discours scientifique, sans valeur de mot-clé
SCIENTIFIC_STOPWORDS = {
    "paper", "propose", "proposed", "approach", "method", "methods", "results", "result",
    "show", "shows", "based", "using", "used", "use", "new", "novel", "present", "study",
    "work", "problem", "problems", "existing", "experiments", "experimental", "demonstrate",
    "performance", "state", "art", "furthermore", "however", "also", "different",
    "various", "provide", "proposes", "presents", "paper", "introduce", "introduces",
}


# ═════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 1 : EXTRACTION DE MOTS-CLÉS PAR TF-IDF
# ═════════════════════════════════════════════════════════════════════════════

def top_keywords_from_matrix(tfidf_matrix, feature_names, top_n: int) -> list[list[str]]:
    """
    Pour chaque ligne de la matrice creuse TF-IDF, retourne les top_n termes.
    Travaille directement sur la structure CSR (pas de conversion en matrice dense).
    """
    csr = tfidf_matrix.tocsr()
    results = []
    for i in range(csr.shape[0]):
        start, end = csr.indptr[i], csr.indptr[i + 1]
        if start == end:
            results.append([])
            continue
        scores = csr.data[start:end]
        cols = csr.indices[start:end]
        best = np.argsort(scores)[::-1][:top_n]
        results.append([feature_names[cols[j]] for j in best if scores[j] > 0])
    return results


def compute_keywords(ids: list[str], texts: list[str], top_n: int = 8, min_df: int = 5) -> dict[str, list[str]]:
    """Calcule les mots-clés TF-IDF (unigrammes + bigrammes) d'une liste de textes."""
    vectorizer = TfidfVectorizer(
        max_features=5000,      # Vocabulaire limité aux 5000 termes les plus fréquents
        stop_words=sorted(ENGLISH_STOP_WORDS | SCIENTIFIC_STOPWORDS),
        min_df=min(min_df, max(1, len(texts) // 10)),  # Ignorer les termes trop rares
        max_df=0.7 if len(texts) >= 10 else 1.0,       # Ignorer les termes présents dans > 70 % des docs
        ngram_range=(1, 2),     # Unigrammes et bigrammes (ex : "neural network")
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",  # Mots de 2+ lettres uniquement
        sublinear_tf=True,
    )
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    print(f"     Matrice {tfidf_matrix.shape[0]} docs × {tfidf_matrix.shape[1]} termes")
    keywords = top_keywords_from_matrix(tfidf_matrix, feature_names, top_n)
    return dict(zip(ids, keywords))


def extract_keywords_tfidf(
    es: Elasticsearch,
    index: str = INDEX_ARXIV,
    max_docs: int = 50_000,
    top_n: int = 8,
) -> dict[str, list[str]]:
    """
    Extrait les mots-clés de chaque article via TF-IDF.

    Retourne
    --------
    dict[str, list[str]]
        { arxiv_id: [keyword1, keyword2, ...], ... }
    """
    print(f"  📖 Extraction des abstracts (max {max_docs:,})...")

    # Collecter les articles (ID + titre + abstract) — seuls les champs utiles sont transférés
    articles = []
    for hit in helpers.scan(
        es, index=index, query={"query": {"match_all": {}}}, size=1000,
        _source=["arxiv_id", "title", "abstract"],
    ):
        src = hit["_source"]
        arxiv_id = src.get("arxiv_id") or hit["_id"]
        abstract = src.get("abstract") or ""
        if arxiv_id and len(abstract) > 50:
            # le titre est ajouté au texte : il concentre les termes les plus discriminants
            articles.append((arxiv_id, f"{src.get('title') or ''}. {abstract}"))
        if len(articles) >= max_docs:
            break

    print(f"     {len(articles):,} articles avec abstract valide")
    if not articles:
        return {}

    print("  🧮 Calcul TF-IDF...")
    start = time.time()
    ids = [a[0] for a in articles]
    texts = [a[1] for a in articles]
    print(f"  🏷  Extraction des top {top_n} mots-clés par article...")
    keywords_map = compute_keywords(ids, texts, top_n=top_n)
    print(f"     ({time.time() - start:.1f}s)")

    non_empty = sum(1 for v in keywords_map.values() if v)
    print(f"     {non_empty:,} articles ont des mots-clés")
    return keywords_map


# ═════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 2 : CROISEMENT ARXIV × HACKER NEWS
# ═════════════════════════════════════════════════════════════════════════════

def aggregate_links(link_sources) -> dict[str, dict]:
    """
    Agrège les documents de liaison par arxiv_id (un article peut être posté plusieurs fois).
    Un même item HN n'est compté qu'une fois.
    """
    hn_data: dict[str, dict] = {}
    seen: set[tuple[str, str]] = set()
    for src in link_sources:
        arxiv_id = src.get("arxiv_id")
        item_id = str(src.get("hn_item_id", ""))
        if not arxiv_id or (arxiv_id, item_id) in seen:
            continue
        seen.add((arxiv_id, item_id))
        entry = hn_data.setdefault(arxiv_id, {"hn_score": 0, "hn_comments_count": 0, "hn_item_ids": []})
        entry["hn_score"] += src.get("hn_score", 0) or 0
        entry["hn_comments_count"] += src.get("hn_comments", 0) or 0
        entry["hn_item_ids"].append(item_id)
    return hn_data


def load_hn_scores(es: Elasticsearch) -> dict[str, dict]:
    """
    Charge les liens ArXiv↔HN depuis l'index arxiv-hn-links et agrège les scores par arxiv_id.

    Retourne
    --------
    dict[str, dict]
        { arxiv_id: { "hn_score": total, "hn_comments_count": total, "hn_item_ids": [...] }, ... }
    """
    print("  🔗 Chargement des liens ArXiv ↔ HN...")

    if not es.indices.exists(index=INDEX_LINKS):
        print(f"     ⚠️  Index '{INDEX_LINKS}' n'existe pas — pas de scores HN")
        return {}

    count = es.count(index=INDEX_LINKS)["count"]
    if count == 0:
        print("     ⚠️  Aucun lien trouvé — pas de scores HN")
        return {}

    sources = (hit["_source"] for hit in helpers.scan(es, index=INDEX_LINKS, query={"query": {"match_all": {}}}))
    hn_data = aggregate_links(sources)
    print(f"     {len(hn_data):,} articles ArXiv liés à des posts HN (sur {count} liens)")
    return hn_data


# ═════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 3 : MISE À JOUR DE L'INDEX ELASTICSEARCH
# ═════════════════════════════════════════════════════════════════════════════

def build_update_actions(keywords_map: dict, hn_scores: dict, index: str = INDEX_ARXIV):
    """Actions bulk « update » (partial update : les autres champs ne sont pas écrasés)."""
    for arxiv_id in set(keywords_map) | set(hn_scores):
        doc_update = {}

        # Mots-clés TF-IDF
        kw = keywords_map.get(arxiv_id)
        if kw:
            doc_update["keywords"] = kw

        # Scores HN
        hn = hn_scores.get(arxiv_id)
        if hn:
            doc_update["hn_score"] = hn["hn_score"]
            doc_update["hn_comments_count"] = hn["hn_comments_count"]
            doc_update["hn_item_ids"] = hn["hn_item_ids"]

        if doc_update:
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": arxiv_id,
                "doc": doc_update,
                "doc_as_upsert": False,   # on n'invente pas d'article absent du corpus
            }


def update_index(es: Elasticsearch, keywords_map: dict, hn_scores: dict, index: str = INDEX_ARXIV):
    """Met à jour les articles dans ES avec les mots-clés et scores HN."""
    all_ids = set(keywords_map) | set(hn_scores)
    print(f"\n  📝 Mise à jour de {len(all_ids):,} articles dans '{index}'...")

    start = time.time()
    success = 0
    missing = 0
    errors = 0

    for ok, result in helpers.streaming_bulk(
        es,
        build_update_actions(keywords_map, hn_scores, index),
        chunk_size=500,
        raise_on_error=False,
        max_retries=3,
    ):
        if ok:
            success += 1
        elif result.get("update", {}).get("status") == 404:
            missing += 1   # article cité sur HN mais absent de notre corpus : normal
        else:
            errors += 1
            if errors <= 3:
                print(f"     ❌ {result}")

        total = success + missing + errors
        if total % 10_000 == 0 and total > 0:
            print(f"     {total:>7,} traités...")

    elapsed = time.time() - start
    es.indices.refresh(index=index)

    print(f"\n  ✅ {success:,} articles mis à jour en {elapsed:.1f}s")
    if missing:
        print(f"  ℹ️  {missing:,} articles cités sur HN sont absents du corpus (ignorés)")
    if errors:
        print(f"  ❌ {errors:,} erreurs")
    return success, missing, errors


# ═════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="SciPulse — Enrichissement ArXiv")
    parser.add_argument("--max", type=int, default=50_000, help="Max articles pour TF-IDF (défaut: 50000)")
    parser.add_argument("--top-keywords", type=int, default=8, help="Mots-clés par article (défaut: 8)")
    parser.add_argument("--skip-tfidf", action="store_true", help="Ne recalculer que les scores HN")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Enrichissement ArXiv                        ║")
    print("║  TF-IDF + croisement Hacker News                        ║")
    print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST, request_timeout=120)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    # Étape 1 : Mots-clés TF-IDF
    keywords_map = {}
    if not args.skip_tfidf:
        print("\n🧠 ÉTAPE 1 — Extraction de mots-clés par TF-IDF\n")
        keywords_map = extract_keywords_tfidf(es, max_docs=args.max, top_n=args.top_keywords)
        if keywords_map:
            sample_id = next(iter(keywords_map))
            print(f"\n  📋 Exemple — {sample_id} :")
            print(f"     Mots-clés : {keywords_map[sample_id]}")

    # Étape 2 : Scores HN
    print("\n🔗 ÉTAPE 2 — Croisement ArXiv × Hacker News\n")
    hn_scores = load_hn_scores(es)
    if hn_scores:
        sample_id = next(iter(hn_scores))
        sample = hn_scores[sample_id]
        print(f"\n  📋 Exemple — {sample_id} :")
        print(f"     Score HN : {sample['hn_score']}, Commentaires : {sample['hn_comments_count']}")

    # Étape 3 : Mise à jour
    print("\n📤 ÉTAPE 3 — Mise à jour de l'index Elasticsearch\n")
    _, _, errors = update_index(es, keywords_map, hn_scores)

    # Vérification
    print("\n🔍 Vérification...")
    sample = es.search(
        index=INDEX_ARXIV,
        query={"exists": {"field": "keywords"}},
        size=1,
        _source=["arxiv_id", "title", "keywords", "hn_score"],
    )
    if sample["hits"]["hits"]:
        hit = sample["hits"]["hits"][0]["_source"]
        print("  Exemple d'article enrichi :")
        print(f"    ID       : {hit.get('arxiv_id')}")
        print(f"    Titre    : {(hit.get('title') or '')[:60]}")
        print(f"    Mots-clés: {hit.get('keywords', [])}")
        print(f"    Score HN : {hit.get('hn_score', 'N/A')}")

    # Statistiques finales
    kw_count = es.count(index=INDEX_ARXIV, query={"exists": {"field": "keywords"}})["count"]
    hn_count = es.count(index=INDEX_ARXIV, query={"exists": {"field": "hn_score"}})["count"]
    print(f"\n  📊 Articles avec mots-clés : {kw_count:,}")
    print(f"  📊 Articles avec score HN  : {hn_count:,}")

    print("\n🎉 Enrichissement terminé !")
    print()
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
