"""
SciPulse — Nettoyage des données ArXiv (TP 4, partie D)
Guidé par les résultats des validations Great Expectations.

Cycle : charger → diagnostiquer → nettoyer → diagnostiquer à nouveau → (optionnel) réindexer.

Corrections appliquées :
  1. Suppression des doublons (sur arxiv_id)
  2. Nettoyage des abstracts (espaces, retours à la ligne) + marquage des abstracts trop courts
  3. Nettoyage des titres (espaces multiples)
  4. Normalisation des noms d'auteurs
  5. Reconstruction des catégories primaires manquantes

La réindexation se fait par mises à jour partielles (_update) : seuls les champs
nettoyés sont réécrits, les enrichissements (keywords, hn_score…) sont préservés.

Usage :
    python -m src.quality.clean_arxiv              # Diagnostic + nettoyage + réindexation
    python -m src.quality.clean_arxiv --dry-run    # Diagnostic + nettoyage sans réindexation
"""

import argparse
import math
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.config import ES_HOST, INDEX_ARXIV
from src.utils.arxiv_utils import domain_of

INDEX = INDEX_ARXIV
BATCH_SIZE = 500
SHORT_ABSTRACT = 50

KNOWN_PREFIXES = (
    "cs.", "stat.", "math.", "eess.", "econ.", "q-fin.", "quant-ph", "physics.", "q-bio.",
    "nlin.", "hep-", "cond-mat", "astro-ph", "gr-qc", "nucl-", "math-ph",
)

# Champs réécrits dans Elasticsearch après nettoyage
CLEANED_FIELDS = [
    "title", "abstract", "authors_flat", "primary_category", "domain",
    "abstract_length", "abstract_too_short",
]


# ═════════════════════════════════════════════════════════════════════════════
#  CHARGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def load_all_articles(es: Elasticsearch, index: str = INDEX) -> pd.DataFrame:
    """
    Charge tous les articles depuis Elasticsearch dans un DataFrame.
    Ne conserve que _id et les champs utiles au nettoyage.
    """
    fields = ["arxiv_id", "title", "abstract", "authors_flat", "categories", "primary_category"]
    docs = []
    for i, hit in enumerate(
        helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=1000, _source=fields), 1
    ):
        doc = {f: hit["_source"].get(f) for f in fields}
        doc["_id"] = hit["_id"]
        docs.append(doc)
        if i % 25_000 == 0:
            print(f"   {i:>7,} chargés...")

    df = pd.DataFrame(docs, columns=fields + ["_id"])
    print(f"   ✅ {len(df):,} articles chargés depuis '{index}'")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  DIAGNOSTIC
# ═════════════════════════════════════════════════════════════════════════════

def diagnose(df: pd.DataFrame) -> dict:
    """Analyse les problèmes de qualité et retourne un rapport (métrique → nombre)."""
    report = {}
    abstracts = df["abstract"]
    titles = df["title"]

    # ── 1. Abstracts courts ou manquants ──────────────────────────────
    report["null_abstracts"] = int(abstracts.isna().sum())
    non_null_abs = abstracts.dropna().astype(str)
    report["empty_abstracts"] = int((non_null_abs.str.strip() == "").sum())
    report["short_abstracts"] = int((non_null_abs.str.len() < SHORT_ABSTRACT).sum())
    report["multiline_abstracts"] = int(non_null_abs.str.contains(r"\n|\s{2,}", regex=True).sum())

    # ── 2. Titres vides ───────────────────────────────────────────────
    report["null_titles"] = int(titles.isna().sum())
    non_null_titles = titles.dropna().astype(str)
    report["empty_titles"] = int((non_null_titles.str.strip() == "").sum())
    report["multi_space_titles"] = int(non_null_titles.str.contains(r"\s{2,}", regex=True).sum())

    # ── 3. Doublons ───────────────────────────────────────────────────
    report["null_arxiv_id"] = int(df["arxiv_id"].isna().sum())
    report["duplicates"] = int(df["arxiv_id"].duplicated().sum())

    # ── 4. Catégories inconnues / manquantes ─────────────────────────
    report["null_categories"] = int(df["primary_category"].isna().sum())
    cats = df["primary_category"].dropna().astype(str)
    report["unknown_categories"] = int((~cats.str.startswith(KNOWN_PREFIXES)).sum())

    # ── 5. Espaces superflus dans les auteurs ─────────────────────────
    authors = df["authors_flat"].dropna().astype(str)
    report["messy_authors"] = int(authors.str.contains(r"\s{2,}|\s,|^\s|\s$", regex=True).sum())

    return report


def print_diagnosis(report: dict, label: str):
    """Affiche le diagnostic de manière lisible."""
    labels = {
        "null_arxiv_id": "arxiv_id nulls",
        "null_abstracts": "Abstracts nulls",
        "empty_abstracts": "Abstracts vides",
        "short_abstracts": f"Abstracts < {SHORT_ABSTRACT} car.",
        "multiline_abstracts": "Abstracts multi-lignes/espaces",
        "null_titles": "Titres nulls",
        "empty_titles": "Titres vides",
        "multi_space_titles": "Titres avec multi-espaces",
        "duplicates": "Doublons (arxiv_id)",
        "null_categories": "Catégories primaires nulles",
        "unknown_categories": "Catégories inconnues",
        "messy_authors": "Auteurs mal formatés",
    }
    print(f"\n  {'─' * 54}")
    print(f"  {label}")
    print(f"  {'─' * 54}")
    for key, text in labels.items():
        print(f"  {text:<34} {report.get(key, 0):>8,}")


# ═════════════════════════════════════════════════════════════════════════════
#  NETTOYAGE
# ═════════════════════════════════════════════════════════════════════════════

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les corrections de qualité. Retourne un nouveau DataFrame nettoyé."""
    original_len = len(df)
    df = df.copy()
    print(f"\n  🧹 Nettoyage de {len(df):,} articles...\n")

    # ── Correction 1 : Supprimer les doublons ──────────────────────
    # Remarque : dans Elasticsearch, _id = arxiv_id rend les doublons impossibles —
    # cette étape protège surtout les sources sans clé (CSV, PostgreSQL…)
    before = len(df)
    df = df.dropna(subset=["arxiv_id"]).drop_duplicates(subset="arxiv_id", keep="first")
    print(f"  [1/5] Déduplication        : {before - len(df):>6,} lignes supprimées")

    # ── Correction 2 : Nettoyer les abstracts ──────────────────────
    abstracts = df["abstract"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    # un abstract vide reste « manquant » (null) : on ne maquille pas la donnée
    df["abstract"] = abstracts.where(abstracts != "", None)
    df["abstract_length"] = abstracts.str.len()
    # On marque les abstracts trop courts sans les supprimer (décision métier tracée)
    df["abstract_too_short"] = df["abstract_length"] < SHORT_ABSTRACT
    print(f"  [2/5] Nettoyage abstracts  : {int(df['abstract_too_short'].sum()):>6,} marqués comme courts")

    # ── Correction 3 : Nettoyer les titres ─────────────────────────
    titles = df["title"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["title"] = titles.where(titles != "", None)
    print("  [3/5] Nettoyage titres     : espaces multiples corrigés")

    # ── Correction 4 : Normaliser les noms d'auteurs ───────────────
    df["authors_flat"] = (
        df["authors_flat"].fillna("").astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"\s+,", ",", regex=True)
        .str.strip(" ,")
    )
    print("  [4/5] Normalisation auteurs: espaces normalisés")

    # ── Correction 5 : Reconstruire les catégories primaires ───────
    mask = df["primary_category"].isna() & df["categories"].apply(lambda x: isinstance(x, list) and len(x) > 0)
    df.loc[mask, "primary_category"] = df.loc[mask, "categories"].apply(lambda x: x[0])
    df["domain"] = df["primary_category"].apply(domain_of)
    print(f"  [5/5] Catégories primaires : {int(mask.sum()):>6,} reconstruites")

    print(f"\n  ✅ Nettoyage terminé — {len(df):,} articles restants ({len(df) - original_len:+,})")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  RÉINDEXATION
# ═════════════════════════════════════════════════════════════════════════════

def _json_safe(value):
    """NaN → None, types numpy → types Python (sinon ES rejette le document)."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item") and not isinstance(value, (list, dict, str)):
        return value.item()
    return value


def reindex_cleaned(es: Elasticsearch, df: pd.DataFrame, index: str = INDEX):
    """Réécrit les champs nettoyés (mise à jour partielle, idempotente)."""
    print(f"\n🔄 Réindexation de {len(df):,} articles nettoyés...")

    def generate_actions():
        # (itertuples renomme les colonnes commençant par « _ » : on renomme _id d'abord)
        subset = df[["_id"] + CLEANED_FIELDS].rename(columns={"_id": "doc_id"})
        for row in subset.itertuples(index=False):
            record = row._asdict()
            doc_id = record.pop("doc_id")
            doc = {k: _json_safe(v) for k, v in record.items()}
            if not doc.get("title"):
                doc.pop("title")  # un titre vide casserait le champ completion
            yield {"_op_type": "update", "_index": index, "_id": doc_id, "doc": doc}

    start = time.time()
    success = 0
    errors_list = []
    for ok, result in helpers.streaming_bulk(
        es, generate_actions(), chunk_size=BATCH_SIZE, raise_on_error=False, max_retries=3
    ):
        if ok:
            success += 1
            if success % 25_000 == 0:
                print(f"   {success:>7,} réindexés ({success / (time.time() - start):,.0f} docs/sec)")
        else:
            errors_list.append(result)

    elapsed = time.time() - start
    es.indices.refresh(index=index)

    print(f"\n  ✅ {success:,} documents mis à jour en {elapsed:.1f}s")
    if errors_list:
        print(f"  ❌ {len(errors_list)} erreurs")
        for e in errors_list[:3]:
            print(f"     {e}")
    return success, len(errors_list)


# ═════════════════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="SciPulse — Nettoyage ArXiv")
    parser.add_argument("--dry-run", action="store_true", help="Simuler sans réindexer")
    parser.add_argument("--index", default=INDEX)
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Nettoyage des données ArXiv                 ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 0. Connexion à Elasticsearch
    es = Elasticsearch(ES_HOST, request_timeout=120)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(1)

    # 1. Charger
    print("\n📥 Chargement des données...")
    df = load_all_articles(es, args.index)
    if df.empty:
        print("⚠️  Index vide — rien à nettoyer.")
        return

    # 2. Diagnostic AVANT
    report_before = diagnose(df)
    print_diagnosis(report_before, "DIAGNOSTIC AVANT NETTOYAGE")

    # 3. Nettoyer
    print("\n🧹 Nettoyage en cours...")
    df_clean = clean(df)

    # 4. Diagnostic APRÈS
    report_after = diagnose(df_clean)
    print_diagnosis(report_after, "DIAGNOSTIC APRÈS NETTOYAGE")

    # 5. Comparer
    print(f"\n  {'─' * 54}")
    print("  AMÉLIORATION")
    print(f"  {'─' * 54}")
    any_change = False
    for key in report_before:
        before = report_before[key]
        after = report_after[key]
        delta = before - after
        if delta > 0:
            any_change = True
            print(f"  📉 {key:<28} {before:>6,} → {after:>6,} ({-delta:>+6,})")
        elif delta == 0 and before > 0:
            print(f"  ── {key:<28} {before:>6,} → {after:>6,} (inchangé)")
    if not any_change:
        print("  ✨ Les données étaient déjà propres sur les points corrigeables.")

    # 6. Réindexer (sauf dry-run)
    if args.dry_run:
        print("\n🏃 Mode dry-run — aucune modification dans Elasticsearch.")
        print("   Pour appliquer les corrections : python -m src.quality.clean_arxiv")
    else:
        _, n_errors = reindex_cleaned(es, df_clean, args.index)
        print("\n💡 Relancez la validation pour constater l'amélioration :")
        print("   python -m src.quality.validate_arxiv")
        if n_errors:
            sys.exit(1)
    print()


if __name__ == "__main__":
    main()
