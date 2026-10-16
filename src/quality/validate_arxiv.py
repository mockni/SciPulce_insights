"""
SciPulse — Suite de validation Great Expectations pour les données ArXiv (TP 4)

Ce script :
  1. Extrait un échantillon depuis Elasticsearch
  2. Définit une suite d'expectations
  3. Exécute la validation (checkpoint)
  4. Génère un rapport HTML (Data Docs)

Usage :
    python -m src.quality.validate_arxiv                              # index arxiv-papers
    python -m src.quality.validate_arxiv --stage raw                  # index arxiv-papers-raw
    python -m src.quality.validate_arxiv --stage raw --from-file "data/arxiv-raw/*.json"   # avant ingestion (Airflow, tâche 2)
    python -m src.quality.validate_arxiv --stage enriched             # + contrôles d'enrichissement (tâche 6)
    python -m src.quality.validate_arxiv --max 50000

Code de sortie : 0 si toutes les expectations passent, 1 sinon (utilisé par Airflow).
"""

import argparse
import sys
from datetime import datetime, timezone

import pandas as pd
from elasticsearch import Elasticsearch

from src.config import ES_HOST, INDEX_ARXIV, INDEX_ARXIV_RAW
from src.quality.extract_for_validation import extract_arxiv, extract_arxiv_from_files
from src.quality.gx_utils import build_and_show_docs, get_context, print_results, run_suite

# Catégories ArXiv de l'informatique (taxonomie officielle) + stat.ML
KNOWN_CS_CATEGORIES = [
    "cs.AI", "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CL", "cs.CR", "cs.CV", "cs.CY",
    "cs.DB", "cs.DC", "cs.DL", "cs.DM", "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR",
    "cs.GT", "cs.HC", "cs.IR", "cs.IT", "cs.LG", "cs.LO", "cs.MA", "cs.MM", "cs.MS",
    "cs.NA", "cs.NE", "cs.NI", "cs.OH", "cs.OS", "cs.PF", "cs.PL", "cs.RO", "cs.SC",
    "cs.SD", "cs.SE", "cs.SI", "cs.SY",
    "stat.ML",
]
# Le subset contient aussi des articles dont cs.* n'est qu'une catégorie secondaire :
# on accepte les disciplines voisines fréquemment « cross-listées » en informatique.
KNOWN_CROSS_LISTED = [
    "stat.ME", "stat.TH", "stat.AP", "stat.CO", "stat.OT",
    "eess.AS", "eess.IV", "eess.SP", "eess.SY",
    "math.OC", "math.IT", "math.ST", "math.NA", "math.PR", "math.CO", "math.LO", "math.DS",
    "quant-ph", "physics.soc-ph", "physics.data-an", "physics.comp-ph",
    "q-bio.NC", "q-bio.QM", "q-bio.BM", "econ.EM", "econ.GN", "econ.TH", "q-fin.CP",
]
KNOWN_CATEGORIES = KNOWN_CS_CATEGORIES + KNOWN_CROSS_LISTED

ARXIV_ID_REGEX = r"^(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})$"
FIRST_ARXIV_YEAR = 1991

SUITES = {
    "raw": ("arxiv_raw_suite", "arxiv_raw_checkpoint", INDEX_ARXIV_RAW),
    "clean": ("arxiv_quality_suite", "arxiv_checkpoint", INDEX_ARXIV),
    "enriched": ("arxiv_enriched_suite", "arxiv_enriched_checkpoint", INDEX_ARXIV),
}


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Colonnes dérivées utiles aux expectations (année de publication)."""
    df = df.copy()
    dates = pd.to_datetime(df["date_published"].fillna(df["date_updated"]), errors="coerce", utc=True)
    df["year_published"] = dates.dt.year
    return df


def validate_arxiv_data(df: pd.DataFrame, stage: str = "clean", min_rows: int = 1_000):
    """
    Valide un DataFrame d'articles ArXiv avec Great Expectations.

    Retourne (context, résultat du checkpoint).
    """
    suite_name, checkpoint_name, _ = SUITES[stage]
    df = prepare_dataframe(df)
    current_year = datetime.now(timezone.utc).year

    def build(validator):
        # ══════════════════════════════════════════════════════════════
        #  EXPECTATIONS — Complétude
        # ══════════════════════════════════════════════════════════════

        # E1 : L'identifiant ArXiv ne doit JAMAIS être null (clé primaire)
        validator.expect_column_values_to_not_be_null("arxiv_id")

        # E2 : Le titre ne doit jamais être null
        validator.expect_column_values_to_not_be_null("title")

        # E3 : L'abstract doit être présent dans au moins 99 % des cas
        validator.expect_column_values_to_not_be_null("abstract", mostly=0.99)

        # E4 : La catégorie primaire ne doit jamais être null
        validator.expect_column_values_to_not_be_null("primary_category")

        # ══════════════════════════════════════════════════════════════
        #  EXPECTATIONS — Unicité
        # ══════════════════════════════════════════════════════════════

        # E5 : L'identifiant ArXiv doit être unique (pas de doublons)
        validator.expect_column_values_to_be_unique("arxiv_id")

        # ══════════════════════════════════════════════════════════════
        #  EXPECTATIONS — Validité
        # ══════════════════════════════════════════════════════════════

        # E6 : L'abstract doit avoir au moins 50 caractères (tolérance 5 %)
        validator.expect_column_value_lengths_to_be_between("abstract", min_value=50, mostly=0.95)

        # E7 : Le titre doit avoir au moins 10 caractères
        validator.expect_column_value_lengths_to_be_between("title", min_value=10, mostly=0.99)

        # E8 : La catégorie primaire appartient à un ensemble connu
        # (informatique + disciplines voisines cross-listées), tolérance de 5 %
        validator.expect_column_values_to_be_in_set(
            "primary_category", value_set=KNOWN_CATEGORIES, mostly=0.95
        )

        # E9 : Format de l'identifiant (nouveau format 2301.07041 ou ancien cs/0112017)
        validator.expect_column_values_to_match_regex("arxiv_id", ARXIV_ID_REGEX)

        # E10 : Année de publication réaliste (ArXiv existe depuis 1991)
        validator.expect_column_values_to_be_between(
            "year_published", min_value=FIRST_ARXIV_YEAR, max_value=current_year
        )

        # ══════════════════════════════════════════════════════════════
        #  EXPECTATIONS — Volume et structure
        # ══════════════════════════════════════════════════════════════

        # E11 : Le nombre de lignes doit être dans un intervalle raisonnable
        validator.expect_table_row_count_to_be_between(min_value=min_rows, max_value=3_000_000)

        # E12 : Toutes les colonnes attendues doivent être présentes
        validator.expect_table_columns_to_match_set(
            column_set=["arxiv_id", "title", "abstract", "categories", "primary_category",
                        "date_published", "date_updated"],
            exact_match=False,
        )

        # ── Après enrichissement (Jour 3) ─────────────────────────────
        if stage == "enriched":
            # E13 : une part significative des articles a des mots-clés TF-IDF
            # (le TP enrichit 50 000 articles sur 100 000 par défaut → seuil à 40 %)
            validator.expect_column_values_to_not_be_null("keywords", mostly=0.40)
            # E14 : un score HN, quand il existe, est positif
            validator.expect_column_values_to_be_between("hn_score", min_value=0)

    return run_suite(
        context=get_context(),
        df=df,
        datasource_name="arxiv_pandas",
        asset_name="arxiv_data",
        suite_name=suite_name,
        checkpoint_name=checkpoint_name,
        build_expectations=build,
    )


def main():
    parser = argparse.ArgumentParser(description="SciPulse — validation qualité ArXiv")
    parser.add_argument("--stage", choices=list(SUITES), default="clean")
    parser.add_argument("--index", help="Index à valider (défaut selon --stage)")
    parser.add_argument("--from-file", metavar="GLOB", help="Valider des fichiers JSON lines au lieu d'un index")
    parser.add_argument("--max", type=int, default=10_000, help="Taille de l'échantillon")
    parser.add_argument("--min-rows", type=int, default=1_000)
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Validation qualité ArXiv                    ║")
    print("╚══════════════════════════════════════════════════════════╝")

    if args.from_file:
        # 1-2. Validation des fichiers bruts, avant toute ingestion
        print(f"\n📥 Lecture d'un échantillon de {args.from_file}...\n")
        df = extract_arxiv_from_files(args.from_file, max_docs=args.max)
        if df.empty:
            print("❌ Aucun article lisible.")
            sys.exit(2)
    else:
        # 1. Connexion à Elasticsearch
        es = Elasticsearch(ES_HOST, request_timeout=120)
        try:
            es.info()
        except Exception as e:
            print(f"\n❌ Elasticsearch non accessible : {e}")
            sys.exit(2)

        index = args.index or SUITES[args.stage][2]
        if not es.indices.exists(index=index):
            print(f"\n❌ L'index '{index}' n'existe pas.")
            sys.exit(2)

        # 2. Extraction des données
        print(f"\n📥 Extraction d'un échantillon depuis '{index}'...\n")
        df = extract_arxiv(es, index=index, max_docs=args.max)

    # 3. Validation
    print(f"\n🔍 Exécution de la suite de validation ({SUITES[args.stage][0]})...\n")
    context, result = validate_arxiv_data(df, stage=args.stage, min_rows=min(args.min_rows, args.max))

    # 4. Résultats
    print_results(result, title=f"RÉSULTATS DE LA VALIDATION ARXIV ({args.stage})")

    # 5. Data Docs
    build_and_show_docs(context)

    # 6. Code de sortie (pour intégration Airflow)
    if not result.success:
        print("⚠️  Certaines validations ont échoué — voir les détails ci-dessus.")
        sys.exit(1)
    print("🎉 Toutes les validations ont réussi !")


if __name__ == "__main__":
    main()
