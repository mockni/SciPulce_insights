"""
SciPulse — Suites Great Expectations ArXiv (Bronze et Silver)

  --layer bronze : diagnostic des données brutes du dump (avant nettoyage).
                   Des échecs sont attendus : ils documentent les problèmes que le
                   Silver corrige. Dans le DAG, cette tâche ne bloque pas le pipeline.
  --layer silver : contrat de qualité des données nettoyées. Un échec BLOQUE
                   l'alimentation de la couche Gold (circuit breaker).

Usage :
    python -m src.quality.validate_arxiv --layer bronze
    python -m src.quality.validate_arxiv --layer silver

Codes de sortie : 0 = toutes les expectations passent, 1 = échecs, 2 = pas de données.
"""

import argparse
import sys
from datetime import datetime, timezone

import pandas as pd

from src.quality.gx_utils import (
    EXIT_FAILED,
    EXIT_NO_DATA,
    EXIT_OK,
    build_and_show_docs,
    get_context,
    print_results,
    publish,
    run_suite,
)
from src.quality.samples import arxiv_bronze_sample, arxiv_silver_sample

ARXIV_ID_REGEX = r"^(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})$"
RAW_ARXIV_ID_REGEX = r"^(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})(?:v\d+)?$"
DOI_REGEX = r"^10\.\d{4,9}/\S+$"
FIRST_ARXIV_YEAR = 1991

# Catégories ArXiv de l'informatique (taxonomie officielle) + stat.ML
KNOWN_CS_CATEGORIES = [
    "cs.AI", "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CL", "cs.CR", "cs.CV", "cs.CY",
    "cs.DB", "cs.DC", "cs.DL", "cs.DM", "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR",
    "cs.GT", "cs.HC", "cs.IR", "cs.IT", "cs.LG", "cs.LO", "cs.MA", "cs.MM", "cs.MS",
    "cs.NA", "cs.NE", "cs.NI", "cs.OH", "cs.OS", "cs.PF", "cs.PL", "cs.RO", "cs.SC",
    "cs.SD", "cs.SE", "cs.SI", "cs.SY", "stat.ML",
]
# Un article du périmètre peut avoir cs.* en catégorie SECONDAIRE seulement
# (primaire math.OC, eess.IV, quant-ph…) : disciplines voisines acceptées.
KNOWN_CROSS_LISTED = [
    "stat.ME", "stat.TH", "stat.AP", "stat.CO", "stat.OT",
    "eess.AS", "eess.IV", "eess.SP", "eess.SY",
    "math.OC", "math.IT", "math.ST", "math.NA", "math.PR", "math.CO", "math.LO", "math.DS",
    "quant-ph", "physics.soc-ph", "physics.data-an", "physics.comp-ph",
    "q-bio.NC", "q-bio.QM", "q-bio.BM", "econ.EM", "econ.GN", "econ.TH", "q-fin.CP",
]
KNOWN_CATEGORIES = KNOWN_CS_CATEGORIES + KNOWN_CROSS_LISTED
KNOWN_DOMAINS = sorted({c.split(".")[0] for c in KNOWN_CATEGORIES})


def bronze_expectations(validator):
    """Contrat attendu de la SOURCE : ce qui échoue ici est corrigé dans le Silver."""
    # Complétude
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_column_values_to_not_be_null("title")
    validator.expect_column_values_to_not_be_null("abstract", mostly=0.99)
    validator.expect_column_values_to_not_be_null("categories")
    validator.expect_column_values_to_not_be_null("update_date")
    # Unicité
    validator.expect_column_values_to_be_unique("id")
    # Validité / format
    validator.expect_column_values_to_match_regex("id", RAW_ARXIV_ID_REGEX)
    validator.expect_column_values_to_match_regex("update_date", r"^\d{4}-\d{2}-\d{2}$")
    validator.expect_column_value_lengths_to_be_between("abstract", min_value=50, mostly=0.95)
    validator.expect_column_values_to_match_regex("doi", DOI_REGEX, mostly=0.99)
    # Structure et volume
    validator.expect_table_columns_to_match_set(
        column_set=["id", "title", "abstract", "authors", "categories", "update_date", "versions"],
        exact_match=False,
    )
    validator.expect_table_row_count_to_be_between(min_value=1)


def silver_expectations(validator):
    """Contrat de la couche Silver : bloquant pour la couche Gold."""
    current_year = datetime.now(timezone.utc).year
    # Clé primaire
    validator.expect_column_values_to_not_be_null("arxiv_id")
    validator.expect_column_values_to_be_unique("arxiv_id")
    validator.expect_column_values_to_match_regex("arxiv_id", ARXIV_ID_REGEX)
    # Complétude (les lignes sans titre / abstract ont été rejetées)
    validator.expect_column_values_to_not_be_null("title")
    validator.expect_column_values_to_not_be_null("abstract")
    validator.expect_column_values_to_not_be_null("primary_category")
    validator.expect_column_values_to_not_be_null("date_published")
    # Validité
    validator.expect_column_value_lengths_to_be_between("title", min_value=10, mostly=0.99)
    validator.expect_column_values_to_be_between("abstract_length", min_value=50, mostly=0.95)
    validator.expect_column_values_to_be_in_set("primary_category", KNOWN_CATEGORIES, mostly=0.95)
    validator.expect_column_values_to_be_in_set("domain", KNOWN_DOMAINS, mostly=0.95)
    validator.expect_column_values_to_be_between("year_published", min_value=FIRST_ARXIV_YEAR,
                                                 max_value=current_year)
    validator.expect_column_values_to_be_between("n_categories", min_value=1)
    # Clés de jointure normalisées (minuscules, sans préfixe https://doi.org/)
    validator.expect_column_values_to_match_regex("doi", DOI_REGEX)
    validator.expect_column_values_to_match_regex("arxiv_doi", r"^10\.48550/arxiv\.")
    # Volume
    validator.expect_table_row_count_to_be_between(min_value=1)


LAYERS = {
    "bronze": ("arxiv_bronze_suite", "arxiv_bronze_checkpoint", bronze_expectations, arxiv_bronze_sample),
    "silver": ("arxiv_silver_suite", "arxiv_silver_checkpoint", silver_expectations, arxiv_silver_sample),
}


def prepare(df: pd.DataFrame, layer: str) -> pd.DataFrame:
    """Colonnes non scalaires (listes, dicts) retirées : inutiles aux expectations."""
    if layer == "bronze":
        df = df.copy()
        df["categories"] = df["categories"].map(lambda v: v if isinstance(v, str) or v is None else str(v))
        return df.drop(columns=["authors_parsed"], errors="ignore").assign(
            versions=df["versions"].map(lambda v: len(v) if isinstance(v, list) else None))
    return df.drop(columns=["authors", "categories"], errors="ignore")


def validate(df: pd.DataFrame, layer: str, context=None):
    suite, checkpoint, build, _ = LAYERS[layer]
    return run_suite(context or get_context(), prepare(df, layer), f"arxiv_{layer}_pandas",
                     f"arxiv_{layer}_data", suite, checkpoint, build)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — validation GX ArXiv")
    parser.add_argument("--layer", choices=list(LAYERS), required=True)
    parser.add_argument("--max", type=int, default=20_000, help="Taille de l'échantillon")
    args = parser.parse_args()

    suite, _, _, loader = LAYERS[args.layer]
    print(f"\n🔍 Validation ArXiv — couche {args.layer} ({suite})")
    df = loader(args.max)
    if df.empty:
        print("❌ Aucune donnée à valider dans le data lake.")
        sys.exit(EXIT_NO_DATA)
    print(f"   Échantillon : {len(df):,} lignes")

    context, result = validate(df, args.layer)
    print_results(result, f"ARXIV — {args.layer.upper()}")
    summary = publish(result, args.layer, "arxiv", suite, len(df))
    build_and_show_docs(context)
    print(f"   Taux d'erreur : {summary['error_rate']:.0%}")
    sys.exit(EXIT_OK if result.success else EXIT_FAILED)


if __name__ == "__main__":
    main()
