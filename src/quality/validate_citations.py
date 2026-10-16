"""
SciPulse — Suite Great Expectations des citations OpenAlex (couche Silver)

Usage :
    python -m src.quality.validate_citations

Codes de sortie : 0 = succès, 1 = échecs, 2 = pas de données.
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
from src.quality.samples import citations_silver_sample

SUITE, CHECKPOINT = "citations_silver_suite", "citations_silver_checkpoint"


def silver_expectations(validator):
    current_year = datetime.now(timezone.utc).year
    validator.expect_column_values_to_not_be_null("openalex_id")
    validator.expect_column_values_to_be_unique("openalex_id")
    validator.expect_column_values_to_match_regex("openalex_id", r"^W\d+$")
    # Clés de jointure normalisées
    validator.expect_column_values_to_not_be_null("doi")
    validator.expect_column_values_to_match_regex("doi", r"^10\.\d{4,9}/\S+$")
    validator.expect_column_values_to_match_regex("doi", r"^[^A-Z]+$")          # minuscules
    validator.expect_column_values_to_be_in_set("match_key_type", ["arxiv_doi", "journal_doi"])
    # Métriques d'impact
    validator.expect_column_values_to_not_be_null("cited_by_count", mostly=0.99)
    validator.expect_column_values_to_be_between("cited_by_count", min_value=0)
    validator.expect_column_values_to_be_between("publication_year", min_value=1900, max_value=current_year + 1)
    validator.expect_table_row_count_to_be_between(min_value=1)


def validate(df: pd.DataFrame, context=None):
    df = df.drop(columns=["citations_by_year"], errors="ignore").copy()
    for col in ("publication_year", "cited_by_count", "referenced_works_count"):
        df[col] = df[col].astype("float64")   # GX 0.18 gère mal les entiers nullables pandas
    return run_suite(context or get_context(), df, "citations_silver_pandas", "citations_silver_data",
                     SUITE, CHECKPOINT, silver_expectations)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — validation GX des citations")
    parser.add_argument("--max", type=int, default=50_000)
    args = parser.parse_args()
    print(f"\n🔍 Validation citations — couche silver ({SUITE})")
    df = citations_silver_sample(args.max)
    if df.empty:
        print("❌ Aucune donnée à valider.")
        sys.exit(EXIT_NO_DATA)
    context, result = validate(df)
    print_results(result, "CITATIONS OPENALEX — SILVER")
    summary = publish(result, "silver", "citations", SUITE, len(df))
    build_and_show_docs(context)
    print(f"   Taux d'erreur : {summary['error_rate']:.0%}")
    sys.exit(EXIT_OK if result.success else EXIT_FAILED)


if __name__ == "__main__":
    main()
