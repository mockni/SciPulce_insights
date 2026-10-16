"""
SciPulse — Suites Great Expectations Hacker News (Bronze et Silver)

Usage :
    python -m src.quality.validate_hn --layer bronze     # diagnostic, non bloquant
    python -m src.quality.validate_hn --layer silver     # contrat, bloquant

Codes de sortie : 0 = succès, 1 = échecs, 2 = pas de données.
"""

import argparse
import sys
import time

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
from src.quality.samples import hn_bronze_sample, hn_silver_sample
from src.silver.cleaning import HN_TYPES

HN_LAUNCH_EPOCH = 1160418111          # premier item HN (9 octobre 2006)
ARXIV_ID_REGEX = r"^(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})$"


def _now_epoch() -> int:
    return int(time.time()) + 86_400   # tolérance d'un jour (horloges)


def bronze_expectations(validator):
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_column_values_to_not_be_null("type")
    validator.expect_column_values_to_be_in_set("type", HN_TYPES)
    validator.expect_column_values_to_be_between("time", min_value=HN_LAUNCH_EPOCH, max_value=_now_epoch())
    validator.expect_column_values_to_be_between("score", min_value=0)
    validator.expect_column_values_to_not_be_null("title", row_condition='type=="story"',
                                                  condition_parser="pandas", mostly=0.99)
    # Items supprimés / masqués : tolérés en petite proportion (ils sont rejetés au Silver)
    validator.expect_column_values_to_be_null("deleted", mostly=0.98)
    validator.expect_column_values_to_be_null("dead", mostly=0.98)
    validator.expect_column_values_to_not_be_null("_fetched_at")
    validator.expect_table_row_count_to_be_between(min_value=1)


def silver_expectations(validator):
    validator.expect_column_values_to_not_be_null("hn_id")
    validator.expect_column_values_to_be_unique("hn_id")
    validator.expect_column_values_to_be_in_set("type", HN_TYPES)
    validator.expect_column_values_to_not_be_null("published_at")
    validator.expect_column_values_to_be_between("time", min_value=HN_LAUNCH_EPOCH, max_value=_now_epoch())
    validator.expect_column_values_to_be_between("score", min_value=0)
    validator.expect_column_values_to_not_be_null("title", row_condition='type=="story"',
                                                  condition_parser="pandas")
    validator.expect_column_values_to_be_null("score", row_condition='type=="comment"',
                                              condition_parser="pandas")
    validator.expect_column_values_to_match_regex("url", r"^https?://")
    validator.expect_column_values_to_match_regex("arxiv_id_linked", ARXIV_ID_REGEX)
    validator.expect_table_row_count_to_be_between(min_value=1)


LAYERS = {
    "bronze": ("hn_bronze_suite", "hn_bronze_checkpoint", bronze_expectations, hn_bronze_sample),
    "silver": ("hn_silver_suite", "hn_silver_checkpoint", silver_expectations, hn_silver_sample),
}


def prepare(df: pd.DataFrame, layer: str) -> pd.DataFrame:
    df = df.copy()
    if layer == "bronze":
        df["time"] = pd.to_numeric(df["time"], errors="coerce")
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        return df.drop(columns=["kids"], errors="ignore")
    for col in ("time", "score", "descendants", "kids_count"):
        df[col] = df[col].astype("float64")   # GX 0.18 gère mal les entiers nullables pandas
    return df


def validate(df: pd.DataFrame, layer: str, context=None):
    suite, checkpoint, build, _ = LAYERS[layer]
    return run_suite(context or get_context(), prepare(df, layer), f"hn_{layer}_pandas",
                     f"hn_{layer}_data", suite, checkpoint, build)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — validation GX Hacker News")
    parser.add_argument("--layer", choices=list(LAYERS), required=True)
    parser.add_argument("--max", type=int, default=20_000)
    args = parser.parse_args()

    suite, _, _, loader = LAYERS[args.layer]
    print(f"\n🔍 Validation Hacker News — couche {args.layer} ({suite})")
    df = loader(args.max)
    if df.empty:
        print("❌ Aucune donnée à valider dans le data lake.")
        sys.exit(EXIT_NO_DATA)
    print(f"   Échantillon : {len(df):,} lignes")

    context, result = validate(df, args.layer)
    print_results(result, f"HACKER NEWS — {args.layer.upper()}")
    summary = publish(result, args.layer, "hn", suite, len(df))
    build_and_show_docs(context)
    print(f"   Taux d'erreur : {summary['error_rate']:.0%}")
    sys.exit(EXIT_OK if result.success else EXIT_FAILED)


if __name__ == "__main__":
    main()
