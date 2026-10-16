"""
SciPulse — Suite de validation Great Expectations pour les données Hacker News (TP 4, partie C)

Usage :
    python -m src.quality.validate_hn
"""

import argparse
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch

from src.config import ES_HOST, INDEX_HN
from src.quality.extract_for_validation import extract_hn
from src.quality.gx_utils import build_and_show_docs, get_context, run_suite

HN_TYPES = ["story", "comment", "job", "poll", "pollopt"]
HN_LAUNCH_EPOCH = 1160418111          # premier item HN (9 octobre 2006)
ARXIV_ID_REGEX = r"^(?:\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})$"


def validate_hn_data(df: pd.DataFrame, min_rows: int = 1):
    """Valide un DataFrame d'items Hacker News. Retourne (context, résultat)."""
    df = df.copy()
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    now_epoch = int(time.time()) + 86_400  # tolérance d'un jour (décalages d'horloge)

    def build(validator):
        # ── Complétude ─────────────────────────────────────────────────
        # L'ID HN ne doit jamais être null
        validator.expect_column_values_to_not_be_null("hn_id")
        # Le type doit toujours être présent
        validator.expect_column_values_to_not_be_null("type")
        # Une story a toujours un titre (les commentaires n'en ont pas)
        validator.expect_column_values_to_not_be_null(
            "title", row_condition='type=="story"', condition_parser="pandas"
        )

        # ── Validité ───────────────────────────────────────────────────
        # Le type doit être dans l'ensemble connu
        validator.expect_column_values_to_be_in_set("type", value_set=HN_TYPES)
        # Le score doit être >= 0 quand il est présent
        # (les commentaires n'ont pas de score : les nulls sont ignorés par GX)
        validator.expect_column_values_to_be_between("score", min_value=0)
        # Le timestamp est dans une plage raisonnable (depuis la création de HN)
        validator.expect_column_values_to_be_between("time", min_value=HN_LAUNCH_EPOCH, max_value=now_epoch)
        # Un identifiant ArXiv détecté a un format valide
        validator.expect_column_values_to_match_regex("arxiv_id_linked", ARXIV_ID_REGEX)

        # ── Unicité ────────────────────────────────────────────────────
        # L'ID HN doit être unique
        validator.expect_column_values_to_be_unique("hn_id")

        # ── Volume ─────────────────────────────────────────────────────
        # On doit avoir au moins quelques documents
        validator.expect_table_row_count_to_be_between(min_value=min_rows)

    return run_suite(
        context=get_context(),
        df=df,
        datasource_name="hn_pandas",
        asset_name="hn_data",
        suite_name="hn_quality_suite",
        checkpoint_name="hn_checkpoint",
        build_expectations=build,
    )


def main():
    parser = argparse.ArgumentParser(description="SciPulse — validation qualité Hacker News")
    parser.add_argument("--max", type=int, default=5_000)
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Validation qualité Hacker News              ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # 1. Connexion à Elasticsearch
    es = Elasticsearch(ES_HOST, request_timeout=60)
    try:
        es.info()
    except Exception as e:
        print(f"\n❌ Elasticsearch non accessible : {e}")
        sys.exit(2)
    if not es.indices.exists(index=INDEX_HN):
        print(f"\n❌ L'index '{INDEX_HN}' n'existe pas — lancez d'abord : python -m src.ingestion.hn_poller")
        sys.exit(2)

    # 2. Extraction des données
    print("\n📥 Extraction des items HN...\n")
    df = extract_hn(es, max_docs=args.max)

    # 3. Jouer la suite de tests sur les données HN
    print("\n🔍 Exécution de la suite de validation...\n")
    context, result = validate_hn_data(df)

    # 4. Réutiliser la fonction print_results du module ArXiv
    from src.quality.validate_arxiv import print_results

    print_results(result, title="RÉSULTATS DE LA VALIDATION HACKER NEWS")

    # 5. Construire la documentation des données
    build_and_show_docs(context)

    if not result.success:
        print("⚠️  Certaines validations ont échoué — voir les détails ci-dessus.")
        sys.exit(1)
    print("🎉 Toutes les validations ont réussi !")


if __name__ == "__main__":
    main()
