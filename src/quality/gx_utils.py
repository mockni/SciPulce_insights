"""
SciPulse — Outils communs Great Expectations (GX 0.18, mode programmatique)

Le contexte est un FileDataContext enraciné à la racine du projet :
  gx/expectations/            → suites (versionnées dans Git)
  gx/checkpoints/             → checkpoints
  gx/uncommitted/data_docs/   → rapports HTML (Data Docs, non versionnés)
"""

import logging

import great_expectations as gx

from src.config import PROJECT_ROOT

logging.getLogger("great_expectations").setLevel(logging.ERROR)


def get_context():
    """Contexte GX persistant (crée le dossier gx/ au premier appel)."""
    context = gx.get_context(project_root_dir=str(PROJECT_ROOT))
    try:  # pas de barres de progression dans les logs Airflow
        from great_expectations.data_context.types.base import ProgressBarsConfig

        context.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # pragma: no cover - dépend de la version de GX
        pass
    return context


def run_suite(context, df, datasource_name: str, asset_name: str, suite_name: str,
              checkpoint_name: str, build_expectations):
    """
    Connecte le DataFrame, (re)construit la suite via `build_expectations(validator)`,
    sauvegarde la suite puis exécute le checkpoint. Retourne (context, result).
    """
    datasource = context.sources.add_or_update_pandas(name=datasource_name)
    try:
        data_asset = datasource.get_asset(asset_name)
    except Exception:
        data_asset = datasource.add_dataframe_asset(name=asset_name)
    batch_request = data_asset.build_batch_request(dataframe=df)

    # La suite est reconstruite à chaque exécution : le code Python est la source de vérité
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

    build_expectations(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[{"batch_request": batch_request, "expectation_suite_name": suite_name}],
    )
    result = checkpoint.run()
    return context, result


def print_results(result, title: str = "RÉSULTATS DE LA VALIDATION ARXIV"):
    """Affiche les résultats de manière lisible dans le terminal."""
    print("\n" + "═" * 60)
    print(f"  {title}")
    print("═" * 60)

    run_results = list(result.run_results.values())
    if not run_results:
        print("  ⚠️  Aucun résultat")
        return

    validation_result = run_results[0]["validation_result"]
    results_list = validation_result["results"]

    success_count = 0
    failure_count = 0

    for r in results_list:
        success = r["success"]
        expectation_type = r["expectation_config"]["expectation_type"]
        kwargs = r["expectation_config"]["kwargs"]

        # Nom de la colonne s'il existe
        column = kwargs.get("column", "—table—")

        if success:
            success_count += 1
            icon = "✅"
        else:
            failure_count += 1
            icon = "❌"

        # Nom simplifié de l'expectation
        short_name = (
            expectation_type.replace("expect_column_values_to_", "")
            .replace("expect_column_", "")
            .replace("expect_table_", "table:")
        )

        # Statistiques
        result_detail = r.get("result", {}) or {}
        unexpected_count = result_detail.get("unexpected_count")
        unexpected_pct = result_detail.get("unexpected_percent")

        stats = ""
        if unexpected_count is not None:
            stats = f" ({unexpected_count} non-conformes"
            if unexpected_pct is not None:
                stats += f", {unexpected_pct:.2f}%"
            stats += ")"
        elif result_detail.get("observed_value") is not None:
            observed = result_detail["observed_value"]
            if isinstance(observed, list):
                observed = f"{len(observed)} colonnes"
            stats = f" (observé : {observed})"

        print(f"  {icon} {column:<22} {short_name:<35} {stats}")

    print(f"\n  {'─' * 56}")
    print(f"  ✅ Succès  : {success_count}")
    print(f"  ❌ Échecs  : {failure_count}")
    total = success_count + failure_count
    pct = success_count / total * 100 if total > 0 else 0
    print(f"  📊 Score   : {pct:.0f}% ({success_count}/{total})")
    print()


def build_and_show_docs(context) -> str | None:
    """Génère les Data Docs et affiche le chemin du rapport HTML."""
    sites = context.build_data_docs()
    url = sites.get("local_site") if sites else None
    if url:
        print(f"📄 Data Docs : {url}")
    return url
