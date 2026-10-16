"""
SciPulse — Outils communs Great Expectations (GX 0.18, mode programmatique)

Le contexte est un FileDataContext enraciné à la racine du projet :
  gx/expectations/            → suites (versionnées dans Git)
  gx/checkpoints/             → checkpoints
  gx/uncommitted/data_docs/   → rapports HTML (Data Docs, non versionnés)

Chaque exécution publie aussi un événement `quality_check` (taux d'erreur) dans
pipeline-logs, pour le dashboard de monitoring.
"""

import logging
import warnings

# Avertissements internes de GX 0.18 (dépréciations) : inutiles dans les logs Airflow
warnings.filterwarnings("ignore", module="great_expectations")
warnings.filterwarnings("ignore", category=UserWarning, message=".*result_format.*")
warnings.filterwarnings("ignore", message=".*parseString.*")

import great_expectations as gx  # noqa: E402

from src.config import PROJECT_ROOT  # noqa: E402
from src.monitoring.events import emit  # noqa: E402

logging.getLogger("great_expectations").setLevel(logging.ERROR)

EXIT_OK, EXIT_FAILED, EXIT_NO_DATA = 0, 1, 2


def get_context(ephemeral: bool = False):
    """Contexte GX persistant (crée le dossier gx/ au premier appel) ; éphémère pour les tests."""
    context = gx.get_context() if ephemeral else gx.get_context(project_root_dir=str(PROJECT_ROOT))
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


def validation_results(result) -> list:
    run_results = list(result.run_results.values())
    return run_results[0]["validation_result"]["results"] if run_results else []


def summarize(result) -> dict:
    results = validation_results(result)
    failed = sum(1 for r in results if not r["success"])
    return {
        "success": bool(result.success),
        "expectations_evaluated": len(results),
        "expectations_failed": failed,
        "error_rate": round(failed / len(results), 4) if results else 0.0,
    }


def publish(result, layer: str, source: str, suite: str, rows: int) -> dict:
    """Publie le résultat dans pipeline-logs (taux d'erreur GX par couche et par source)."""
    summary = summarize(result)
    failed = [r["expectation_config"]["expectation_type"] + ":" +
              str(r["expectation_config"]["kwargs"].get("column", "table"))
              for r in validation_results(result) if not r["success"]]
    emit("quality_check", layer=layer, source=source, suite=suite, rows=rows,
         failed_expectations=failed or None, **summary)
    return summary


def print_results(result, title: str = "RÉSULTATS DE LA VALIDATION"):
    """Affiche les résultats de manière lisible dans le terminal."""
    print("\n" + "═" * 60)
    print(f"  {title}")
    print("═" * 60)

    results_list = validation_results(result)
    if not results_list:
        print("  ⚠️  Aucun résultat")
        return

    success_count = failure_count = 0
    for r in results_list:
        expectation_type = r["expectation_config"]["expectation_type"]
        column = r["expectation_config"]["kwargs"].get("column", "—table—")
        if r["success"]:
            success_count += 1
            icon = "✅"
        else:
            failure_count += 1
            icon = "❌"
        short_name = (expectation_type.replace("expect_column_values_to_", "")
                      .replace("expect_column_", "").replace("expect_table_", "table:"))
        detail = r.get("result", {}) or {}
        stats = ""
        if detail.get("unexpected_count") is not None:
            stats = f" ({detail['unexpected_count']} non-conformes"
            if detail.get("unexpected_percent") is not None:
                stats += f", {detail['unexpected_percent']:.2f}%"
            stats += ")"
        elif detail.get("observed_value") is not None:
            observed = detail["observed_value"]
            stats = f" (observé : {len(observed)} colonnes)" if isinstance(observed, list) else f" (observé : {observed})"
        print(f"  {icon} {column:<22} {short_name:<35} {stats}")

    total = success_count + failure_count
    print(f"\n  {'─' * 56}")
    print(f"  ✅ Succès  : {success_count}")
    print(f"  ❌ Échecs  : {failure_count}")
    print(f"  📊 Score   : {success_count / total * 100:.0f}% ({success_count}/{total})\n")


def build_and_show_docs(context) -> str | None:
    """Génère les Data Docs et affiche le chemin du rapport HTML."""
    sites = context.build_data_docs()
    url = sites.get("local_site") if sites else None
    if url:
        print(f"📄 Data Docs : {url}")
    return url
