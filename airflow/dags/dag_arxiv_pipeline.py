"""
SciPulse — DAG principal : pipeline ArXiv (batch) — TP 7
Orchestration complète :

  init_indexes → download_arxiv_dump → validate_raw_arxiv → ingest_arxiv_logstash
      → reindex_arxiv → clean_arxiv → transform_dbt → enrich_arxiv_python
      → validate_enriched → collect_pipeline_metrics

Par rapport à l'énoncé, deux tâches ont été ajoutées entre l'ingestion et dbt :
`reindex_arxiv` (arxiv-papers-raw → arxiv-papers, mapping optimisé, TP 3) et
`clean_arxiv` (nettoyage guidé par Great Expectations, TP 4) : sans elles, dbt et
l'enrichissement travailleraient sur un index arxiv-papers vide ou périmé.

Toutes les commandes s'exécutent dans le conteneur Airflow avec le virtualenv
du pipeline (/opt/scipulse-venv, voir docker/airflow/Dockerfile).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from scipulse_callbacks import MONITORING_CALLBACKS

PY = os.getenv("SCIPULSE_PYTHON", "python")
DBT = os.getenv("SCIPULSE_DBT", "dbt")
HOME = "/opt/airflow"
RAW_GLOB = f"{HOME}/data/arxiv-raw/*.json"

# dbt écrit target/ et logs/ hors du dossier monté (évite les problèmes de droits)
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}

default_args = {
    "owner": "scipulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    **MONITORING_CALLBACKS,
}

with DAG(
    dag_id="scipulse_arxiv_pipeline",
    default_args=default_args,
    description="Pipeline batch ArXiv : ingestion, qualité, transformation, enrichissement",
    schedule_interval=None,  # Déclenchement manuel pour le TP
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["scipulse", "arxiv", "batch"],
    doc_md=__doc__,
) as dag:

    # ── 0. Index Elasticsearch (création des index absents, idempotent) ───
    init_indexes = BashOperator(
        task_id="init_indexes",
        bash_command=f"cd {HOME} && {PY} -m src.utils.mappings",
    )

    # ── 1. Données ArXiv disponibles ? (sinon : récupération depuis MinIO) ─
    download_arxiv = BashOperator(
        task_id="download_arxiv_dump",
        bash_command=f"""
            set -e
            echo "📥 Vérification des données ArXiv..."
            mkdir -p {HOME}/data/arxiv-raw
            FILE_COUNT=$(ls -1 {RAW_GLOB} 2>/dev/null | wc -l)
            if [ "$FILE_COUNT" -eq "0" ]; then
                echo "   Aucun fichier local → récupération depuis la landing zone MinIO (s3://arxiv-raw)"
                cd {HOME} && {PY} -m src.ingestion.upload_to_minio --download arxiv-cs-subset-100k.json --dest {HOME}/data/arxiv-raw || true
                FILE_COUNT=$(ls -1 {RAW_GLOB} 2>/dev/null | wc -l)
            fi
            if [ "$FILE_COUNT" -eq "0" ]; then
                echo "❌ Aucun fichier JSON trouvé dans {HOME}/data/arxiv-raw/"
                echo "   Placez le dump ArXiv dans data/arxiv-raw/ (TP 1, étape 4)"
                exit 1
            fi
            echo "✅ $FILE_COUNT fichier(s) trouvé(s)"
            LINES=$(cat {RAW_GLOB} | wc -l)
            echo "   $LINES articles au total"
        """,
    )

    # ── 2. Validation des données brutes AVANT ingestion (shift-left) ─────
    validate_raw = BashOperator(
        task_id="validate_raw_arxiv",
        bash_command=f"""
            echo "🔍 Validation des données brutes ArXiv..."
            cd {HOME}
            {PY} -m src.quality.validate_arxiv --stage raw --from-file "{RAW_GLOB}" --max 20000
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 2 ]; then
                echo "❌ Validation impossible (fichiers illisibles)"; exit 1
            elif [ $EXIT_CODE -ne 0 ]; then
                echo "⚠️  Des validations ont échoué — vérifiez les Data Docs (gx/uncommitted/data_docs)"
                echo "   Le pipeline continue (les échecs sont tolérés au stade raw)"
            fi
            echo "✅ Validation terminée"
        """,
    )

    # ── 3. Ingestion via Logstash (qui surveille /data/arxiv-raw/) ────────
    ingest_logstash = BashOperator(
        task_id="ingest_arxiv_logstash",
        bash_command=f"cd {HOME} && {PY} -m src.ingestion.check_logstash_ingestion --timeout 900 --fallback-python",
        execution_timeout=timedelta(minutes=30),
    )

    # ── 3b. Réindexation vers le mapping optimisé (TP 3) ──────────────────
    reindex = BashOperator(
        task_id="reindex_arxiv",
        bash_command=f"cd {HOME} && {PY} -m src.ingestion.reindex_arxiv --yes",
    )

    # ── 3c. Nettoyage guidé par la qualité (TP 4) ─────────────────────────
    clean = BashOperator(
        task_id="clean_arxiv",
        bash_command=f"cd {HOME} && {PY} -m src.quality.clean_arxiv",
    )

    # ── 4. Transformation dbt (ES → PostgreSQL → dbt seed/run/test) ───────
    transform_dbt = BashOperator(
        task_id="transform_dbt",
        bash_command=f"""
            set -e
            echo "⚙️  Chargement dans PostgreSQL + transformations dbt..."
            cd {HOME}
            echo "  [1/4] Chargement ES → PostgreSQL..."
            {PY} -m src.transformation.load_to_postgres
            cd {HOME}/dbt
            echo "  [2/4] dbt seed..."
            {DBT} seed --profiles-dir .
            echo "  [3/4] dbt run..."
            {DBT} run --profiles-dir .
            echo "  [4/4] dbt test..."
            {DBT} test --profiles-dir .
            echo "✅ Transformations dbt terminées"
        """,
        env=DBT_ENV,
        append_env=True,
    )

    # ── 5. Enrichissement Python (TF-IDF + croisement HN) ─────────────────
    enrich_python = BashOperator(
        task_id="enrich_arxiv_python",
        bash_command=f"cd {HOME} && {PY} -m src.enrichment.enrich_arxiv --max 200000",
        execution_timeout=timedelta(minutes=45),
    )

    # ── 6. Validation des données enrichies (circuit breaker) ─────────────
    validate_enriched = BashOperator(
        task_id="validate_enriched",
        bash_command=f"""
            echo "✅ Validation des données enrichies..."
            cd {HOME}
            {PY} -m src.quality.validate_arxiv --stage enriched --max 20000
        """,
    )

    # ── 7. Métriques du pipeline (même si une tâche a échoué) ─────────────
    collect_metrics = BashOperator(
        task_id="collect_pipeline_metrics",
        bash_command=f"cd {HOME} && {PY} -m src.utils.pipeline_metrics",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    # ── Dépendances ──────────────────────────────────────────────────────
    (
        init_indexes
        >> download_arxiv
        >> validate_raw
        >> ingest_logstash
        >> reindex
        >> clean
        >> transform_dbt
        >> enrich_python
        >> validate_enriched
        >> collect_metrics
    )
