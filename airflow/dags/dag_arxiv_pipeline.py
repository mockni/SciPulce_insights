"""
SciPulse — DAG principal : pipeline batch Medallion (ArXiv + citations + Spark)

    init_indexes
      → bronze_arxiv ─→ validate_bronze_arxiv (diagnostic, non bloquant)
      → silver_arxiv ─→ validate_silver_arxiv (contrat, BLOQUANT)
            ├─→ gold_index_arxiv          (Module 1 : arxiv-papers, exploration)
            ├─→ transform_dbt             (ArXiv Silver → PostgreSQL → dbt seed/run/test)
            └─→ bronze_citations → silver_citations → validate_silver_citations (BLOQUANT)
                    → spark_insights      (Module 2 : jointure × 3, score → arxiv-papers-enriched)
      → collect_pipeline_metrics (toujours exécutée, même après un échec)

Le signal Hacker News est alimenté en continu par le DAG scipulse_hn_poller (couche hn-clean).
Toutes les commandes utilisent le virtualenv du pipeline (/opt/scipulse-venv).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from scipulse_callbacks import MONITORING_CALLBACKS

PY = os.getenv("SCIPULSE_PYTHON", "python")
DBT = os.getenv("SCIPULSE_DBT", "dbt")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "spark-submit")
HOME = "/opt/airflow"

# dbt écrit target/ et logs/ hors du dossier monté (évite les problèmes de droits)
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}


def python_task(task_id: str, module_args: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"cd {HOME} && {PY} -m {module_args}", **kwargs)


def tolerant_validation(task_id: str, module_args: str) -> BashOperator:
    """Validation de diagnostic : un échec d'expectation (code 1) est documenté mais toléré."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            cd {HOME}
            {PY} -m {module_args}
            CODE=$?
            if [ $CODE -eq 1 ]; then
                echo "⚠️  Expectations en échec au stade Bronze : anomalies documentées (Data Docs),"
                echo "    corrigées par le nettoyage Silver. Le pipeline continue."
                exit 0
            fi
            exit $CODE
        """,
    )


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
    description="Batch Medallion : ArXiv & citations (Bronze → Silver) puis Spark (Gold)",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["scipulse", "medallion", "batch", "spark"],
    doc_md=__doc__,
) as dag:

    init_indexes = python_task("init_indexes", "src.utils.mappings")

    # ── 🥉 Bronze ────────────────────────────────────────────────────────
    bronze_arxiv = python_task("bronze_arxiv", "src.bronze.arxiv", execution_timeout=timedelta(hours=2))
    validate_bronze_arxiv = tolerant_validation("validate_bronze_arxiv", "src.quality.validate_arxiv --layer bronze")

    # ── 🥈 Silver ────────────────────────────────────────────────────────
    silver_arxiv = python_task("silver_arxiv", "src.silver.arxiv", execution_timeout=timedelta(hours=2))
    validate_silver_arxiv = python_task("validate_silver_arxiv", "src.quality.validate_arxiv --layer silver")

    # ── 🥇 Gold partielle (Module 1) + entrepôt dbt ──────────────────────
    gold_index_arxiv = python_task("gold_index_arxiv", "src.gold.index_arxiv", execution_timeout=timedelta(hours=1))
    transform_dbt = BashOperator(
        task_id="transform_dbt",
        bash_command=f"""
            set -e
            cd {HOME} && {PY} -m src.transformation.load_to_postgres
            cd {HOME}/dbt
            {DBT} seed --profiles-dir .
            {DBT} run --profiles-dir .
            {DBT} test --profiles-dir .
        """,
        env=DBT_ENV,
        append_env=True,
    )

    # ── Module 2 : citations OpenAlex ────────────────────────────────────
    bronze_citations = python_task("bronze_citations", "src.bronze.citations", execution_timeout=timedelta(hours=3))
    silver_citations = python_task("silver_citations", "src.silver.citations")
    validate_silver_citations = python_task("validate_silver_citations", "src.quality.validate_citations")

    # ── Module 2 : job Spark → Gold finale (elasticsearch-hadoop) ────────
    # Mode client : le driver tourne ici, les executors sur spark-worker.
    # spark.driver.host = IP du conteneur, joignable depuis les workers.
    spark_insights = BashOperator(
        task_id="spark_insights",
        bash_command=f"""
            set -e
            cd {HOME}
            DRIVER_IP=$(hostname -i | awk '{{print $1}}')
            {SPARK_SUBMIT} \\
              --master "$SPARK_MASTER" \\
              --name scipulse-insights \\
              --conf spark.driver.host="$DRIVER_IP" \\
              --conf spark.driver.bindAddress=0.0.0.0 \\
              --conf spark.executor.memory=1g \\
              --conf spark.sql.shuffle.partitions=16 \\
              --jars "$SPARK_EXTRA_JARS" \\
              --driver-class-path "$(echo "$SPARK_EXTRA_JARS" | tr ',' ':')" \\
              src/spark/insights_job.py --explain
        """,
        execution_timeout=timedelta(hours=2),
    )

    collect_metrics = python_task("collect_pipeline_metrics", "src.monitoring.pipeline_metrics",
                                  trigger_rule=TriggerRule.ALL_DONE, retries=0)

    # ── Dépendances ──────────────────────────────────────────────────────
    init_indexes >> bronze_arxiv >> validate_bronze_arxiv >> silver_arxiv >> validate_silver_arxiv
    validate_silver_arxiv >> [gold_index_arxiv, transform_dbt, bronze_citations]
    bronze_citations >> silver_citations >> validate_silver_citations >> spark_insights
    [gold_index_arxiv, transform_dbt, spark_insights] >> collect_metrics
