"""
SciPulse — DAG secondaire : poller Hacker News (micro-batch) — TP 7
Exécuté toutes les 5 minutes pour récupérer les derniers items HN.

  poll_hn_api → detect_arxiv_links → update_arxiv_hn_scores → collect_pipeline_metrics
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from scipulse_callbacks import MONITORING_CALLBACKS

PY = os.getenv("SCIPULSE_PYTHON", "python")
HOME = "/opt/airflow"

default_args = {
    "owner": "scipulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),   # un cycle ne doit pas chevaucher le suivant
    **MONITORING_CALLBACKS,
}

with DAG(
    dag_id="scipulse_hn_poller",
    default_args=default_args,
    description="Polling Hacker News : ingestion micro-batch + détection liens ArXiv",
    schedule_interval="*/5 * * * *",  # Toutes les 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scipulse", "hacker-news", "streaming"],
    doc_md=__doc__,
) as dag:

    # ── 1. Polling de l'API HN ──────────────────────────────────────────
    # --with-comments : 3 commentaires de 1er niveau par nouvelle story (requêtes has_child)
    # --archive       : copie brute des items dans MinIO (bucket hn-raw)
    poll_hn = BashOperator(
        task_id="poll_hn_api",
        bash_command=f"""
            set -e
            echo "📡 Polling Hacker News..."
            cd {HOME}
            {PY} -m src.ingestion.hn_poller --limit 100 --with-comments 3 --archive
            echo "✅ Cycle de polling terminé"
        """,
    )

    # ── 2. Détection des liens ArXiv (second passage + croisement corpus) ─
    detect_links = BashOperator(
        task_id="detect_arxiv_links",
        bash_command=f"cd {HOME} && {PY} -m src.enrichment.detect_arxiv_links",
    )

    # ── 3. Mise à jour des scores croisés dans arxiv-papers ──────────────
    update_scores = BashOperator(
        task_id="update_arxiv_hn_scores",
        bash_command=f"cd {HOME} && {PY} -m src.enrichment.update_hn_scores",
    )

    # ── 4. Métriques (historique pour le dashboard de monitoring) ────────
    collect_metrics = BashOperator(
        task_id="collect_pipeline_metrics",
        bash_command=f"cd {HOME} && {PY} -m src.utils.pipeline_metrics --quiet",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    poll_hn >> detect_links >> update_scores >> collect_metrics
