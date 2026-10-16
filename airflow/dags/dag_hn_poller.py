"""
SciPulse — DAG micro-batch Hacker News (toutes les 5 minutes)

    bronze_hn → validate_bronze_hn (diagnostic) → silver_hn → validate_silver_hn (contrat)
      → collect_pipeline_metrics

Le Silver HN (hn-clean/items/hn-items.parquet) est lu par le job Spark du DAG principal.
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
    description="Micro-batch Hacker News : API → Bronze (hn-raw) → Silver (hn-clean)",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scipulse", "medallion", "hacker-news", "micro-batch"],
    doc_md=__doc__,
) as dag:

    bronze_hn = BashOperator(
        task_id="bronze_hn",
        bash_command=f"cd {HOME} && {PY} -m src.bronze.hn --limit 100 --top 30 --with-comments 3",
    )

    validate_bronze_hn = BashOperator(
        task_id="validate_bronze_hn",
        bash_command=f"""
            cd {HOME}
            {PY} -m src.quality.validate_hn --layer bronze --max 5000
            CODE=$?
            [ $CODE -eq 1 ] && echo "⚠️  Anomalies Bronze documentées (corrigées au Silver)" && exit 0
            exit $CODE
        """,
    )

    silver_hn = BashOperator(task_id="silver_hn", bash_command=f"cd {HOME} && {PY} -m src.silver.hn")

    validate_silver_hn = BashOperator(
        task_id="validate_silver_hn",
        bash_command=f"cd {HOME} && {PY} -m src.quality.validate_hn --layer silver",
    )

    collect_metrics = BashOperator(
        task_id="collect_pipeline_metrics",
        bash_command=f"cd {HOME} && {PY} -m src.monitoring.pipeline_metrics",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    bronze_hn >> validate_bronze_hn >> silver_hn >> validate_silver_hn >> collect_metrics
