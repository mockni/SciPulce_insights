"""Les DAGs Airflow se chargent sans erreur et respectent la chaîne attendue (TP 7).

Nécessite apache-airflow (optionnel en local, test ignoré sinon) :
    pip install "apache-airflow==2.9.3" --constraint \
      https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    pytest.importorskip("airflow.models")  # (un dossier local airflow/ existe : on teste le vrai paquet)
    from airflow.models import DagBag

    # Airflow ajoute DAGS_FOLDER au sys.path (scipulse_callbacks y est importé) : on fait de même
    sys.path.insert(0, str(DAGS_DIR))
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_arxiv_pipeline_medallion_order(dagbag):
    dag = dagbag.dags["scipulse_arxiv_pipeline"]
    order = [t.task_id for t in dag.topological_sort()]
    chain = ["bronze_arxiv", "validate_bronze_arxiv", "silver_arxiv", "validate_silver_arxiv",
             "bronze_citations", "silver_citations", "validate_silver_citations", "spark_insights"]
    assert [t for t in order if t in chain] == chain
    # La couche Gold n'est alimentée qu'après la validation Silver (circuit breaker)
    for gold in ("gold_index_arxiv", "spark_insights", "transform_dbt"):
        assert "validate_silver_arxiv" in dag.get_task(gold).upstream_task_ids or \
            "validate_silver_citations" in dag.get_task(gold).upstream_task_ids
    assert dag.get_task("collect_pipeline_metrics").trigger_rule == "all_done"


def test_spark_task_renders_valid_shell(dagbag):
    cmd = dagbag.dags["scipulse_arxiv_pipeline"].get_task("spark_insights").bash_command
    assert "awk '{print $1}'" in cmd and "--driver-class-path" in cmd and "insights_job.py" in cmd


def test_hn_poller_every_5_minutes(dagbag):
    dag = dagbag.dags["scipulse_hn_poller"]
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.max_active_runs == 1
    order = [t.task_id for t in dag.topological_sort()]
    assert order[:4] == ["bronze_hn", "validate_bronze_hn", "silver_hn", "validate_silver_hn"]


def test_monitoring_callbacks_never_raise():
    sys.path.insert(0, str(DAGS_DIR))
    import scipulse_callbacks

    ti = MagicMock(dag_id="d", task_id="t", try_number=1, duration=1.5, operator="BashOperator")
    with patch.object(scipulse_callbacks.requests, "head", return_value=MagicMock(status_code=200)), \
         patch.object(scipulse_callbacks.requests, "post") as post:
        scipulse_callbacks.on_task_failure({"task_instance": ti, "run_id": "r1", "exception": ValueError("boom")})
    doc = post.call_args[1]["json"]
    assert doc["state"] == "failed" and doc["duration"] == 1.5 and "boom" in doc["error"]

    with patch.object(scipulse_callbacks.requests, "post", side_effect=ConnectionError("ES down")):
        scipulse_callbacks.on_task_success({"task_instance": ti, "run_id": "r1"})  # ne lève pas
