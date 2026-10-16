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


def test_arxiv_pipeline_order(dagbag):
    dag = dagbag.dags["scipulse_arxiv_pipeline"]
    order = [t.task_id for t in dag.topological_sort()]
    expected = ["download_arxiv_dump", "validate_raw_arxiv", "ingest_arxiv_logstash", "reindex_arxiv",
                "clean_arxiv", "transform_dbt", "enrich_arxiv_python", "validate_enriched"]
    assert [t for t in order if t in expected] == expected
    assert dag.schedule_interval is None


def test_hn_poller_every_5_minutes(dagbag):
    dag = dagbag.dags["scipulse_hn_poller"]
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.max_active_runs == 1


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
