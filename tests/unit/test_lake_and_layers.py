"""Data lake (S3 simulé par moto), Bronze, Gold et monitoring."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src import config


def test_parquet_respects_explicit_schema(s3_lake):
    """Une colonne entièrement vide garde son type (sinon Spark ne lirait pas le fichier)."""
    import io

    from src.silver.cleaning import clean_arxiv
    from src.silver.schemas import ARXIV_SCHEMA

    df = clean_arxiv([{"id": "2301.00001", "title": "A title long enough", "abstract": "x" * 80,
                       "categories": "cs.AI", "update_date": "2023-01-02"}])
    assert df["journal_ref"].isna().all()
    s3_lake.put_parquet(config.BUCKET_ARXIV_CLEAN, "papers/part-00000.parquet", df, ARXIV_SCHEMA)
    table = pq.read_table(io.BytesIO(s3_lake.get_bytes(config.BUCKET_ARXIV_CLEAN, "papers/part-00000.parquet")))
    assert table.schema.equals(ARXIV_SCHEMA)
    assert str(table.schema.field("date_published").type) == "timestamp[us, tz=UTC]"


def test_bronze_arxiv_rewrites_partition(s3_lake, arxiv_lines):
    from src.bronze.arxiv import write_bronze

    write_bronze(iter(arxiv_lines), "2026-10-16", "test", chunk_lines=50)
    manifest = write_bronze(iter(arxiv_lines[:10]), "2026-10-16", "test", chunk_lines=50)  # relance
    keys = s3_lake.list_keys(config.BUCKET_ARXIV_RAW, "ingest_date=2026-10-16/", ".jsonl")
    assert keys == ["ingest_date=2026-10-16/part-00000.jsonl"] and manifest["rows"] == 10
    assert s3_lake.list_partitions(config.BUCKET_ARXIV_RAW) == ["2026-10-16"]


def test_bronze_hn_keeps_items_intact_and_tracks_state(s3_lake, hn_items):
    from src.bronze import hn

    by_id = {it["id"]: it for it in hn_items}
    with patch.object(hn, "fetch_ids", side_effect=lambda feed: [41000001] if feed == "newstories" else [41000002]), \
         patch.object(hn, "fetch_items", side_effect=lambda ids: [by_id[i] for i in ids]):
        result = hn.poll_once(limit=10, top=5, with_comments=1)
    lines = list(s3_lake.iter_jsonl_lines(config.BUCKET_HN_RAW, result["key"]))
    records = [json.loads(l) for l in lines]
    assert [r["_feed"] for r in records] == ["newstories", "topstories", "comment"]
    original = {k: v for k, v in records[0].items() if not k.startswith("_")}
    assert original == by_id[41000001]                                 # contenu brut inchangé
    assert records[2]["_story_id"] == 41000001
    assert hn.load_state() == [41000001]                               # top stories : pas dans l'état


def test_gold_document_conversion():
    from src.gold.index_arxiv import to_document

    row = {"arxiv_id": "2301.00001", "title": "T", "date_published": pd.Timestamp("2023-01-02T03:04:05", tz="UTC"),
           "year_published": pd.NA, "authors": [{"name": "A", "affiliation": None}], "doi": None,
           "categories": ["cs.AI"], "abstract_too_short": False, "not_in_mapping": 1}
    doc = to_document(row)
    assert doc == {"arxiv_id": "2301.00001", "title": "T", "date_published": "2023-01-02T03:04:05.000Z",
                   "authors": [{"name": "A", "affiliation": None}], "categories": ["cs.AI"],
                   "abstract_too_short": False}


def test_postgres_text_conversion():
    from src.transformation.load_to_postgres import to_text

    assert to_text(["cs.AI", "cs.LG"]) == "cs.AI cs.LG"
    assert to_text(pd.Timestamp("2023-01-02", tz="UTC")).startswith("2023-01-02T00:00:00")
    assert to_text(74.0) == "74" and to_text(float("nan")) is None and to_text(pd.NA) is None


def test_monitoring_never_raises(monkeypatch):
    from src.monitoring import events

    monkeypatch.setenv("SCIPULSE_MONITORING", "1")
    broken = MagicMock()
    broken.indices.exists.side_effect = ConnectionError("ES down")
    monkeypatch.setattr(events, "_get_client", lambda: broken)
    monkeypatch.setattr(events, "_index_ready", False)
    assert events.emit("layer_write", layer="bronze") is False

    ok = MagicMock()
    ok.indices.exists.return_value = True
    monkeypatch.setattr(events, "_get_client", lambda: ok)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "scipulse_arxiv_pipeline")
    assert events.emit("quality_check", layer="silver", error_rate=0.0, empty=None) is True
    doc = ok.index.call_args[1]["document"]
    assert doc["dag_id"] == "scipulse_arxiv_pipeline" and "empty" not in doc and doc["type"] == "quality_check"


def test_pipeline_metrics_snapshots(s3_lake, arxiv_lines):
    from src.bronze.arxiv import write_bronze
    from src.monitoring.pipeline_metrics import collect

    write_bronze(iter(arxiv_lines), "2026-10-16", "test", chunk_lines=100)
    snaps = {s["bucket"]: s for s in collect(es=None)}
    assert set(snaps) == set(config.ALL_BUCKETS)
    assert snaps["arxiv-raw"]["layer"] == "bronze" and snaps["arxiv-raw"]["objects"] == 2   # part + manifeste
    assert snaps["arxiv-raw"]["freshness_minutes"] >= 0
    assert snaps["hn-clean"]["objects"] == 0 and "freshness_minutes" not in snaps["hn-clean"]


@pytest.mark.parametrize("module", ["src.bronze.arxiv", "src.bronze.hn", "src.bronze.citations",
                                    "src.silver.arxiv", "src.silver.hn", "src.silver.citations",
                                    "src.gold.index_arxiv", "src.quality.validate_arxiv",
                                    "src.quality.validate_hn", "src.quality.validate_citations",
                                    "src.transformation.load_to_postgres", "src.monitoring.pipeline_metrics",
                                    "src.monitoring.kibana_setup", "src.utils.mappings", "src.search.search_service",
                                    "src.utils.seed_test_set"])
def test_cli_help(module):
    """Chaque étape du pipeline expose une ligne de commande documentée."""
    import importlib
    import sys

    mod = importlib.import_module(module)
    with patch.object(sys, "argv", [module, "--help"]), pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 0


def test_openalex_pagination_and_api_key(monkeypatch):
    """Pagination par curseur : une page pleine → page suivante ; une page incomplète → arrêt."""
    from src.bronze import citations

    calls = []
    pages = [
        {"results": [{"id": f"W{i}"} for i in range(citations.PER_PAGE)], "meta": {"next_cursor": "c2"}},
        {"results": [{"id": "Wlast"}], "meta": {"next_cursor": "c3"}},
    ]

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def fake_get(url, params, timeout):
        calls.append(params)
        return FakeResponse(pages[len(calls) - 1])

    monkeypatch.setattr(citations, "OPENALEX_API_KEY", "secret-key")
    monkeypatch.setattr(citations, "MIN_INTERVAL_S", 0)
    monkeypatch.setattr(citations._session, "get", fake_get)

    works = citations.fetch_works_by_dois(["10.48550/arxiv.1706.03762", "10.1109/cvpr.2021.abc"])
    assert len(works) == citations.PER_PAGE + 1
    assert len(calls) == 2                                    # pas de 3ᵉ requête après une page incomplète
    assert calls[0]["cursor"] == "*" and calls[1]["cursor"] == "c2"
    assert calls[0]["filter"] == "doi:10.48550/arxiv.1706.03762|10.1109/cvpr.2021.abc"
    assert all(c["api_key"] == "secret-key" for c in calls)


def test_seed_test_set_fills_bronze(s3_lake, expected):
    """Le chargeur du jeu de test (validation sur la stack réelle) remplit les trois Bronze."""
    from src import config
    from src.utils.seed_test_set import seed

    out = seed("2026-10-16")
    assert out["arxiv_bronze"]["rows"] == expected["bronze_lines"]
    assert out["hn_bronze"]["rows"] == 6
    assert out["arxiv_silver"]["rows"] == expected["silver_arxiv_rows"]
    assert s3_lake.list_keys(config.BUCKET_HN_RAW, "date=", ".jsonl")
    assert s3_lake.list_keys(config.BUCKET_CITATIONS_RAW, "ingest_date=2026-10-16/", ".jsonl")
