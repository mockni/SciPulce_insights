"""
SciPulse — Data Views et dashboards Kibana « as code » (TP 8)

Le TP construit les dashboards à la main dans Lens. Ce script en fournit une
version versionnable (DataOps) : il génère les objets Kibana (data views,
visualisations, recherches, dashboards) et les importe via l'API Saved Objects.

Usage :
    python -m src.utils.kibana_setup                 # génère kibana/scipulse_saved_objects.ndjson et l'importe
    python -m src.utils.kibana_setup --export-only   # génère seulement le fichier NDJSON
    python -m src.utils.kibana_setup --export-from-kibana kibana/export.ndjson   # sauvegarde de VOS dashboards

Le fichier NDJSON peut aussi être importé à la main :
Stack Management → Saved Objects → Import.

Les objets sont écrits au format 7.10 (visualisations « agrégation ») : Kibana 8.x
applique lui-même ses migrations à l'import.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

from src.config import KIBANA_HOST, PROJECT_ROOT

NDJSON_PATH = PROJECT_ROOT / "kibana" / "scipulse_saved_objects.ndjson"
HEADERS = {"kbn-xsrf": "true"}

# ═════════════════════════════════════════════════════════════════════════════
#  DATA VIEWS (index patterns)
# ═════════════════════════════════════════════════════════════════════════════

DATA_VIEWS = {
    # date_published (v1 de l'article) plutôt que date_updated : le dump contient des
    # mises à jour massives de métadonnées qui créent des pics artificiels sur date_updated
    "arxiv-papers": ("arxiv-papers", "date_published"),
    "arxiv-papers-raw": ("arxiv-papers-raw", "date_updated"),
    "hn-items": ("hn-items", "time"),
    "arxiv-hn-links": ("arxiv-hn-links", "hn_time"),
    "pipeline-logs": ("pipeline-logs*", "@timestamp"),
}


def index_pattern(obj_id: str) -> dict:
    title, time_field = DATA_VIEWS[obj_id]
    return {
        "type": "index-pattern",
        "id": obj_id,
        "attributes": {"title": title, "timeFieldName": time_field},
        "references": [],
        "migrationVersion": {"index-pattern": "7.11.0"},
    }


# ═════════════════════════════════════════════════════════════════════════════
#  VISUALISATIONS (format agrégation 7.10)
# ═════════════════════════════════════════════════════════════════════════════

def _search_source(query: str = "") -> str:
    return json.dumps({
        "query": {"query": query, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    })


def visualization(obj_id: str, title: str, data_view: str, vis_type: str, aggs: list,
                  params: dict, query: str = "") -> dict:
    vis_state = {"title": title, "type": vis_type, "aggs": aggs, "params": params}
    return {
        "type": "visualization",
        "id": obj_id,
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source(query)},
        },
        "references": [{
            "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "type": "index-pattern",
            "id": data_view,
        }],
        "migrationVersion": {"visualization": "7.10.0"},
    }


def _agg(agg_id: str, agg_type: str, schema: str, **params) -> dict:
    return {"id": agg_id, "enabled": True, "type": agg_type, "params": params, "schema": schema}


def metric_vis(obj_id, title, data_view, metric_agg, query=""):
    params = {
        "addTooltip": True, "addLegend": False, "type": "metric",
        "metric": {
            "percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
            "metricColorMode": "None", "colorsRange": [{"from": 0, "to": 10000}],
            "labels": {"show": True}, "invertColors": False,
            "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 48},
        },
    }
    return visualization(obj_id, title, data_view, "metric", [metric_agg], params, query)


def xy_vis(obj_id, title, data_view, vis_type, metrics: list[dict], buckets: list[dict],
           mode="normal", horizontal=False, query=""):
    """Graphiques area / line / histogram / horizontal_bar."""
    category_axis = {
        "id": "CategoryAxis-1", "type": "category", "position": "left" if horizontal else "bottom",
        "show": True, "style": {}, "scale": {"type": "linear"},
        "labels": {"show": True, "filter": not horizontal, "truncate": 200, "rotate": 0}, "title": {},
    }
    value_axis = {
        "id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
        "position": "bottom" if horizontal else "left", "show": True, "style": {},
        "scale": {"type": "linear", "mode": mode},
        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100}, "title": {"text": ""},
    }
    series_type = "histogram" if vis_type in ("histogram", "horizontal_bar") else vis_type
    params = {
        "type": vis_type,
        "grid": {"categoryLines": False},
        "categoryAxes": [category_axis],
        "valueAxes": [value_axis],
        "seriesParams": [{
            "show": True, "type": series_type, "mode": mode,
            "data": {"label": m["params"].get("customLabel", m["type"]), "id": m["id"]},
            "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True,
            "interpolate": "linear", "valueAxis": "ValueAxis-1",
        } for m in metrics],
        "addTooltip": True, "addLegend": True, "legendPosition": "right",
        "times": [], "addTimeMarker": False,
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"},
        "labels": {},
    }
    return visualization(obj_id, title, data_view, vis_type, metrics + buckets, params, query)


def date_histogram(agg_id, field, interval="M"):
    return _agg(agg_id, "date_histogram", "segment", field=field, interval=interval,
                min_doc_count=1, extended_bounds={}, useNormalizedEsInterval=True, drop_partials=False)


def terms(agg_id, field, size, schema="segment", order_by="1", label=None):
    params = dict(field=field, size=size, order="desc", orderBy=order_by,
                  otherBucket=False, missingBucket=False)
    if label:
        params["customLabel"] = label
    return _agg(agg_id, "terms", schema, **params)


def count(agg_id="1", label="Nombre"):
    return _agg(agg_id, "count", "metric", customLabel=label)


def field_metric(agg_id, agg_type, field, label):
    return _agg(agg_id, agg_type, "metric", field=field, customLabel=label)


def last_value(agg_id, field, label):
    """« Last value » : top_hits trié par @timestamp décroissant."""
    return _agg(agg_id, "top_hits", "metric", field=field, aggregate="concat", size=1,
                sortField="@timestamp", sortOrder="desc", customLabel=label)


def tagcloud_vis(obj_id, title, data_view, field, size=50):
    params = {"scale": "linear", "orientation": "single", "minFontSize": 14, "maxFontSize": 64,
              "showLabel": False}
    return visualization(obj_id, title, data_view, "tagcloud",
                         [count(), terms("2", field, size)], params)


def pie_vis(obj_id, title, data_view, field, size=10, query=""):
    params = {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right",
              "isDonut": True, "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}}
    return visualization(obj_id, title, data_view, "pie", [count(), terms("2", field, size)], params, query)


def table_vis(obj_id, title, data_view, metrics, buckets, query="", per_page=15):
    params = {"perPage": per_page, "showPartialRows": False, "showMetricsAtAllLevels": False,
              "showTotal": False, "totalFunc": "sum", "percentageCol": ""}
    return visualization(obj_id, title, data_view, "table", metrics + buckets, params, query)


def saved_search(obj_id, title, data_view, columns, sort_field, query=""):
    return {
        "type": "search",
        "id": obj_id,
        "attributes": {
            "title": title, "description": "", "hits": 0,
            "columns": columns, "sort": [[sort_field, "desc"]], "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source(query)},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "index-pattern", "id": data_view}],
        "migrationVersion": {"search": "7.9.3"},
    }


def dashboard(obj_id, title, panels: list[tuple[str, str, int, int, int, int]], time_from="now-15y"):
    """panels : (type, id, x, y, w, h) — la grille Kibana fait 48 colonnes de large."""
    panels_json, references = [], []
    for i, (obj_type, ref_id, x, y, w, h) in enumerate(panels):
        panels_json.append({
            "version": "7.10.0",
            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i + 1)},
            "panelIndex": str(i + 1),
            "embeddableConfig": {},
            "panelRefName": f"panel_{i}",
        })
        references.append({"name": f"panel_{i}", "type": obj_type, "id": ref_id})
    return {
        "type": "dashboard",
        "id": obj_id,
        "attributes": {
            "title": title, "hits": 0, "description": "",
            "panelsJSON": json.dumps(panels_json),
            "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True}),
            "version": 1, "timeRestore": True, "timeFrom": time_from, "timeTo": "now",
            "refreshInterval": {"pause": True, "value": 0},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
                {"query": {"query": "", "language": "kuery"}, "filter": []})},
        },
        "references": references,
        "migrationVersion": {"dashboard": "7.9.3"},
    }


# ═════════════════════════════════════════════════════════════════════════════
#  CONTENU DES DASHBOARDS
# ═════════════════════════════════════════════════════════════════════════════

def build_objects() -> list[dict]:
    A, HN, LINKS, LOGS = "arxiv-papers", "hn-items", "arxiv-hn-links", "pipeline-logs"
    objects = [index_pattern(dv) for dv in DATA_VIEWS]

    # ── Dashboard métier : SciPulse — Observatoire scientifique ───────────
    business = [
        metric_vis("sp-total-articles", "Total articles", A, count(label="Total articles")),
        metric_vis("sp-articles-enrichis", "Articles enrichis (mots-clés)", A,
                   count(label="Articles enrichis (mots-clés)"), query="keywords : *"),
        metric_vis("sp-articles-hn", "Articles liés à HN", A, count(label="Articles liés à HN"),
                   query="hn_score > 0"),
        metric_vis("sp-categories", "Catégories distinctes", A,
                   field_metric("1", "cardinality", "primary_category", "Catégories distinctes")),
        xy_vis("sp-publications-mois", "Publications par mois (top 5 catégories)", A, "area",
               [count()], [date_histogram("2", "date_published", "M"), terms("3", "primary_category", 5, "group")],
               mode="stacked"),
        xy_vis("sp-top-categories", "Top 20 catégories", A, "horizontal_bar",
               [count()], [terms("2", "primary_category", 20)], horizontal=True),
        tagcloud_vis("sp-keywords-cloud", "Mots-clés les plus fréquents", A, "keywords", 50),
        xy_vis("sp-tendances", "Tendances par catégorie (top 3)", A, "line",
               [count()], [date_histogram("2", "date_published", "y"), terms("3", "primary_category", 3, "group")]),
        pie_vis("sp-hn-types", "Répartition des types HN", HN, "post_type"),
        table_vis("sp-hn-populaires", "Articles les plus populaires sur Hacker News", A,
                  [field_metric("1", "max", "hn_score", "Score HN"),
                   field_metric("3", "max", "hn_comments_count", "Commentaires HN")],
                  [terms("2", "title.keyword", 15, "bucket", label="Titre")], query="hn_score > 0"),
        xy_vis("sp-hn-score-semaine", "Score HN moyen des articles ArXiv liés (par semaine)", LINKS, "line",
               [field_metric("1", "avg", "hn_score", "Score HN moyen")], [date_histogram("2", "hn_time", "w")]),
        xy_vis("sp-hn-score-type", "Score HN moyen par type de post", HN, "histogram",
               [field_metric("1", "avg", "score", "Score moyen")], [terms("2", "post_type", 10)],
               query='type : "story"'),
        saved_search("sp-articles-recents", "Articles récents", A,
                     ["title", "primary_category", "date_published", "keywords", "hn_score"], "date_published"),
    ]
    objects += business
    objects.append(dashboard("sp-dashboard-observatoire", "SciPulse — Observatoire scientifique", [
        ("visualization", "sp-total-articles", 0, 0, 12, 7),
        ("visualization", "sp-articles-enrichis", 12, 0, 12, 7),
        ("visualization", "sp-articles-hn", 24, 0, 12, 7),
        ("visualization", "sp-categories", 36, 0, 12, 7),
        ("visualization", "sp-publications-mois", 0, 7, 48, 14),
        ("visualization", "sp-top-categories", 0, 21, 24, 16),
        ("visualization", "sp-keywords-cloud", 24, 21, 24, 16),
        ("visualization", "sp-tendances", 0, 37, 24, 13),
        ("visualization", "sp-hn-types", 24, 37, 24, 13),
        ("visualization", "sp-hn-populaires", 0, 50, 24, 15),
        ("visualization", "sp-hn-score-semaine", 24, 50, 24, 15),
        ("visualization", "sp-hn-score-type", 0, 65, 24, 13),
        ("search", "sp-articles-recents", 24, 65, 24, 13),
    ]))

    # ── Dashboard de monitoring : SciPulse — Monitoring Pipeline ──────────
    metrics_q = 'type : "pipeline_metrics"'
    tasks_q = 'type : "airflow_task"'
    monitoring = [
        metric_vis("mon-es-status", "Statut Elasticsearch", LOGS,
                   last_value("1", "es_status", "Statut Elasticsearch"), query=metrics_q),
        metric_vis("mon-volume-arxiv", "Volume arxiv-papers", LOGS,
                   last_value("1", "volume_arxiv_papers", "Volume arxiv-papers"), query=metrics_q),
        metric_vis("mon-volume-hn", "Volume hn-items", LOGS,
                   last_value("1", "volume_hn_items", "Volume hn-items"), query=metrics_q),
        metric_vis("mon-index-size", "Taille index (Mo)", LOGS,
                   last_value("1", "index_size_mb", "Taille index (Mo)"), query=metrics_q),
        xy_vis("mon-volumes", "Évolution des volumes", LOGS, "line",
               [field_metric("1", "max", "volume_arxiv_papers", "ArXiv"),
                field_metric("3", "max", "volume_hn_items", "HN"),
                field_metric("4", "max", "enriched_with_keywords", "Enrichis")],
               [date_histogram("2", "@timestamp", "auto")], query=metrics_q),
        pie_vis("mon-task-states", "Taux de succès / échec des tâches Airflow", LOGS, "state", query=tasks_q),
        xy_vis("mon-task-duration", "Durée moyenne par tâche (s)", LOGS, "horizontal_bar",
               [field_metric("1", "avg", "duration", "Durée moyenne (s)")], [terms("2", "task_id", 20)],
               horizontal=True, query=tasks_q),
        xy_vis("mon-ingested-per-run", "Documents HN ingérés par mesure", LOGS, "histogram",
               [field_metric("1", "sum", "delta_hn_items", "Nouveaux items HN")],
               [date_histogram("2", "@timestamp", "auto")], query=metrics_q),
        saved_search("mon-historique", "Historique des métriques", LOGS,
                     ["es_status", "volume_arxiv_papers", "volume_hn_items", "enriched_with_keywords",
                      "index_size_mb", "dag_id"], "@timestamp", query=metrics_q),
        saved_search("mon-echecs", "Dernières tâches en échec", LOGS,
                     ["dag_id", "task_id", "run_id", "try_number", "error"], "@timestamp",
                     query='type : "airflow_task" and state : "failed"'),
    ]
    objects += monitoring
    objects.append(dashboard("sp-dashboard-monitoring", "SciPulse — Monitoring Pipeline", [
        ("visualization", "mon-es-status", 0, 0, 12, 7),
        ("visualization", "mon-volume-arxiv", 12, 0, 12, 7),
        ("visualization", "mon-volume-hn", 24, 0, 12, 7),
        ("visualization", "mon-index-size", 36, 0, 12, 7),
        ("visualization", "mon-volumes", 0, 7, 48, 13),
        ("visualization", "mon-task-states", 0, 20, 16, 14),
        ("visualization", "mon-task-duration", 16, 20, 32, 14),
        ("visualization", "mon-ingested-per-run", 0, 34, 24, 12),
        ("search", "mon-echecs", 24, 34, 24, 12),
        ("search", "mon-historique", 0, 46, 48, 12),
    ], time_from="now-7d"))
    return objects


def write_ndjson(path: Path = NDJSON_PATH) -> Path:
    objects = build_objects()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print(f"📝 {len(objects)} objets Kibana écrits dans {path}")
    return path


def wait_for_kibana(timeout: int = 180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{KIBANA_HOST}/api/status", timeout=5)
            if r.ok and r.json().get("status", {}).get("overall", {}).get("level") == "available":
                return True
        except requests.RequestException:
            pass
        print("   ⏳ Kibana pas encore disponible...")
        time.sleep(5)
    return False


def import_ndjson(path: Path):
    with open(path, "rb") as f:
        r = requests.post(
            f"{KIBANA_HOST}/api/saved_objects/_import",
            params={"overwrite": "true"},
            headers=HEADERS,
            files={"file": (path.name, f, "application/ndjson")},
            timeout=60,
        )
    r.raise_for_status()
    result = r.json()
    print(f"   ✅ {result.get('successCount', 0)} objet(s) importé(s)")
    for err in result.get("errors", []):
        print(f"   ❌ {err.get('type')} {err.get('id')} : {err.get('error')}")
    return result


def export_from_kibana(path: str):
    """Sauvegarde des dashboards créés à la main (à versionner dans Git)."""
    r = requests.post(
        f"{KIBANA_HOST}/api/saved_objects/_export",
        headers={**HEADERS, "Content-Type": "application/json"},
        json={"type": ["dashboard", "visualization", "lens", "search", "index-pattern"],
              "includeReferencesDeep": True},
        timeout=60,
    )
    r.raise_for_status()
    Path(path).write_bytes(r.content)
    print(f"💾 Export Kibana écrit dans {path}")


def main():
    parser = argparse.ArgumentParser(description="SciPulse — dashboards Kibana as code")
    parser.add_argument("--export-only", action="store_true", help="Générer le NDJSON sans l'importer")
    parser.add_argument("--export-from-kibana", metavar="FICHIER", help="Exporter les objets existants")
    args = parser.parse_args()

    if args.export_from_kibana:
        export_from_kibana(args.export_from_kibana)
        return

    path = write_ndjson()
    if args.export_only:
        return

    print(f"📤 Import dans Kibana ({KIBANA_HOST})...")
    if not wait_for_kibana():
        print("❌ Kibana injoignable — importez le fichier à la main : Stack Management → Saved Objects → Import")
        sys.exit(1)
    result = import_ndjson(path)
    print(f"\n🎉 Dashboards disponibles : {KIBANA_HOST}/app/dashboards")
    if not result.get("success", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
