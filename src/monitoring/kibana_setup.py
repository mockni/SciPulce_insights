"""
SciPulse — Data Views et dashboards Kibana « as code »

Génère et importe trois dashboards (API Saved Objects) :
  1. SciPulse — Monitoring Pipeline          (pipeline-logs)       Module 1
  2. SciPulse — Exploration scientifique     (arxiv-papers)        Module 1
  3. SciPulse — Insights (Gold augmentée)    (arxiv-papers-enriched) Module 2

Usage :
    python -m src.monitoring.kibana_setup                 # génère kibana/scipulse_saved_objects.ndjson et l'importe
    python -m src.monitoring.kibana_setup --export-only   # génère seulement le fichier NDJSON
    python -m src.monitoring.kibana_setup --export-from-kibana kibana/export.ndjson   # sauvegarde des dashboards modifiés

Import manuel possible : Stack Management → Saved Objects → Import.
Les objets sont écrits au format 7.10 (visualisations « agrégation ») : Kibana 8.x
applique lui-même ses migrations à l'import.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

from src.config import INDEX_ARXIV, INDEX_ENRICHED, INDEX_PIPELINE_LOGS, KIBANA_HOST, PROJECT_ROOT

NDJSON_PATH = PROJECT_ROOT / "kibana" / "scipulse_saved_objects.ndjson"
HEADERS = {"kbn-xsrf": "true"}

# id du data view → (motif d'index, champ temporel)
DATA_VIEWS = {
    "dv-arxiv": (INDEX_ARXIV, "date_published"),
    "dv-enriched": (INDEX_ENRICHED, "date_published"),
    "dv-logs": (f"{INDEX_PIPELINE_LOGS}*", "@timestamp"),   # inclut pipeline-logs-airflow-* (Logstash)
}


# ═════════════════════════════════════════════════════════════════════════════
#  BRIQUES (format agrégation 7.10)
# ═════════════════════════════════════════════════════════════════════════════

def index_pattern(obj_id: str) -> dict:
    title, time_field = DATA_VIEWS[obj_id]
    return {"type": "index-pattern", "id": obj_id, "references": [],
            "attributes": {"title": title, "timeFieldName": time_field},
            "migrationVersion": {"index-pattern": "7.11.0"}}


def _search_source(query: str = "") -> str:
    return json.dumps({"query": {"query": query, "language": "kuery"}, "filter": [],
                       "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})


def visualization(obj_id, title, data_view, vis_type, aggs, params, query=""):
    return {
        "type": "visualization", "id": obj_id,
        "attributes": {
            "title": title, "description": "", "version": 1, "uiStateJSON": "{}",
            "visState": json.dumps({"title": title, "type": vis_type, "aggs": aggs, "params": params}),
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source(query)},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "index-pattern", "id": data_view}],
        "migrationVersion": {"visualization": "7.10.0"},
    }


def _agg(agg_id, agg_type, schema, **params):
    return {"id": agg_id, "enabled": True, "type": agg_type, "params": params, "schema": schema}


def count(agg_id="1", label="Nombre"):
    return _agg(agg_id, "count", "metric", customLabel=label)


def field_metric(agg_id, agg_type, field, label):
    return _agg(agg_id, agg_type, "metric", field=field, customLabel=label)


def last_value(agg_id, field, label, sort_field="@timestamp"):
    """« Dernière valeur » : top_hits trié par date décroissante."""
    return _agg(agg_id, "top_hits", "metric", field=field, aggregate="concat", size=1,
                sortField=sort_field, sortOrder="desc", customLabel=label)


def terms(agg_id, field, size, schema="segment", order_by="1", label=None):
    params = dict(field=field, size=size, order="desc", orderBy=order_by, otherBucket=False, missingBucket=False)
    if label:
        params["customLabel"] = label
    return _agg(agg_id, "terms", schema, **params)


def date_histogram(agg_id, field, interval="M"):
    return _agg(agg_id, "date_histogram", "segment", field=field, interval=interval, min_doc_count=1,
                extended_bounds={}, useNormalizedEsInterval=True, drop_partials=False)


def histogram(agg_id, field, interval):
    return _agg(agg_id, "histogram", "segment", field=field, interval=interval, min_doc_count=False,
                has_extended_bounds=False, extended_bounds={})


def metric_vis(obj_id, title, data_view, metric_agg, query=""):
    params = {"addTooltip": True, "addLegend": False, "type": "metric",
              "metric": {"percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
                         "metricColorMode": "None", "colorsRange": [{"from": 0, "to": 10000}],
                         "labels": {"show": True}, "invertColors": False,
                         "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "",
                                   "fontSize": 40}}}
    return visualization(obj_id, title, data_view, "metric", [metric_agg], params, query)


def xy_vis(obj_id, title, data_view, vis_type, metrics, buckets, mode="normal", horizontal=False, query=""):
    """Graphiques area / line / histogram / horizontal_bar."""
    category_axis = {"id": "CategoryAxis-1", "type": "category", "position": "left" if horizontal else "bottom",
                     "show": True, "style": {}, "scale": {"type": "linear"},
                     "labels": {"show": True, "filter": not horizontal, "truncate": 200, "rotate": 0}, "title": {}}
    value_axis = {"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                  "position": "bottom" if horizontal else "left", "show": True, "style": {},
                  "scale": {"type": "linear", "mode": mode},
                  "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100}, "title": {"text": ""}}
    series_type = "histogram" if vis_type in ("histogram", "horizontal_bar") else vis_type
    params = {
        "type": vis_type, "grid": {"categoryLines": False},
        "categoryAxes": [category_axis], "valueAxes": [value_axis],
        "seriesParams": [{"show": True, "type": series_type, "mode": mode,
                          "data": {"label": m["params"].get("customLabel", m["type"]), "id": m["id"]},
                          "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True,
                          "interpolate": "linear", "valueAxis": "ValueAxis-1"} for m in metrics],
        "addTooltip": True, "addLegend": True, "legendPosition": "right", "times": [], "addTimeMarker": False,
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"},
        "labels": {},
    }
    return visualization(obj_id, title, data_view, vis_type, metrics + buckets, params, query)


def pie_vis(obj_id, title, data_view, field, size=10, query=""):
    params = {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True,
              "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}}
    return visualization(obj_id, title, data_view, "pie", [count(), terms("2", field, size)], params, query)


def tagcloud_vis(obj_id, title, data_view, field, size=50, query=""):
    params = {"scale": "linear", "orientation": "single", "minFontSize": 14, "maxFontSize": 60, "showLabel": False}
    return visualization(obj_id, title, data_view, "tagcloud", [count(), terms("2", field, size)], params, query)


def table_vis(obj_id, title, data_view, metrics, buckets, query="", per_page=10):
    params = {"perPage": per_page, "showPartialRows": False, "showMetricsAtAllLevels": False,
              "showTotal": False, "totalFunc": "sum", "percentageCol": ""}
    return visualization(obj_id, title, data_view, "table", metrics + buckets, params, query)


def saved_search(obj_id, title, data_view, columns, sort_field, query=""):
    return {
        "type": "search", "id": obj_id,
        "attributes": {"title": title, "description": "", "hits": 0, "columns": columns,
                       "sort": [[sort_field, "desc"]], "version": 1,
                       "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source(query)}},
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern",
                        "id": data_view}],
        "migrationVersion": {"search": "7.9.3"},
    }


def dashboard(obj_id, title, panels, time_from="now-15y", description=""):
    """panels : (type, id, x, y, largeur, hauteur) — la grille Kibana fait 48 colonnes."""
    panels_json, references = [], []
    for i, (obj_type, ref_id, x, y, w, h) in enumerate(panels):
        panels_json.append({"version": "7.10.0", "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i + 1)},
                            "panelIndex": str(i + 1), "embeddableConfig": {}, "panelRefName": f"panel_{i}"})
        references.append({"name": f"panel_{i}", "type": obj_type, "id": ref_id})
    return {
        "type": "dashboard", "id": obj_id,
        "attributes": {
            "title": title, "hits": 0, "description": description, "version": 1,
            "panelsJSON": json.dumps(panels_json),
            "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True}),
            "timeRestore": True, "timeFrom": time_from, "timeTo": "now",
            "refreshInterval": {"pause": True, "value": 0},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
                {"query": {"query": "", "language": "kuery"}, "filter": []})},
        },
        "references": references,
        "migrationVersion": {"dashboard": "7.9.3"},
    }


# ═════════════════════════════════════════════════════════════════════════════
#  DASHBOARDS
# ═════════════════════════════════════════════════════════════════════════════

def monitoring_objects() -> tuple[list, dict]:
    L = "dv-logs"
    writes, quality, snaps, tasks = ('type : "layer_write"', 'type : "quality_check"',
                                     'type : "layer_snapshot"', 'type : "airflow_task"')
    objs = [
        metric_vis("mon-gold-docs", "Documents Gold (arxiv-papers)", L,
                   last_value("1", "objects", "Documents Gold"), query=f'{snaps} and bucket : "arxiv-papers"'),
        metric_vis("mon-ge-error-rate", "Taux d'erreur GX moyen (Silver)", L,
                   field_metric("1", "avg", "error_rate", "Taux d'erreur GX"), query=f'{quality} and layer : "silver"'),
        metric_vis("mon-es-status", "Statut Elasticsearch", L,
                   last_value("1", "es_status", "Statut Elasticsearch"), query='type : "pipeline_metrics"'),
        metric_vis("mon-task-failures", "Tâches Airflow en échec", L, count(label="Échecs"),
                   query=f'{tasks} and state : "failed"'),
        xy_vis("mon-volume-by-layer", "Volume ingéré par couche et par source (lignes)", L, "histogram",
               [field_metric("1", "sum", "rows", "Lignes écrites")],
               [terms("2", "layer", 3), terms("3", "source", 5, "group")], query=writes),
        xy_vis("mon-volume-over-time", "Lignes écrites dans le temps, par couche", L, "histogram",
               [field_metric("1", "sum", "rows", "Lignes")],
               [date_histogram("2", "@timestamp", "auto"), terms("3", "layer", 3, "group")],
               mode="stacked", query=writes),
        xy_vis("mon-ge-error-over-time", "Taux d'erreur Great Expectations par suite", L, "line",
               [field_metric("1", "max", "error_rate", "Taux d'erreur")],
               [date_histogram("2", "@timestamp", "auto"), terms("3", "suite", 10, "group")], query=quality),
        table_vis("mon-ge-last", "Dernier résultat par suite GX", L,
                  [last_value("1", "success", "Succès"), last_value("3", "expectations_failed", "Échecs"),
                   last_value("4", "rows", "Lignes validées")],
                  [terms("2", "suite", 10, "bucket", label="Suite")], query=quality),
        table_vis("mon-freshness", "Fraîcheur et volume des couches", L,
                  [last_value("1", "freshness_minutes", "Fraîcheur (min)"), last_value("3", "objects", "Objets / docs"),
                   last_value("4", "size_mb", "Taille (Mo)")],
                  [terms("2", "bucket", 12, "bucket", label="Bucket / index")], query=snaps),
        xy_vis("mon-task-duration", "Durée moyenne par tâche Airflow (s)", L, "horizontal_bar",
               [field_metric("1", "avg", "duration", "Durée (s)")], [terms("2", "task_id", 20)],
               horizontal=True, query=tasks),
        pie_vis("mon-task-states", "États des tâches Airflow", L, "state", query=tasks),
        saved_search("mon-failures", "Dernières tâches en échec", L,
                     ["dag_id", "task_id", "run_id", "try_number", "error"], "@timestamp",
                     query=f'{tasks} and state : "failed"'),
    ]
    dash = dashboard("dash-monitoring", "SciPulse — Monitoring Pipeline", [
        ("visualization", "mon-gold-docs", 0, 0, 12, 7),
        ("visualization", "mon-ge-error-rate", 12, 0, 12, 7),
        ("visualization", "mon-es-status", 24, 0, 12, 7),
        ("visualization", "mon-task-failures", 36, 0, 12, 7),
        ("visualization", "mon-volume-by-layer", 0, 7, 24, 14),
        ("visualization", "mon-volume-over-time", 24, 7, 24, 14),
        ("visualization", "mon-ge-error-over-time", 0, 21, 24, 14),
        ("visualization", "mon-ge-last", 24, 21, 24, 14),
        ("visualization", "mon-freshness", 0, 35, 24, 14),
        ("visualization", "mon-task-duration", 24, 35, 24, 14),
        ("visualization", "mon-task-states", 0, 49, 16, 13),
        ("search", "mon-failures", 16, 49, 32, 13),
    ], time_from="now-7d", description="Volume par couche, taux d'erreur Great Expectations, fraîcheur des données")
    return objs, dash


def exploration_objects() -> tuple[list, dict]:
    A = "dv-arxiv"
    objs = [
        metric_vis("exp-total", "Articles indexés", A, count(label="Articles")),
        metric_vis("exp-categories", "Catégories distinctes", A,
                   field_metric("1", "cardinality", "primary_category", "Catégories")),
        metric_vis("exp-authors", "Auteurs moyens par article", A, field_metric("1", "avg", "n_authors", "Auteurs / article")),
        metric_vis("exp-short", "Abstracts signalés trop courts", A, count(label="Abstracts courts"),
                   query="abstract_too_short : true"),
        xy_vis("exp-categories-bar", "Distribution des catégories (top 20)", A, "horizontal_bar",
               [count()], [terms("2", "primary_category", 20)], horizontal=True),
        pie_vis("exp-domains", "Répartition par domaine", A, "domain"),
        xy_vis("exp-monthly", "Évolution mensuelle des publications (top 5 catégories)", A, "area",
               [count()], [date_histogram("2", "date_published", "M"), terms("3", "primary_category", 5, "group")],
               mode="stacked"),
        xy_vis("exp-yearly", "Publications par année", A, "histogram", [count()],
               [date_histogram("2", "date_published", "y")]),
        table_vis("exp-latest-by-domain", "Article le plus récent par domaine", A,
                  [last_value("1", "title.keyword", "Titre", sort_field="date_published"),
                   field_metric("3", "max", "date_published", "Publié le")],
                  [terms("2", "domain", 15, "bucket", label="Domaine")]),
        saved_search("exp-recent", "Articles les plus récents par domaine", A,
                     ["title", "domain", "primary_category", "authors_flat", "date_published"], "date_published"),
    ]
    dash = dashboard("dash-exploration", "SciPulse — Exploration scientifique", [
        ("visualization", "exp-total", 0, 0, 12, 7),
        ("visualization", "exp-categories", 12, 0, 12, 7),
        ("visualization", "exp-authors", 24, 0, 12, 7),
        ("visualization", "exp-short", 36, 0, 12, 7),
        ("visualization", "exp-monthly", 0, 7, 48, 14),
        ("visualization", "exp-categories-bar", 0, 21, 24, 16),
        ("visualization", "exp-domains", 24, 21, 24, 16),
        ("visualization", "exp-yearly", 0, 37, 24, 13),
        ("visualization", "exp-latest-by-domain", 24, 37, 24, 13),
        ("search", "exp-recent", 0, 50, 48, 14),
    ], description="Distribution des catégories, évolution temporelle, articles récents par domaine")
    return objs, dash


def insights_objects() -> tuple[list, dict]:
    E = "dv-enriched"
    cols = ["title", "primary_category", "impact_score", "cited_by_count", "hn_score_total", "date_published"]
    objs = [
        metric_vis("ins-avg-impact", "Score d'impact moyen", E, field_metric("1", "avg", "impact_score", "Impact moyen")),
        metric_vis("ins-with-citations", "Articles avec données de citation", E, count(label="Avec citations"),
                   query="has_citations_data : true"),
        metric_vis("ins-with-hn", "Articles discutés sur Hacker News", E, count(label="Sur HN"), query="has_hn : true"),
        metric_vis("ins-citations-total", "Citations cumulées", E, field_metric("1", "sum", "cited_by_count", "Citations")),
        table_vis("ins-top-impact", "Top 15 — score d'impact composite", E,
                  [field_metric("1", "max", "impact_score", "Impact"), field_metric("3", "max", "cited_by_count", "Citations"),
                   field_metric("4", "max", "hn_score_total", "Points HN")],
                  [terms("2", "arxiv_id", 15, "bucket", label="Article")], per_page=15),
        pie_vis("ins-segments", "Segments analytiques", E, "segment"),
        saved_search("ins-academic-only", "Très cités, jamais mentionnés sur Hacker News", E, cols, "cited_by_count",
                     query='segment : "academic_only"'),
        saved_search("ins-viral-low", "Viraux sur Hacker News, peu cités", E, cols, "hn_score_total",
                     query='segment : "viral_low_citations"'),
        xy_vis("ins-growth", "Sous-domaines en croissance : publications et impact par année", E, "line",
               [field_metric("1", "avg", "impact_score", "Impact moyen")],
               [date_histogram("2", "date_published", "y"), terms("3", "primary_category", 5, "group")]),
        table_vis("ins-category-growth", "Croissance annuelle et vélocité de citation par catégorie", E,
                  [field_metric("1", "max", "category_yoy_growth", "Croissance annuelle (max)"),
                   field_metric("3", "avg", "category_avg_velocity", "Vélocité moyenne"),
                   field_metric("4", "avg", "impact_score", "Impact moyen")],
                  [terms("2", "primary_category", 15, "bucket", order_by="4", label="Catégorie")]),
        xy_vis("ins-impact-dist", "Distribution du score d'impact", E, "histogram", [count()],
               [histogram("2", "impact_score", 5)]),
        tagcloud_vis("ins-keywords", "Mots-clés TF-IDF des articles à fort impact", E, "keywords", 40,
                     query="impact_score >= 50"),
    ]
    dash = dashboard("dash-insights", "SciPulse — Insights (Gold augmentée)", [
        ("visualization", "ins-avg-impact", 0, 0, 12, 7),
        ("visualization", "ins-with-citations", 12, 0, 12, 7),
        ("visualization", "ins-with-hn", 24, 0, 12, 7),
        ("visualization", "ins-citations-total", 36, 0, 12, 7),
        ("visualization", "ins-top-impact", 0, 7, 30, 16),
        ("visualization", "ins-segments", 30, 7, 18, 16),
        ("search", "ins-academic-only", 0, 23, 24, 14),
        ("search", "ins-viral-low", 24, 23, 24, 14),
        ("visualization", "ins-growth", 0, 37, 24, 14),
        ("visualization", "ins-category-growth", 24, 37, 24, 14),
        ("visualization", "ins-impact-dist", 0, 51, 24, 13),
        ("visualization", "ins-keywords", 24, 51, 24, 13),
    ], description="Score d'impact composite, citations OpenAlex × popularité Hacker News")
    return objs, dash


def build_objects() -> list[dict]:
    objects = [index_pattern(dv) for dv in DATA_VIEWS]
    for builder in (monitoring_objects, exploration_objects, insights_objects):
        objs, dash = builder()
        objects += objs + [dash]
    return objects


def write_ndjson(path: Path = NDJSON_PATH) -> Path:
    objects = build_objects()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print(f"📝 {len(objects)} objets Kibana écrits dans {path}")
    return path


def wait_for_kibana(timeout: int = 180) -> bool:
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


def import_ndjson(path: Path) -> dict:
    with open(path, "rb") as f:
        r = requests.post(f"{KIBANA_HOST}/api/saved_objects/_import", params={"overwrite": "true"},
                          headers=HEADERS, files={"file": (path.name, f, "application/ndjson")}, timeout=60)
    r.raise_for_status()
    result = r.json()
    print(f"   ✅ {result.get('successCount', 0)} objet(s) importé(s)")
    for err in result.get("errors", []):
        print(f"   ❌ {err.get('type')} {err.get('id')} : {err.get('error')}")
    return result


def export_from_kibana(path: str):
    r = requests.post(f"{KIBANA_HOST}/api/saved_objects/_export",
                      headers={**HEADERS, "Content-Type": "application/json"},
                      json={"type": ["dashboard", "visualization", "lens", "search", "index-pattern"],
                            "includeReferencesDeep": True}, timeout=60)
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
        print("❌ Kibana injoignable — import manuel : Stack Management → Saved Objects → Import")
        sys.exit(1)
    result = import_ndjson(path)
    print(f"\n🎉 Dashboards disponibles : {KIBANA_HOST}/app/dashboards")
    if not result.get("success", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
