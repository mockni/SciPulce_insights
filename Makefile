# =============================================================================
#  SciPulse — raccourcis (make help)
#  Les scripts Python s'exécutent depuis la machine hôte (Jours 1-3),
#  les DAGs Airflow dans les conteneurs (Jour 4).
# =============================================================================

PY      ?= python
DATA    ?= data/arxiv-raw/arxiv-cs-subset-100k.json
DUMP    ?= data/arxiv-metadata-oai-snapshot.json
ES_HOST ?= http://localhost:9200

.DEFAULT_GOAL := help
.PHONY: help venv up down clean status logs subset minio ingest-python wait-logstash \
        indexes reindex hn hn-loop validate validate-hn clean-dry clean-data postgres dbt \
        dbt-docs enrich links scores search-demo metrics kibana watcher test test-integration \
        pipeline

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Environnement ────────────────────────────────────────────────────────────
venv: ## Crée .venv et installe les dépendances (Python 3.11 recommandé)
	$(PY) -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements.txt
	@test -f .env || cp .env.example .env

up: ## Lance la stack (build de l'image Airflow au premier lancement)
	docker compose up -d --build

down: ## Arrête la stack (les volumes sont conservés)
	docker compose down

clean: ## Arrête la stack ET supprime les volumes (⚠️ perte des données ES/PG/MinIO)
	docker compose down -v
	rm -rf dbt/target dbt/logs gx/uncommitted

status: ## État des conteneurs + index Elasticsearch
	docker compose ps
	@curl -s "$(ES_HOST)/_cat/indices?v&s=index" || echo "Elasticsearch injoignable"

logs: ## Suit les logs de Logstash
	docker compose logs -f logstash

# ── Jour 1 : données + ingestion ─────────────────────────────────────────────
subset: ## TP1 — extrait 100k articles cs.*/stat.ML du dump Kaggle
	$(PY) -m src.ingestion.prepare_subset $(DUMP) --max 100000

minio: ## TP1 — dépose le fichier + chunks dans MinIO (bucket arxiv-raw)
	$(PY) -m src.ingestion.upload_to_minio $(DATA)

ingest-python: ## TP2 — ingestion Python bulk (index arxiv-papers-raw-py)
	$(PY) -m src.ingestion.arxiv_bulk_ingest $(DATA)

wait-logstash: ## TP2 — attend la fin de l'ingestion Logstash (arxiv-papers-raw)
	docker compose restart logstash
	$(PY) -m src.ingestion.check_logstash_ingestion --timeout 900

# ── Jour 2 : modélisation, HN, qualité ───────────────────────────────────────
indexes: ## TP3 — crée les index avec leurs mappings
	$(PY) -m src.utils.mappings

reindex: ## TP3 — arxiv-papers-raw → arxiv-papers (mapping optimisé)
	$(PY) -m src.ingestion.reindex_arxiv

hn: ## TP3 — un cycle de polling Hacker News (+ commentaires)
	$(PY) -m src.ingestion.hn_poller --with-comments 3

hn-loop: ## TP3 — polling continu toutes les 30 s
	$(PY) -m src.ingestion.hn_poller --continuous --interval 30 --with-comments 3

validate: ## TP4 — suite Great Expectations ArXiv
	$(PY) -m src.quality.validate_arxiv

validate-hn: ## TP4 — suite Great Expectations Hacker News
	$(PY) -m src.quality.validate_hn

clean-dry: ## TP4 — diagnostic de nettoyage sans écriture
	$(PY) -m src.quality.clean_arxiv --dry-run

clean-data: ## TP4 — nettoyage + réécriture dans ES
	$(PY) -m src.quality.clean_arxiv

# ── Jour 3 : transformation, enrichissement, recherche ───────────────────────
postgres: ## TP5 — charge arxiv-papers dans PostgreSQL
	$(PY) -m src.transformation.load_to_postgres

dbt: ## TP5 — dbt seed + run + test
	cd dbt && dbt seed && dbt run && dbt test

dbt-docs: ## TP5 — documentation dbt sur http://localhost:8081
	cd dbt && dbt docs generate && dbt docs serve --port 8081

enrich: ## TP5 — mots-clés TF-IDF + scores HN
	$(PY) -m src.enrichment.enrich_arxiv

links: ## TP7 — second passage de détection des liens ArXiv ↔ HN
	$(PY) -m src.enrichment.detect_arxiv_links

scores: ## TP7 — rafraîchit les scores HN des articles liés
	$(PY) -m src.enrichment.update_hn_scores

search-demo: ## TP6 — démonstration du module search_service
	$(PY) -m src.search.search_service "reinforcement learning robotics" --categories cs.AI cs.RO cs.LG
	$(PY) -m src.search.search_service --suggest "convlutional nueral network"
	$(PY) -m src.search.search_service --trends --since 2023-06-01

# ── Jour 4 : monitoring ──────────────────────────────────────────────────────
metrics: ## TP8 — collecte de métriques dans pipeline-logs
	$(PY) -m src.utils.pipeline_metrics

kibana: ## TP8 — importe data views + dashboards dans Kibana
	$(PY) -m src.utils.kibana_setup

watcher: ## TP8 — active la licence d'essai et installe l'alerte Watcher
	curl -s -X POST "$(ES_HOST)/_license/start_trial?acknowledge=true"; echo
	curl -s -X PUT "$(ES_HOST)/_watcher/watch/scipulse_pipeline_errors" \
	  -H 'Content-Type: application/json' -d @kibana/watcher_pipeline_errors.json; echo

# ── Tests ────────────────────────────────────────────────────────────────────
test: ## Tests unitaires (sans la stack)
	$(PY) -m pytest tests/unit -v

test-integration: ## Tests d'intégration (stack lancée)
	$(PY) -m pytest -m integration -v

# ── Tout le pipeline batch depuis l'hôte (équivalent du DAG) ─────────────────
pipeline: indexes minio wait-logstash reindex clean-data postgres dbt enrich validate metrics ## Enchaîne les TP 1→5
