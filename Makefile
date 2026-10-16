# =============================================================================
#  SciPulse & SciPulse Insights — raccourcis (make help)
#  Depuis l'hôte : les scripts utilisent localhost (MinIO :9000, ES :9200, PG :5432).
#  En production, les DAGs Airflow exécutent les mêmes modules dans les conteneurs.
# =============================================================================

PY      ?= python
DUMP    ?= data/arxiv-metadata-oai-snapshot.json
ES_HOST ?= http://localhost:9200

.DEFAULT_GOAL := help
.PHONY: help venv up down clean status subset indexes \
        bronze-arxiv bronze-hn bronze-citations validate-bronze \
        silver-arxiv silver-hn silver-citations validate-silver \
        gold-arxiv dbt spark kibana metrics pipeline test test-e2e fixtures watcher \
        seed-test-set validate-test-set

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ───────────────────────────────────────────────────────────
venv: ## Crée .venv (Python 3.11) et installe les dépendances
	$(PY) -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements.txt
	@test -f .env || cp .env.example .env

up: ## Démarre toute la stack de zéro (build des images Airflow et Spark)
	@test -f .env || cp .env.example .env
	docker compose up -d --build

down: ## Arrête la stack (volumes conservés)
	docker compose down

clean: ## Arrête la stack ET supprime les volumes (⚠️ perte des données)
	docker compose down -v
	rm -rf dbt/target dbt/logs gx/uncommitted

status: ## État des conteneurs et des index
	docker compose ps
	@curl -s "$(ES_HOST)/_cat/indices?v&s=index" || echo "Elasticsearch injoignable"

subset: ## Sous-ensemble de développement (100 000 articles cs.* / stat.ML)
	$(PY) -m src.utils.prepare_subset $(DUMP) --max 100000

indexes: ## Index Elasticsearch + gabarit des logs Airflow
	$(PY) -m src.utils.mappings

# ── 🥉 Bronze ────────────────────────────────────────────────────────────────
bronze-arxiv: ## Dump ArXiv brut → s3://arxiv-raw
	$(PY) -m src.bronze.arxiv

bronze-hn: ## Un cycle de polling Hacker News → s3://hn-raw
	$(PY) -m src.bronze.hn

bronze-citations: ## Citations OpenAlex → s3://citations-raw
	$(PY) -m src.bronze.citations

validate-bronze: ## Suites GX Bronze (diagnostic)
	-$(PY) -m src.quality.validate_arxiv --layer bronze
	-$(PY) -m src.quality.validate_hn --layer bronze

# ── 🥈 Silver ────────────────────────────────────────────────────────────────
silver-arxiv: ## Nettoyage ArXiv → s3://arxiv-clean (Parquet)
	$(PY) -m src.silver.arxiv

silver-hn: ## Nettoyage HN → s3://hn-clean (Parquet)
	$(PY) -m src.silver.hn

silver-citations: ## Normalisation des citations → s3://citations-clean (Parquet)
	$(PY) -m src.silver.citations

validate-silver: ## Suites GX Silver (contrat)
	$(PY) -m src.quality.validate_arxiv --layer silver
	$(PY) -m src.quality.validate_hn --layer silver
	$(PY) -m src.quality.validate_citations

# ── 🥇 Gold ──────────────────────────────────────────────────────────────────
gold-arxiv: ## Module 1 : ArXiv Silver → index arxiv-papers
	$(PY) -m src.gold.index_arxiv

dbt: ## ArXiv Silver → PostgreSQL puis dbt seed / run / test
	$(PY) -m src.transformation.load_to_postgres
	cd dbt && dbt seed && dbt run && dbt test

spark: ## Module 2 : job Spark sur le cluster (dans le conteneur Airflow)
	docker exec scipulse-airflow-sched bash -c 'cd /opt/airflow && $$SPARK_SUBMIT --master $$SPARK_MASTER \
	  --conf spark.driver.host=$$(hostname -i | awk "{print \$$1}") --jars $$SPARK_EXTRA_JARS \
	  --driver-class-path $$(echo $$SPARK_EXTRA_JARS | tr , :) src/spark/insights_job.py --explain'

kibana: ## Data views + 3 dashboards
	$(PY) -m src.monitoring.kibana_setup

metrics: ## Instantané volume / fraîcheur par couche → pipeline-logs
	$(PY) -m src.monitoring.pipeline_metrics

watcher: ## Alerte Watcher sur le taux d'échec (licence d'essai)
	curl -s -X POST "$(ES_HOST)/_license/start_trial?acknowledge=true"; echo
	curl -s -X PUT "$(ES_HOST)/_watcher/watch/scipulse_pipeline_errors" \
	  -H 'Content-Type: application/json' -d @kibana/watcher_pipeline_errors.json; echo

pipeline: indexes bronze-arxiv validate-bronze silver-arxiv silver-hn validate-silver gold-arxiv dbt bronze-citations silver-citations ## Bronze → Silver → Gold partielle depuis l'hôte

# ── Tests ────────────────────────────────────────────────────────────────────
test: ## Tests unitaires (aucun service requis)
	$(PY) -m pytest tests/unit -v

test-e2e: ## Validation de bout en bout sur le jeu de test (moto + Spark local)
	$(PY) -m pytest tests/integration -v

fixtures: ## Régénère le jeu de test déterministe
	$(PY) tests/fixtures/make_fixtures.py

seed-test-set: ## Charge le jeu de test dans la couche Bronze de MinIO (stack vide)
	$(PY) -m src.utils.seed_test_set

validate-test-set: indexes seed-test-set validate-bronze silver-hn silver-citations validate-silver gold-arxiv spark metrics kibana ## Validation sur la stack réelle (docs/validation.md)
	@echo "✅ Jeu de test traité : comparer Kibana à tests/fixtures/expected.json (docs/validation.md)"
