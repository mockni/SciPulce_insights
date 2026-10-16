# SciPulse — Veille scientifique ArXiv × Hacker News

Pipeline de données DataOps autour de la pile Elastic (projet fil rouge du cours
*Ingénierie de Données & DataOps*). Il ingère le corpus ArXiv (batch) et le flux Hacker
News (micro-batch), contrôle la qualité, transforme, enrichit, indexe dans Elasticsearch
et restitue le tout dans Kibana, orchestré par Airflow.

```
 ArXiv (dump Kaggle) ──► MinIO arxiv-raw ──► Logstash ──► arxiv-papers-raw ──► reindex ──► arxiv-papers ◄── enrichissement
                                │                                                    │          ▲          (TF-IDF + scores HN)
                     Great Expectations (fichiers)                         nettoyage + GX       │
                                                                                     ▼          │
 Hacker News (API) ──► hn_poller.py ──► hn-items (story ⇄ comment) ──► arxiv-hn-links ─────────┘
                           └─► MinIO hn-raw                          PostgreSQL ──► dbt (staging / marts)
                                                                     Kibana : 2 dashboards · Airflow : 2 DAGs · pipeline-logs
```

---

## 1. Correspondance TP → fichiers

| TP | Livrable | Fichiers |
|---|---|---|
| 1 — Fondements | stack, subset ArXiv, landing zone MinIO | `docker-compose.yml`, `docker/`, `src/ingestion/prepare_subset.py`, `src/ingestion/upload_to_minio.py`, `src/ingestion/minio_client.py` |
| 2 — Ingestion | pipeline Logstash + alternative Python | `docker/logstash/pipeline/arxiv.conf`, `src/ingestion/arxiv_bulk_ingest.py` |
| 3 — Modélisation | mappings, réindexation, poller HN | `src/utils/mappings.py`, `src/ingestion/reindex_arxiv.py`, `src/ingestion/hn_poller.py`, `scripts/monitor_ingestion.sh` |
| 4 — Qualité | suites Great Expectations, nettoyage | `src/quality/` (`validate_arxiv.py`, `validate_hn.py`, `clean_arxiv.py`, …), `gx/` |
| 5 — Transformation | ES → PostgreSQL, dbt, TF-IDF, croisement HN | `src/transformation/load_to_postgres.py`, `dbt/`, `src/enrichment/` |
| 6 — Fouille de texte | `search_service.py` + tests, carnet Dev Tools | `src/search/search_service.py`, `tests/unit/test_search_service.py`, `docs/requetes_devtools.md` |
| 7 — Orchestration | DAGs batch + polling | `airflow/dags/`, `docker/airflow/Dockerfile` |
| 8 — Visualisation | métriques, dashboards, Filebeat, alerte | `src/utils/pipeline_metrics.py`, `src/utils/kibana_setup.py`, `kibana/`, `docker/filebeat/filebeat.yml` |

Réponses aux questions des énoncés : `docs/reponses_questions.md`. Corrigé des exercices `EXC-06E` : `docs/requetes_devtools.md`.

---

## 2. Prérequis

- Docker Desktop (8 Go de RAM minimum alloués, 16 recommandés). Linux : `sudo sysctl -w vm.max_map_count=262144`.
- Python **3.11** (Great Expectations 0.18 ne supporte pas pandas 3 / Python 3.13).
- Le dump ArXiv Kaggle (`Cornell-University/arxiv`) ou le subset fourni par le formateur.

```bash
cp .env.example .env
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Linux uniquement : fichiers écrits par Airflow à votre nom
echo "AIRFLOW_UID=$(id -u)" >> .env

# Versionnement (le .gitignore exclut déjà données, logs, .env et rapports générés)
git init && git add . && git commit -m "feat: projet SciPulse"
git checkout -b feature/setup-<votre-nom>
```

---

## 3. Démarrage pas à pas

Toutes les commandes se lancent **depuis la racine du projet** (les scripts sont des modules : `python -m src....`).
Chaque étape a aussi un raccourci `make` (`make help` pour la liste).

### Jour 1 — Stack, données, ingestion

```bash
docker compose up -d --build          # 1er lancement : téléchargement des images + build de l'image Airflow
docker compose ps                     # attendre scipulse-es "healthy"

# Subset de 100 000 articles cs.* / stat.ML (échantillon réparti sur toutes les années)
kaggle datasets download -d Cornell-University/arxiv -p data/ --unzip
python -m src.ingestion.prepare_subset data/arxiv-metadata-oai-snapshot.json --max 100000

python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-cs-subset-100k.json   # landing zone
python -m src.ingestion.upload_to_minio --list

docker compose restart logstash       # Logstash lit data/arxiv-raw/*.json → arxiv-papers-raw
bash scripts/monitor_ingestion.sh arxiv-papers-raw
python -m src.ingestion.arxiv_bulk_ingest data/arxiv-raw/arxiv-cs-subset-100k.json   # → arxiv-papers-raw-py
```

> Seul le subset doit se trouver dans `data/arxiv-raw/` : Logstash ingère **tous** les `*.json` de ce dossier.
> Gardez le dump complet dans `data/`.

### Jour 2 — Modélisation, Hacker News, qualité

```bash
python -m src.utils.mappings                    # arxiv-papers, hn-items, arxiv-hn-links, pipeline-logs
python -m src.ingestion.reindex_arxiv           # arxiv-papers-raw → arxiv-papers (auteurs nested, dates, domaine…)
python -m src.ingestion.hn_poller --with-comments 3
python -m src.ingestion.hn_poller --continuous --interval 30 --with-comments 3   # optionnel, en tâche de fond

python -m src.quality.validate_arxiv            # suite ArXiv (12 expectations)
python -m src.quality.validate_hn               # suite HN (9 expectations)
python -m src.quality.clean_arxiv --dry-run
python -m src.quality.clean_arxiv
python -m src.quality.validate_arxiv            # revalidation
open gx/uncommitted/data_docs/local_site/index.html   # Data Docs (xdg-open sous Linux)
```

### Jour 3 — Transformation, enrichissement, recherche

```bash
python -m src.transformation.load_to_postgres
cd dbt && dbt debug && dbt seed && dbt run && dbt test && cd ..
cd dbt && dbt docs generate && dbt docs serve --port 8081   # lignage

python -m src.enrichment.enrich_arxiv --max 10000           # puis sans --max (50 000) ou --max 200000 (tout)

python -m src.search.search_service "reinforcement learning robotics" --categories cs.AI cs.RO
python -m src.search.search_service --mlt 1706.03762
python -m src.search.search_service --suggest "convlutional nueral network"
python -m src.search.search_service --trends --since 2023-06-01
pytest tests/unit -v
```

### Jour 4 — Airflow, Kibana, monitoring

```bash
open http://localhost:8080             # admin / admin — activer puis déclencher les DAGs
python -m src.utils.kibana_setup       # data views + dashboards « Observatoire » et « Monitoring »
python -m src.utils.pipeline_metrics   # (aussi appelé à la fin de chaque run de DAG)
docker compose --profile monitoring up -d filebeat   # logs Airflow → pipeline-logs-filebeat-*
make watcher                           # alerte Watcher (active la licence d'essai 30 jours)
```

| Service | URL | Identifiants |
|---|---|---|
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | scipulse / scipulse |

---

## 4. Les DAGs

**`scipulse_arxiv_pipeline`** (manuel) :
`init_indexes → download_arxiv_dump → validate_raw_arxiv → ingest_arxiv_logstash → reindex_arxiv → clean_arxiv → transform_dbt → enrich_arxiv_python → validate_enriched → collect_pipeline_metrics`

- `download_arxiv_dump` récupère le subset depuis MinIO s'il n'est pas déjà dans `data/arxiv-raw/`.
- `ingest_arxiv_logstash` attend que l'index contienne autant de documents que les fichiers ; si Logstash n'a rien
  ingéré dans le délai, il bascule sur l'ingestion Python (même transformation).
- `validate_raw_arxiv` tolère les échecs ; `validate_enriched` bloque (circuit breaker).

**`scipulse_hn_poller`** (toutes les 5 min) : `poll_hn_api → detect_arxiv_links → update_arxiv_hn_scores → collect_pipeline_metrics`

Les tâches tournent dans l'image `scipulse-airflow` (Dockerfile fourni) : les dépendances du pipeline sont installées
dans un virtualenv dédié `/opt/scipulse-venv`, ce qui remplace le `pip install` à la volée de l'étape 3 du TP 7
et évite les conflits de versions avec Airflow. Chaque fin de tâche est enregistrée dans `pipeline-logs`
(état + durée) par des callbacks (`airflow/dags/scipulse_callbacks.py`).

---

## 5. Choix techniques et écarts par rapport aux énoncés

À connaître pour la soutenance — chaque écart corrige un problème qui aurait bloqué le pipeline réel.

| Sujet | Énoncé | Ici | Pourquoi |
|---|---|---|---|
| Hôtes des services | `ES_HOST = "http://localhost:9200"` en dur | variables d'environnement (`src/config.py`) | le même code tourne sur l'hôte et dans Airflow |
| Subset ArXiv | 100 000 premiers articles cs.* | échantillon aléatoire reproductible (option `--mode first` disponible) | le dump est trié par identifiant, donc chronologiquement : les 100 000 premiers articles cs.* sont tous anciens, sans tendance récente (LLM, diffusion…) |
| `date_published` | dans le mapping, jamais alimenté | date de la v1 (`versions[0].created`) dans Logstash et Python | `date_updated` présente des pics artificiels (mises à jour en masse du dump) |
| Synonymes | filtre `synonym` seul | + `search_analyzer` en `synonym_graph` | sinon « CNN » cherche aussi « network » seul |
| `significant_terms` sur `abstract` | requête du TP | `significant_text` | `significant_terms` sur un champ `text` échoue (fielddata désactivé) |
| Phrase suggester | champ `title` | `title.trigram` | `title` est stemmé : suggestions du type « convolut » |
| Great Expectations | `>=0.18` | `>=0.18.12,<1.0` + pandas < 2.3 | GX 1.x a changé d'API ; GX 0.18 ne gère pas pandas 3 |
| `load_to_postgres` | `psycopg` + `psycopg.extras` | `psycopg2` + `execute_values` | `extras.execute_values` n'existe que dans psycopg2 |
| dbt | schémas `public_staging` par défaut | macro `generate_schema_name` | pour obtenir `staging.*` et `marts.*` |
| DAG ArXiv | ingestion → dbt | + `reindex_arxiv` et `clean_arxiv` | dbt lit `arxiv-papers`, qui n'était jamais alimenté |
| Validation raw | sur l'index | sur les fichiers, avant ingestion | sur un premier run l'index n'existe pas encore |
| Dépendances Airflow | `pip install` dans les conteneurs | image dédiée + virtualenv | survit aux redémarrages, pas de conflit |
| Liens ArXiv | `arxiv.org/abs/2301.07041v2` → `2301.07041v2` | version retirée, anciens IDs (`cs/0112017`), `/html/`, liens dans le texte | sinon aucun lien ne correspond aux `arxiv_id` du corpus |
| Nettoyage | réindexation complète | mises à jour partielles (`_update`) | ne pas écraser les mots-clés et scores HN |
| Commentaires HN | non récupérés | option `--with-comments N` | les requêtes `has_child` retournaient toujours 0 |

---

## 6. Tests

```bash
pytest tests/unit -v                 # 76 tests sans Docker (Elasticsearch simulé), +3 si Airflow est installé
pytest -m integration -v             # avec la stack : vrai Elasticsearch, index temporaire
```

Les tests unitaires couvrent : transformation ArXiv (parité avec Logstash), parsing des auteurs et des dates,
extraction d'identifiants ArXiv, poller HN (routing des commentaires, liens), nettoyage, suites Great Expectations,
TF-IDF, construction des requêtes de `search_service`, cohérence des mappings, chargement des DAGs (si Airflow est installé).

---

## 7. Dépannage

| Symptôme | Cause / solution |
|---|---|
| `ConnectionError` vers `localhost:9200` dans Airflow | variable `ES_HOST` absente : relancer avec le `docker-compose.yml` fourni |
| `arxiv-papers-raw` reste à 0 | fichier ajouté après le démarrage de Logstash → `docker compose restart logstash` |
| `init.sql` non exécuté (base `scipulse` absente) | volume PostgreSQL déjà initialisé → `docker compose down -v && docker compose up -d` |
| `Fielddata is disabled on [abstract]` | utiliser `significant_text` (voir `docs/requetes_devtools.md`) |
| Erreur à la création de `arxiv-papers` sur les synonymes | ne pas ajouter de règle contenant un mot vide (of, the, for…) |
| Permission denied sur `gx/` ou `airflow/logs` (Linux) | `AIRFLOW_UID=$(id -u)` dans `.env`, puis `docker compose up -d` |
| Import Kibana en erreur sur un objet | le dashboard se reconstruit à la main en suivant le TP 8 ; exporter ensuite avec `python -m src.utils.kibana_setup --export-from-kibana kibana/export.ndjson` |
| `es_status` jaune | normal en single-node avec réplicas > 0 (les index du projet ont 0 réplica) |

---

## 8. Arborescence

```
scipulse/
├── docker-compose.yml · Makefile · requirements.txt · pyproject.toml · .env.example
├── docker/            logstash/ · postgres/init.sql · airflow/Dockerfile · filebeat/
├── airflow/dags/      dag_arxiv_pipeline.py · dag_hn_poller.py · scipulse_callbacks.py
├── src/
│   ├── config.py
│   ├── ingestion/     prepare_subset · upload_to_minio · minio_client · arxiv_bulk_ingest
│   │                  check_logstash_ingestion · reindex_arxiv · hn_poller
│   ├── quality/       extract_for_validation · gx_utils · validate_arxiv · validate_hn · clean_arxiv
│   ├── transformation/load_to_postgres
│   ├── enrichment/    enrich_arxiv · detect_arxiv_links · update_hn_scores
│   ├── search/        search_service
│   └── utils/         mappings · arxiv_utils · pipeline_metrics · kibana_setup
├── dbt/               models/staging · models/marts · seeds · tests · macros
├── kibana/            scipulse_saved_objects.ndjson · watcher_pipeline_errors.json
├── tests/             unit/ · integration/
├── docs/              requetes_devtools.md · reponses_questions.md
├── gx/                (créé à la 1re validation) suites Great Expectations
└── data/  logs/  notebooks/   (non versionnés)
```
