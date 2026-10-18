# SciPulse & SciPulse Insights — Projet #4

Pipeline DataOps de veille scientifique en **architecture Medallion** : le corpus ArXiv (batch), le flux
Hacker News (micro-batch) et les citations OpenAlex sont ingérés bruts (🥉 Bronze), nettoyés et validés
(🥈 Silver), puis croisés par un job **Spark** en une couche analytique (🥇 Gold) servie par **Elasticsearch**
et restituée dans **Kibana**. L'ensemble est orchestré par **Airflow** et observé de bout en bout.

Ahmed mokni — Master Data & IA, HETIC (AIA01)

```
                    🥉 BRONZE (MinIO, JSON Lines bruts)    🥈 SILVER (MinIO, Parquet validé)     🥇 GOLD (Elasticsearch)
 ArXiv (dump Kaggle) ──► arxiv-raw/ingest_date=…  ──GX──►  arxiv-clean/papers/*.parquet ──┬──► arxiv-papers          (Module 1)
 Hacker News (API)   ──► hn-raw/date=…            ──GX──►  hn-clean/items/*.parquet ──────┤
 OpenAlex (API)      ──► citations-raw/ingest_date=… ─GX─► citations-clean/works/*.parquet┴─► Spark ─► arxiv-papers-enriched (Module 2)
                                                                       │                          (es-hadoop)
                                                                       └──► PostgreSQL ──► dbt (staging / marts)
 Airflow : DAG batch hebdomadaire + DAG micro-batch HN (5 min) · Logstash : logs Airflow → pipeline-logs-airflow-*
 Kibana  : Monitoring pipeline · Exploration scientifique · Insights (Gold augmentée)
```

Règle Medallion appliquée strictement : **Kibana ne lit que la couche Gold et les index d'observabilité** ;
aucune donnée Bronze ou Silver n'est indexée dans Elasticsearch.

---

## 1. Correspondance brief → fichiers

### Module 1 — SciPulse (Medallion ArXiv + Hacker News)

| Étape du brief | Réalisation | Fichiers |
|---|---|---|
| Bronze : données brutes, fidèles à la source | lignes JSON **inchangées**, partitionnées par date d'ingestion, manifeste ; HN : métadonnées techniques `_fetched_at`, `_feed`, `_story_id` | `src/bronze/arxiv.py`, `src/bronze/hn.py`, `src/lake/s3.py` |
| Analyse de qualité et statistiques descriptives | notebook exécuté sur le jeu de test | `notebooks/01_analyse_qualite.ipynb` |
| Procédure de nettoyage | règles documentées, fonctions pures testées | `src/silver/cleaning.py`, `tests/unit/test_silver_cleaning.py` |
| Great Expectations ≥ 5 expectations / source | ArXiv : 12 (Bronze) + 16 (Silver) · HN : 10 + 11 · suites Bronze = diagnostic, Silver = **contrat bloquant** | `src/quality/validate_arxiv.py`, `src/quality/validate_hn.py`, `gx/expectations/` |
| Silver en Parquet | schémas explicites (pyarrow), timestamps µs UTC | `src/silver/arxiv.py`, `src/silver/hn.py`, `src/silver/schemas.py` |
| Gold : uniquement dans Elasticsearch | index `arxiv-papers` (analyseur scientifique, synonymes, completion) | `src/gold/index_arxiv.py`, `src/utils/mappings.py` |
| Dashboard de monitoring | volume par couche, taux d'erreur GX, fraîcheur, états / durées des tâches | `src/monitoring/kibana_setup.py`, `src/monitoring/pipeline_metrics.py`, `src/monitoring/events.py` |
| Dashboard d'exploration | catégories, domaines, évolution temporelle, articles récents | idem, `kibana/scipulse_saved_objects.ndjson` |
| Orchestration | DAG batch + DAG micro-batch, ordre Bronze → validation → Silver → validation → Gold | `airflow/dags/` |

### Module 2 — SciPulse Insights (citations + Spark)

| Étape du brief | Réalisation | Fichiers |
|---|---|---|
| Source de citations (Bronze / Silver) | OpenAlex, interrogation par lots de DOI | `src/bronze/citations.py`, `src/silver/citations.py` |
| Normalisation des clés DOI / arxiv_id | DOI en minuscules sans préfixe, DOI ArXiv `10.48550/arxiv.<id>`, anciens identifiants | `src/utils/arxiv_utils.py` |
| ≥ 3 expectations citations | 10 expectations (bloquantes) | `src/quality/validate_citations.py` |
| Jointure à 3 sources + taux de correspondance | `match_citations`, `join_sources`, `coverage` | `src/spark/insights_job.py` |
| Broadcast vs sort-merge (`.explain()`) | `explain_strategies`, option `--explain` | idem, `docs/02_pipeline_spark.md` § 2 |
| Window functions | `lag` (croissance annuelle), `percent_rank` (percentile de cohorte), `row_number` (rang, dédoublonnage) | idem |
| TF-IDF distribué (MLlib) | `RegexTokenizer → StopWordsRemover → CountVectorizer → IDF` | idem |
| Score d'impact composite | `100 × (0,5·S_citations + 0,3·S_HN + 0,2·S_récence)`, normalisé par cohorte | idem, justification `docs/02_pipeline_spark.md` § 5 |
| Écriture `arxiv-papers-enriched` via elasticsearch-hadoop | `org.elasticsearch.spark.sql`, `es.mapping.id = arxiv_id` | `write_elasticsearch`, `docker/*/download-jars.sh` |
| Tâche Spark dans le DAG | `spark_insights` (spark-submit sur le cluster standalone) | `airflow/dags/dag_arxiv_pipeline.py`, `docker/spark/` |
| Dashboard augmenté | très cités absents de HN · viraux peu cités · sous-domaines en croissance · top impact · mots-clés | `src/monitoring/kibana_setup.py` |

### Livrables

| Livrable | Emplacement |
|---|---|
| README (dont justification des bibliothèques) | ce fichier, § 5 |
| Notebook d'analyse de qualité | `notebooks/01_analyse_qualite.ipynb` |
| Documentation / notebook du pipeline Spark | `docs/02_pipeline_spark.md`, `notebooks/02_pipeline_spark.ipynb` |
| Procédure de validation sur jeu de test | `docs/validation.md`, `tests/fixtures/`, `tests/integration/test_pipeline_e2e.py` |
| Captures Kibana | `docs/screenshots/` (liste et noms attendus dans `docs/screenshots/README.md`) |
| Configurations | `docker-compose.yml`, `docker/`, `.env.example`, `gx/`, `kibana/`, `dbt/` |

---

## 2. Prérequis

- **Docker Desktop ≥ 4.x** avec **12 à 16 Go de RAM** alloués (Elasticsearch, Kibana, Logstash, Airflow, Spark, MinIO, PostgreSQL).
  Linux : `sudo sysctl -w vm.max_map_count=262144`.
- **Python 3.11** sur l'hôte (Great Expectations 0.18 ne supporte ni pandas 3 ni Python 3.13).
- Le dump ArXiv Kaggle `Cornell-University/arxiv` (`arxiv-metadata-oai-snapshot.json`) dans `data/`,
  **ou** des identifiants Kaggle dans `.env` pour un téléchargement automatique.
- Facultatif : une clé API OpenAlex gratuite (`OPENALEX_API_KEY`) pour le corpus complet (voir `docs/02_pipeline_spark.md` § 1).

---

## 3. Démarrage de zéro

```bash
cp .env.example .env                     # Linux : echo "AIRFLOW_UID=$(id -u)" >> .env
make venv && source .venv/bin/activate   # dépendances de l'hôte (scripts, tests, notebooks)
make subset                              # data/arxiv-cs-subset-100k.json (100 000 articles cs.* / stat.ML)
make up                                  # build des images Airflow et Spark, démarrage de la stack
```

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| MinIO (console) | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Spark master | http://localhost:8090 | — |

Puis, dans Airflow, activer les deux DAG (ils sont en pause à la création) :

1. `scipulse_hn_poller` : alimente le Bronze / Silver HN toutes les 5 minutes ;
2. `scipulse_arxiv_pipeline` : lancer un run manuel (*Trigger DAG*) ;
3. `make kibana` : data views et trois dashboards (ou import de `kibana/scipulse_saved_objects.ndjson`).

Sans Airflow, depuis l'hôte : `make pipeline` (Bronze → Silver → Gold partielle), puis `make spark`, `make metrics`, `make kibana`.
`make help` liste tous les raccourcis.

### Validation sur jeu de test

```bash
make test        # tests unitaires (aucun service)
make test-e2e    # pipeline complet sur le jeu de test : moto (S3), GX éphémère, Spark local
make clean && make up         # stack vide ; attendre que tous les services soient prêts (make status)
make validate-test-set         # même jeu de test sur la stack réelle → Kibana
```

Détail et résultats attendus : `docs/validation.md`.

---

## 4. Les DAG

**`scipulse_arxiv_pipeline`** (`@weekly`) :

```
init_indexes → bronze_arxiv → validate_bronze_arxiv → silver_arxiv → validate_silver_arxiv
                                 (diagnostic)                           (contrat, bloquant)
     validate_silver_arxiv ─┬─► gold_index_arxiv                         (Module 1 : arxiv-papers)
                            ├─► transform_dbt                            (Silver → PostgreSQL → dbt seed/run/test)
                            └─► bronze_citations → silver_citations → validate_silver_citations → spark_insights
                                                                                           (Module 2 : arxiv-papers-enriched)
     [gold_index_arxiv, transform_dbt, spark_insights] ─► collect_pipeline_metrics   (trigger_rule = all_done)
```

**`scipulse_hn_poller`** (toutes les 5 min, `max_active_runs = 1`) :
`bronze_hn → validate_bronze_hn → silver_hn → validate_silver_hn → collect_pipeline_metrics`

- Une validation **Bronze** en échec est documentée (Data Docs) mais n'arrête pas le pipeline : le Bronze est
  par définition brut. Une validation **Silver** en échec arrête la branche : aucune donnée non conforme
  n'atteint la Gold (*circuit breaker*).
- Les tâches s'exécutent dans l'image `scipulse-airflow` (virtualenv dédié `/opt/scipulse-venv`, Java 17 et jars
  Spark embarqués) ; `spark_insights` lance `spark-submit` en mode client vers `spark://spark-master:7077`.
- Observabilité : chaque étape écrit des événements dans `pipeline-logs` (`layer_write`, `quality_check`,
  `cleaning_report`, `join_coverage`, `layer_snapshot`, `airflow_task` via `airflow/dags/scipulse_callbacks.py`) ;
  Logstash collecte en plus les fichiers de logs Airflow (`docker/logstash/pipeline/airflow-logs.conf`).

---

## 5. Choix techniques et justification des bibliothèques

| Besoin | Choix | Pourquoi (et alternatives écartées) |
|---|---|---|
| Data lake Bronze / Silver | **MinIO** + **boto3** | API S3 standard : le même code fonctionne sur AWS S3 ; boto3 est la référence et se simule avec **moto** dans les tests (le SDK `minio` n'a pas d'équivalent aussi complet) |
| Format Bronze | **JSON Lines** inchangé | fidélité à la source (le Bronze se rejoue à l'identique), lecture en flux ligne à ligne |
| Format Silver | **Parquet** via **pyarrow** avec schémas explicites | colonnaire et compressé, typé ; lu nativement par Spark. Schéma explicite : une colonne entièrement nulle garde son type et les timestamps sont écrits en µs (Spark 3.5 refuse les nanosecondes de pandas) |
| Nettoyage Silver | **pandas** | volumes par fichier modestes (≤ 50 000 lignes), règles lisibles et testables ; Spark est réservé aux jointures et agrégats du Module 2 |
| Qualité | **Great Expectations 0.18** (API programmatique) | standard du cours, Data Docs HTML, suites versionnées en JSON ; 0.18 plutôt que 1.x car l'API 1.x a changé et l'écosystème du cours l'utilise |
| Sources HTTP | **requests** | API Hacker News (Firebase) et OpenAlex : appels REST simples, reprise exponentielle maison |
| Citations | **OpenAlex** | lots de 50 DOI par requête, DOI ArXiv DataCite pour les prépublications, `counts_by_year`, licence CC0 ; Semantic Scholar sans clé est trop lent pour 100 000 articles |
| Traitement distribué | **PySpark 3.5.3** + **Spark MLlib** | jointure à trois sources, window functions et TF-IDF à l'échelle du corpus ; MLlib évite de sortir les données de Spark (scikit-learn imposerait une collecte sur le driver) |
| Lecture MinIO depuis Spark | **hadoop-aws 3.3.4** + **aws-java-sdk-bundle 1.12.262** | versions alignées sur le Hadoop 3.3.4 embarqué par PySpark 3.5.3 (`s3a://`) |
| Écriture Gold | **elasticsearch-hadoop** (`elasticsearch-spark-30_2.12` 8.13.4) | écriture parallèle depuis les executors, imposée par le brief ; version = version du cluster ES |
| Serving / recherche | **Elasticsearch 8.13** + client **elasticsearch-py** | recherche plein texte, agrégations pour Kibana ; mappings explicites (`dynamic: false`) |
| Orchestration | **Airflow 2.9** (LocalExecutor) | DAG Python, retries, callbacks ; LocalExecutor suffit sur un nœud (pas de Redis / Celery) |
| Logs | **Logstash** | collecte et parsing (dissect / grok) des logs Airflow ; les données métier n'y transitent pas |
| Entrepôt | **PostgreSQL** + **dbt-postgres** | modèles SQL testés (staging / marts) à partir du Silver ArXiv |
| Tests | **pytest** + **moto** | pipeline complet sans aucun service ; Spark en `local[2]` |
| Notebooks | **matplotlib**, **JupyterLab** | visualisations simples de l'analyse de qualité |

Autres choix notables :

- **Subset ArXiv** : échantillon aléatoire reproductible (`--seed 42`) plutôt que les 100 000 premiers articles :
  le dump est trié par identifiant, donc chronologiquement ; les premiers articles sont tous anciens.
- **`date_published`** = date de la version v1 (`versions[0].created`) ; `update_date` présente des pics artificiels
  (mises à jour en masse du dump).
- **Liens HN → ArXiv** : version retirée (`2303.08774v3` → `2303.08774`), anciens identifiants (`cs/0112017`),
  liens `/abs/`, `/pdf/`, `/html/` et liens dans le texte HTML des posts.
- **Synonymes** : filtre `synonym` à l'indexation et `synonym_graph` à la recherche (synonymes multi-mots).
- **Idempotence** : chaque tâche peut être rejouée (partition Bronze réécrite, Silver reconstruit,
  `arxiv_id` comme identifiant de document dans les deux index Gold).

---

## 6. Tests

```bash
pytest tests/unit -v          # règles Silver, couches du lac (moto), mappings, search_service, DAG, utilitaires
pytest tests/integration -v   # Bronze → Silver → GX → Spark → Gold sur tests/fixtures/
```

Les tests de DAG nécessitent Airflow installé (ils sont ignorés sinon) ; le test Spark nécessite Java.

---

## 7. Limites connues

- Le projet a été développé et testé **sans la stack Docker complète** : tout ce qui est Python et Spark est
  vérifié par les tests (y compris le job Spark de bout en bout en local) ; la construction des images,
  les appels réels aux API (Kaggle, Hacker News, OpenAlex), l'écriture es-hadoop vers un vrai cluster et le rendu
  Kibana se vérifient avec `make validate-test-set` (`docs/validation.md`).
- Le budget gratuit d'OpenAlex sans clé limite le nombre de requêtes quotidiennes : utiliser `OPENALEX_API_KEY`
  (gratuite) pour le corpus complet, ou `--max-articles` pour un essai.
- Les dashboards sont générés par code (`kibana_setup.py`) : une modification faite à la main dans Kibana doit être
  ré-exportée (`python -m src.monitoring.kibana_setup --export-from-kibana kibana/export.ndjson`).

---

## 8. Dépannage

| Symptôme | Cause / solution |
|---|---|
| `validate_silver_*` en échec | contrat Silver non respecté : ouvrir les Data Docs (`gx/uncommitted/data_docs/local_site/index.html`) ; la Gold n'a pas été modifiée |
| `spark_insights` : `PYTHON_VERSION_MISMATCH` | driver et workers doivent être en Python 3.11 (images fournies) |
| `spark_insights` : connexion refusée vers le driver | le worker doit joindre le conteneur Airflow : `spark.driver.host` est fixé à son IP par la tâche |
| Base `scipulse` absente dans PostgreSQL | `init.sql` ne s'exécute que sur un volume vide : `make clean && make up` |
| Permission denied sur `gx/` ou `airflow/logs` (Linux) | `AIRFLOW_UID=$(id -u)` dans `.env`, puis `make up` |
| Dashboards vides | élargir la période Kibana (les articles datent de plusieurs années) |
| OpenAlex HTTP 429 | budget quotidien atteint : ajouter `OPENALEX_API_KEY` ou relancer le lendemain |

---

## 9. Arborescence

```
scipulse/
├── docker-compose.yml · Makefile · requirements.txt · pyproject.toml · .env.example
├── docker/        airflow/ (image + jars + venv) · spark/ (master / worker) · logstash/ · postgres/init.sql
├── airflow/dags/  dag_arxiv_pipeline.py · dag_hn_poller.py · scipulse_callbacks.py
├── src/
│   ├── config.py
│   ├── lake/            s3.py (MinIO : JSON Lines, Parquet, partitions)
│   ├── bronze/          arxiv.py · hn.py · citations.py
│   ├── silver/          cleaning.py (règles) · schemas.py · arxiv.py · hn.py · citations.py
│   ├── quality/         gx_utils.py · samples.py · validate_arxiv.py · validate_hn.py · validate_citations.py
│   ├── gold/            index_arxiv.py
│   ├── spark/           insights_job.py
│   ├── monitoring/      events.py · pipeline_metrics.py · kibana_setup.py
│   ├── transformation/  load_to_postgres.py
│   ├── search/          search_service.py
│   └── utils/           mappings.py · arxiv_utils.py · prepare_subset.py · seed_test_set.py
├── gx/            suites et checkpoints Great Expectations (versionnés)
├── dbt/           models/staging · models/marts · seeds · tests · macros
├── kibana/        scipulse_saved_objects.ndjson · watcher_pipeline_errors.json
├── notebooks/     01_analyse_qualite.ipynb · 02_pipeline_spark.ipynb
├── docs/          02_pipeline_spark.md · validation.md · requetes_devtools.md · screenshots/
├── tests/         unit/ · integration/ · fixtures/ (jeu de test + résultats attendus)
└── data/ · logs/  (non versionnés)
```

---

## 10. Conventions de travail en groupe

- Branches par fonctionnalité (`feature/bronze-hn`, `feature/spark-insights`…), fusion par *pull request* relue.
- Messages de commit au format *Conventional Commits* : `feat(silver): dédoublonnage HN`, `fix(spark): cohorte de percentile`,
  `docs: procédure de validation`, `test: cas viral_low_citations`.
- `make test` avant chaque fusion ; le `.gitignore` exclut données, logs, `.env` et rapports générés.
