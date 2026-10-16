# Procédure de validation sur jeu de test

Le pipeline est validé sur un **jeu de test déterministe et versionné** dont les résultats attendus sont
connus à l'avance. La validation se fait à deux niveaux :

1. **automatique, sans service** : `pytest` rejoue tout le pipeline (Bronze → Silver → Great Expectations →
   Spark → Gold) sur le jeu de test et vérifie chaque résultat attendu ;
2. **sur la stack Docker réelle** : le même jeu de test est chargé dans MinIO et traité par les vraies tâches ;
   on contrôle visuellement les dashboards Kibana (captures d'écran dans `docs/screenshots/`).

## 1. Le jeu de test

Généré par `tests/fixtures/make_fixtures.py` (graine fixe : régénération identique), versionné dans
`tests/fixtures/` :

| Fichier | Contenu |
|---|---|
| `arxiv_bronze.jsonl` | 75 lignes au format du dump Kaggle ArXiv (catégories `cs.CL`, `cs.LG`, `cs.CV`, années 2001 à 2023) |
| `hn_items.json` | 6 items au format de l'API Hacker News (stories, Ask HN, Show HN, commentaire, item supprimé) |
| `openalex_works.json` | 66 œuvres au format de l'API OpenAlex `/works` |
| `expected.json` | résultats attendus |

### Anomalies volontaires (Bronze) et traitement attendu (Silver)

| Anomalie | Traitement attendu | Vérification |
|---|---|---|
| ligne JSON invalide | rejetée, `json_errors = 1` | test e2e + rapport `cleaning_report` |
| article hors périmètre (`hep-th`) | rejeté, `out_of_scope = 1` | test e2e |
| doublon d'identifiant (`2101.00006` v1 / v2) | version la plus récente conservée (titre « … (v2) ») | test e2e |
| abstract manquant | rejeté, `missing_abstract = 1` | test e2e |
| abstract trop court | conservé, `abstract_too_short = True` | test e2e |
| DOI `https://doi.org/10.1109/CVPR.2021.ABC` | `10.1109/cvpr.2021.abc` | test e2e |
| date mal formée | comptée (`invalid_dates = 1`), date dérivée des versions | test e2e |
| ancien identifiant `cs/0112017`, titre sur plusieurs lignes | identifiant conservé, titre normalisé | test e2e |
| item HN supprimé (`deleted`) | rejeté au Silver ; détecté par la suite GX Bronze | test e2e + test GX |
| lien ArXiv dans l'URL (`…/abs/2303.08774v3`) ou dans le texte HTML | `arxiv_id_linked = 2303.08774`, version retirée | test e2e |

**Volumes attendus** : 75 lignes Bronze → **71 articles Silver** ; 6 items HN → 5 au Silver ; 66 œuvres OpenAlex.

### Cas analytiques construits (Gold)

| Article | Construction | Résultat attendu |
|---|---|---|
| `1706.03762` *Attention Is All You Need* | 120 000 citations, cité sur HN uniquement dans un **commentaire** | segment `academic_only`, `has_hn = false`, rattaché par DOI ArXiv |
| `2303.08774` *GPT-4 Technical Report* | 2 stories HN (900 + 12 points), 2 citations | segment `viral_low_citations`, `hn_score_total = 912` |
| `2305.10001` | 300 points HN, 5 000 citations via son **DOI d'éditeur** | segment `academic_and_viral`, `citation_match_key = journal_doi`, score d'impact **maximal** du corpus et rang 1 de `cs.LG` |
| catégorie `cs.LG` | 6 articles en 2022, 19 en 2023 | `category_yoy_growth` 2023 > 0 (≈ 2,17) |
| catégorie `cs.CL` | articles en 2001, 2017 et 2023, aucune année consécutive | `category_yoy_growth` nul (pas de croissance calculée entre années non consécutives) |

## 2. Validation automatique (aucun service requis)

```bash
make test        # tests unitaires : règles de nettoyage, mappings, DAG, search, couches du lac
make test-e2e    # pipeline complet sur le jeu de test : pytest tests/integration -v
```

`tests/integration/test_pipeline_e2e.py` remplace MinIO par **moto** (S3 en mémoire), les API Hacker News et
OpenAlex par les fichiers de fixtures, utilise un contexte Great Expectations éphémère et Spark en `local[2]` :

| Test | Ce qui est vérifié |
|---|---|
| `test_bronze_is_faithful_and_silver_applies_rules` | Bronze identique octet pour octet à la source (découpage + manifeste) ; chaque règle Silver appliquée et comptée ; types Parquet (timestamps µs UTC) ; liens HN → ArXiv ; clés de citation normalisées |
| `test_great_expectations_suites` | les suites **Bronze** détectent les anomalies (doublon, date, item supprimé) ; les suites **Silver** passent intégralement après nettoyage |
| `test_spark_gold_answers_analytical_questions` | 71 documents Gold ; segments, totaux HN et clés de rattachement attendus ; score dans [0, 100] et maximal pour `2305.10001` ; window functions ; mots-clés TF-IDF ; taux de correspondance ; plans `SortMergeJoin` vs `BroadcastHashJoin` |

Les notebooks `notebooks/01_analyse_qualite.ipynb` et `notebooks/02_pipeline_spark.ipynb` ont été exécutés sur ce
même jeu de test : leurs sorties montrent les valeurs intermédiaires (rapports de nettoyage, taux de
correspondance, plans physiques, scores, segments).

## 3. Validation sur la stack réelle (Kibana)

À faire sur une stack **vide** : le jeu de test ne doit pas se mélanger aux données réelles.

```bash
make clean && make up          # stack neuve ; attendre que tous les services soient prêts (make status)
make venv && source .venv/bin/activate
make validate-test-set
```

`make validate-test-set` enchaîne : création des index → `seed-test-set` (fixtures → Bronze MinIO, via
`src/utils/seed_test_set.py`, mêmes fonctions d'écriture que la production) → suites GX Bronze → Silver HN et
citations → suites GX Silver → Gold `arxiv-papers` → **job Spark sur le cluster** (`arxiv-papers-enriched`, via
elasticsearch-hadoop) → métriques de couche → dashboards Kibana.

### Contrôles

**Elasticsearch** (Dev Tools, `http://localhost:5601/app/dev_tools`) :

```
GET arxiv-papers/_count                                   → 71
GET arxiv-papers-enriched/_count                          → 71
GET arxiv-papers-enriched/_doc/2305.10001                 → segment academic_and_viral, impact_rank_in_category 1
GET arxiv-papers-enriched/_search
{ "size": 0, "aggs": { "segments": { "terms": { "field": "segment" } } } }
GET pipeline-logs/_search
{ "query": { "term": { "type": "join_coverage" } }, "sort": [{ "@timestamp": "desc" }], "size": 1 }
```

**Kibana** (`http://localhost:5601`, menu *Dashboards*). Les articles du jeu de test datent de 2001 à 2023 :
élargir la période à « Last 30 years » pour les dashboards d'exploration et d'insights.

| Dashboard | Panneau | Attendu sur le jeu de test |
|---|---|---|
| SciPulse — Monitoring Pipeline | Volume ingéré par couche et par source | Bronze ArXiv 75, Silver ArXiv 71, Gold 71 (`arxiv-papers`) et 71 (`arxiv-papers-enriched`) |
| | Taux d'erreur Great Expectations par suite | Bronze ArXiv et HN > 0 (anomalies volontaires), toutes les suites Silver à 0 |
| | Fraîcheur et volume des couches | 6 buckets + 2 index, fraîcheur de quelques minutes |
| SciPulse — Exploration scientifique | Distribution des catégories | `cs.CL` 30, `cs.LG` 25, `cs.CV` 16 (catégories primaires ; l'article `hep-th` a été rejeté) |
| | Publications par année | 2001 : 1, 2017 : 13, 2021 : 16, 2022 : 6, 2023 : 35 |
| SciPulse — Insights (Gold augmentée) | Top 15 — score d'impact composite | `2305.10001` en tête |
| | Segments analytiques | `academic_and_viral` 1, `viral_low_citations` 1, `academic_only` ≥ 1 |
| | Très cités, jamais mentionnés sur Hacker News | contient `1706.03762` |
| | Viraux sur Hacker News, peu cités | `2303.08774` |
| | Croissance annuelle et vélocité de citation par catégorie | `cs.LG` : croissance 2023 ≈ 2,17 |

Les captures à produire sont listées dans `docs/screenshots/README.md`.

## 4. Ce que cette procédure ne couvre pas

- Le volume réel (100 000 articles) : temps d'exécution, mémoire du worker Spark, pagination OpenAlex.
- La disponibilité des API externes (Kaggle, Hacker News, OpenAlex) : couverte par les reprises
  (`retries` Airflow, reprise exponentielle des appels HTTP), pas par les tests.
- La collecte des logs Airflow par Logstash : vérifiable dans Kibana (*Discover*, data view `pipeline-logs*`,
  filtre `log_type : airflow_log`) une fois les DAG exécutés.
