# Captures d'écran Kibana

Captures à produire après `make validate-test-set` (procédure : `docs/validation.md`, section 3),
période Kibana réglée sur « Last 30 years ». Déposer les fichiers PNG dans ce dossier avec ces noms :

| Fichier | Écran | Ce que la capture doit montrer |
|---|---|---|
| `01_monitoring.png` | Dashboard *SciPulse — Monitoring Pipeline* | volume par couche (Bronze 75 → Silver 71 → Gold 71), taux d'erreur GX par suite, fraîcheur des couches |
| `02_exploration.png` | Dashboard *SciPulse — Exploration scientifique* | catégories `cs.CL` / `cs.LG` / `cs.CV`, publications par année |
| `03_insights.png` | Dashboard *SciPulse — Insights (Gold augmentée)* | top impact (`2305.10001` en tête), segments |
| `04_academic_only.png` | Panneau *Très cités, jamais mentionnés sur Hacker News* | `1706.03762` présent |
| `05_viral_low.png` | Panneau *Viraux sur Hacker News, peu cités* | `2303.08774` |
| `06_growth.png` | Panneau *Croissance annuelle et vélocité de citation par catégorie* | `cs.LG` 2023 ≈ 2,17 |
| `07_devtools_count.png` | Dev Tools : `GET arxiv-papers-enriched/_count` et `_doc/2305.10001` | 71 documents, segment `academic_and_viral` |
| `08_airflow_dag.png` | Airflow, vue *Graph* du DAG `scipulse_arxiv_pipeline` | enchaînement Bronze → Silver → Gold, tâche `spark_insights` |
| `09_gx_data_docs.png` | Data Docs Great Expectations (`gx/uncommitted/data_docs/local_site/index.html`) | suites Bronze en échec documenté, suites Silver au vert |
