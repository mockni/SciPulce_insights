# SciPulse Insights — Pipeline Spark (Module 2)

Ce document décrit le job `src/spark/insights_job.py`, exécuté par la tâche Airflow `spark_insights`
du DAG `scipulse_arxiv_pipeline`, et justifie ses choix. Le notebook `notebooks/02_pipeline_spark.ipynb`
déroule les mêmes fonctions étape par étape, avec les résultats intermédiaires sur le jeu de test.

```
 🥈 Silver (MinIO, Parquet)                                 🥇 Gold (Elasticsearch)
 arxiv-clean/papers/*.parquet      ─┐
 hn-clean/items/hn-items.parquet   ─┼─► Spark ─► arxiv-papers-enriched ─► Kibana « SciPulse Insights »
 citations-clean/works/works.parquet┘   (es-hadoop)
```

## 1. Source de citations : OpenAlex

| Critère | OpenAlex | Semantic Scholar |
|---|---|---|
| Interrogation par lot | `filter=doi:A\|B\|…` (50 DOI par requête dans notre code) | une requête par article, ou lot limité |
| Articles ArXiv sans DOI d'éditeur | retrouvés par le DOI DataCite `10.48550/arxiv.<id>` | par identifiant ArXiv |
| Historique des citations | `counts_by_year` dans la réponse | requête supplémentaire |
| Accès | gratuit ; une clé API gratuite multiplie le budget quotidien par 10 | débit très bas sans clé |
| Licence | CC0 | ODC-BY / conditions d'API |

Pour 100 000 articles, il faut de l'ordre de 2 000 à 4 000 requêtes (DOI ArXiv + DOI d'éditeur quand il existe,
50 DOI par requête, une seule page de résultats par lot) : un run complet tient dans une journée avec une clé gratuite (`OPENALEX_API_KEY`).

**Ingestion** (`src/bronze/citations.py`) : réponses écrites telles quelles dans `citations-raw/ingest_date=…/`
(JSON Lines + manifeste), avec `_fetched_at` et `_query_dois`. Débit limité, reprise exponentielle sur 429 / 5xx.

**Silver** (`src/silver/citations.py`) : identifiant OpenAlex court (`W…`), DOI en minuscules sans préfixe
`https://doi.org/`, `arxiv_id` déduit du DOI ArXiv (`normalize_doi`, `arxiv_id_from_doi`), type de clé
(`arxiv_doi` / `journal_doi`), rejet des œuvres sans DOI ou au compteur de citations invalide, dédoublonnage.
Validation : suite `citations_silver_suite` (10 expectations, bloquante).

## 2. Jointure à trois sources

### Clés

| Source | Clé | Préparée au Silver par |
|---|---|---|
| ArXiv | `arxiv_id` (sans version, ex. `2303.08774`, `cs/0112017`) ; `doi` ; `arxiv_doi` | `clean_arxiv_record` |
| Hacker News | `arxiv_id_linked` : premier identifiant ArXiv trouvé dans l'URL ou le texte du post | `detect_arxiv_link` |
| OpenAlex | `arxiv_id` (via DOI ArXiv) **ou** `doi` (DOI d'éditeur) | `clean_work` |

`match_citations` réunit les deux voies de rattachement (`unionByName`) ; si un article a deux œuvres
(prépublication et version publiée), on garde la plus citée (`row_number()` sur une fenêtre par `arxiv_id`).

### Stratégies de jointure

`ArXiv ⟕ HN agrégé ⟕ citations` (jointures gauches : la Gold contient tous les articles Silver).

| Jointure | Taille relative | Stratégie | Pourquoi |
|---|---|---|---|
| ArXiv × HN agrégé | quelques milliers de lignes au plus | **broadcast hash join** (`F.broadcast`) | la petite table est copiée sur chaque executor ; aucun shuffle de la grande table |
| ArXiv × citations | même ordre que l'ArXiv (≤ 1 ligne par article) | sort-merge par défaut, broadcast possible | à 100 000 lignes × ~10 colonnes, la table tient en mémoire ; au-delà du seuil `spark.sql.autoBroadcastJoinThreshold`, Spark (ou AQE à l'exécution) choisit le sort-merge |
| Gold × indicateurs catégorie × année | quelques centaines de lignes | broadcast | table de dimension |

`explain_strategies` produit les deux plans physiques de la jointure ArXiv × citations (lancer le job avec
`--explain`, ou voir la section 3 du notebook 02). Extrait obtenu sur le jeu de test :

```
===== sort_merge =====                                  (autoBroadcastJoinThreshold = -1)
+- SortMergeJoin [arxiv_id], [arxiv_id], LeftOuter
   :- Sort [arxiv_id ASC NULLS FIRST]
   :  +- Exchange hashpartitioning(arxiv_id, 16)        ← shuffle de la table ArXiv
   ...
===== broadcast =====
+- BroadcastHashJoin [arxiv_id], [arxiv_id], LeftOuter, BuildRight
   +- BroadcastExchange HashedRelationBroadcastMode     ← seule la petite table circule
```

Le sort-merge paie deux shuffles (`Exchange`) et deux tris ; le broadcast n'en paie aucun côté ArXiv,
au prix de la mémoire nécessaire pour copier la petite table sur chaque executor.

### Taux de correspondance (match coverage)

`coverage()` calcule, après jointure :

| Indicateur | Définition |
|---|---|
| `pct_with_citations` | part des articles rattachés à une œuvre OpenAlex |
| `pct_with_hn` | part des articles cités par au moins une story HN |
| `pct_with_both` | les deux |
| `pct_hn_links_in_corpus` | part des articles ArXiv cités sur HN qui appartiennent au corpus (les autres sont hors périmètre) |
| `openalex_works` | œuvres Silver disponibles |

Ces valeurs sont journalisées par le job et envoyées dans `pipeline-logs` (événement `join_coverage`),
visibles dans le dashboard de monitoring.

## 3. Indicateurs temporels (window functions)

| Indicateur | Calcul |
|---|---|
| `citations_last_2y` | somme de `counts_by_year` des deux dernières années civiles — fonctions d'ordre supérieur `F.filter` + `F.aggregate`, sans UDF Python |
| `citation_velocity` | `cited_by_count / max(âge en années, 1)` |
| `category_publications_year` | nombre d'articles par catégorie primaire × année |
| `category_yoy_growth` | `(N_année − N_année−1) / N_année−1` avec `lag()` sur `Window.partitionBy("primary_category").orderBy("year_published")` ; nul si l'année précédente n'a aucune publication (pas de « croissance » calculée entre deux années non consécutives) |
| `category_avg_velocity` | vélocité moyenne de la catégorie × année |
| `citation_percentile` | `percent_rank()` des citations dans la cohorte catégorie × année, calculé parmi les seuls articles ayant des données de citation |
| `impact_rank_in_category` | `row_number()` par catégorie, ordonné par score décroissant |

## 4. TF-IDF distribué (Spark MLlib)

`RegexTokenizer` (mots alphabétiques ≥ 3 lettres) → `StopWordsRemover` (anglais + ~45 mots vides du
vocabulaire scientifique : *paper, propose, results, approach…*) → `CountVectorizer` (20 000 termes,
`minDF` = 5 documents sur le corpus complet) → `IDF`. Les 8 termes de plus fort poids de chaque vecteur
deviennent le champ `keywords` (agrégation *terms* dans Kibana). L'UDF qui extrait le top-k ne parcourt que
les entrées non nulles du vecteur creux.

## 5. Score d'impact composite

$$
\text{impact} = 100 \times \left(0{,}5\,S_{\text{citations}} + 0{,}3\,S_{\text{HN}} + 0{,}2\,S_{\text{récence}}\right) \in [0, 100]
$$

| Composante | Formule | Justification |
|---|---|---|
| $S_{\text{citations}}$ | $\dfrac{\log(1+c)}{\max_{\text{cohorte}} \log(1+c)}$, cohorte = catégorie primaire × année | les citations suivent une loi à longue traîne (le notebook 01 le montre) : le logarithme évite qu'un seul article écrase l'échelle. La normalisation **par cohorte** corrige deux biais : un article ancien a eu plus de temps pour être cité, et les volumes de citations diffèrent fortement d'une discipline à l'autre |
| $S_{\text{HN}}$ | $\dfrac{\log(1+p)}{\max_{\text{corpus}} \log(1+p)}$, $p$ = points HN cumulés des stories | même raison (longue traîne) ; normalisation sur tout le corpus car la visibilité HN est récente et peu liée à la catégorie. Le maximum est calculé une fois (scalaire) : pas de fenêtre globale sur une seule partition |
| $S_{\text{récence}}$ | $e^{-\ln 2 \cdot \text{âge} / 2}$ | demi-vie de 2 ans : un article de l'année vaut 1, de 2 ans 0,5, de 4 ans 0,25. Favorise les travaux récents encore peu cités |

**Pondérations.** Les citations (0,5) sont le signal d'impact académique le plus robuste et le plus
répandu (taux de correspondance OpenAlex élevé). Le signal HN (0,3) mesure l'impact hors du monde académique,
objectif propre de SciPulse, mais il est rare (une petite fraction des articles est discutée sur HN) et
bruité. La récence (0,2) compense le retard structurel des citations sans dominer le score. Les poids sont
des constantes en tête du job (`W_CITATIONS`, `W_HN`, `W_RECENCY`), modifiables en un seul endroit, et
chaque composante est écrite dans la Gold (`score_citations`, `score_hn`, `score_recency`) pour que le score
reste explicable dans Kibana.

## 6. Segments (dashboard augmenté)

| Segment | Règle | Question du brief |
|---|---|---|
| `academic_and_viral` | très cité **et** viral | — |
| `academic_only` | `citation_percentile ≥ 0,90` dans sa cohorte, `cited_by_count ≥ 10`, absent de HN | articles très cités mais absents de HN |
| `viral_low_citations` | `hn_score_total ≥ 100` et `citation_percentile ≤ 0,50` (ou pas de données de citation) | articles viraux sur HN mais peu cités |
| `other` | le reste | — |

La troisième question (sous-domaines en croissance) s'appuie sur `category_yoy_growth` et
`category_publications_year`. Les trois cas sont construits dans le jeu de test et vérifiés par
`tests/integration/test_pipeline_e2e.py` (voir `docs/validation.md`).

## 7. Écriture Gold : elasticsearch-hadoop

```python
gold.write.format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch").option("es.port", "9200")
    .option("es.nodes.wan.only", "true")       # réseau Docker : pas de découverte des nœuds
    .option("es.mapping.id", "arxiv_id")       # un document par article → ré-exécution idempotente
    .option("es.index.auto.create", "false")   # l'index est créé avant, avec son mapping explicite
    .mode("append").save("arxiv-papers-enriched")
```

L'index est créé par `create_index(…, ENRICHED_SETTINGS)` (`src/utils/mappings.py`) : analyseur scientifique
(synonymes, stemming), `keyword` pour les agrégations, `float` / `integer` pour les indicateurs.
Le notebook 02 vérifie que chaque colonne Gold possède un mapping (`dynamic: false`).

## 8. Exécution dans Airflow

- Cluster Spark standalone `spark-master` + `spark-worker` (image `docker/spark/`, Python 3.11, comme le driver).
- `spark-submit` en **mode client** depuis le conteneur Airflow : le driver tourne dans Airflow
  (`spark.driver.host` = IP du conteneur), les executors sur le worker.
- Jars embarqués dans les images (`docker/*/download-jars.sh`) : `hadoop-aws 3.3.4`,
  `aws-java-sdk-bundle 1.12.262` (lecture `s3a://` de MinIO) et `elasticsearch-spark-30_2.12 8.13.4`.
- La tâche ne démarre qu'après `validate_silver_citations` et `validate_silver_arxiv` (contrats Silver).
  Si le Silver HN est encore vide (premier lancement), le job continue avec un DataFrame vide au schéma minimal.

Exécution hors planification :

```bash
# Rejouer uniquement la tâche Spark du DAG (mêmes options que le run planifié)
docker compose exec airflow-scheduler airflow tasks test scipulse_arxiv_pipeline spark_insights 2026-10-16
# En local, sur une copie de la couche Silver, sortie Parquet au lieu d'Elasticsearch
python -m src.spark.insights_job --master "local[*]" --lake-uri file:///chemin/lake/ --output-parquet /tmp/gold
```
