# Réponses aux questions et exercices des énoncés

## TP 1 — Prise en main

**Quel pilier DataOps est déjà en action avec la seule structure du dépôt ?**
La **reproductibilité par le versioning « as code »** : l'infrastructure (`docker-compose.yml`, configs Logstash,
`init.sql`), les transformations (`dbt/`), les tests (`tests/`, suites GX) et l'orchestration (`airflow/dags/`) sont
du texte versionné dans Git. N'importe qui recrée le même environnement avec `docker compose up`. La séparation
`data/` (non versionné) / code (versionné) et le `Makefile` relèvent aussi de l'automatisation.

**Combien de catégories uniques contient le subset ?**
```bash
python -c "
import json, sys
cats = set()
for line in open('data/arxiv-raw/arxiv-cs-subset-100k.json'):
    cats.update(json.loads(line)['categories'].split())
print(len(cats), 'catégories uniques')
"
```
(Après ingestion : agrégation `cardinality` sur `categories`.)

**Observations sur les données brutes**
- `authors` est une chaîne brute (« A, B and C ») ; `authors_parsed` donne la version structurée `[nom, prénom, suffixe]`.
- `categories` est une chaîne multi-valuée séparée par des espaces, la première étant la catégorie principale.
- `update_date` (AAAA-MM-JJ) = dernière mise à jour des métadonnées ; la date de publication est `versions[0].created`
  (format RFC 2822 : « Mon, 2 Apr 2007 19:18:42 GMT »).
- Titres et abstracts contiennent des retours à la ligne et des espaces multiples.

## TP 2 — Logstash vs Python

| Critère | Logstash (`arxiv.conf`) | Python (`arxiv_bulk_ingest.py`) |
|---|---|---|
| Nature | configuration déclarative | code impératif |
| Tests | difficile | `tests/unit/test_ingestion.py` |
| Erreurs | logs Logstash | compteurs + fichier *dead letter* `logs/arxiv_bulk_ingest.dead-letter.jsonl` |
| Suivi | aucun natif | débit affiché tous les 10 000 documents |
| Ressources | JVM (~0,5–1 Go) | ~50 Mo |
| Force | parsing de logs (grok), connecteurs | logique métier, intégration Python |

Les deux produisent **les mêmes documents** (mêmes noms de champs, mêmes formats de date, y compris `date_published`) :
la comparaison de l'étape 7.2 doit montrer deux `_source` identiques (à l'ordre des clés près).

## TP 3 — Mapping

Types devinés par le mapping dynamique sur `arxiv-papers-raw` :

| Champ | Type deviné | Bon ? |
|---|---|---|
| `arxiv_id` | `text` + `keyword` | non : identifiant → `keyword` seul |
| `title` | `text` + `keyword` | partiellement : pas d'analyseur anglais |
| `abstract` | `text` + `keyword` (`ignore_above: 256`) | non : sous-champ keyword inutile, pas de stemming |
| `categories` | `text` + `keyword` | non : valeurs exactes → `keyword` |
| `primary_category` | `text` + `keyword` | non : `keyword` |
| `date_updated` | `date` | oui (déjà parsé par Logstash) |

Choix ajoutés au mapping de l'énoncé :
- **`search_analyzer` avec `synonym_graph`** : à l'indexation, le filtre `synonym` rend l'expansion visible dans
  `_analyze` (comme demandé) ; à la recherche, `synonym_graph` traite correctement les synonymes multi-mots
  (« CNN » → `cnn` OU la phrase « convolutional neural network »). Sans lui, chercher « CNN » reviendrait à chercher
  `cnn OR convolut OR neural OR network` et remonterait tout article contenant « network ».
- **`title.trigram`** (shingles) pour le phrase suggester ; **`dynamic: false`** pour refuser les champs imprévus.
- **`lenient: true`** sur les synonymes, et aucune règle ne contient de mot vide (sinon la création de l'index échoue).

## TP 4 — Qualité

- Les **doublons** sur `arxiv_id` sont toujours à 0 dans Elasticsearch : `_id = arxiv_id` rend l'ingestion idempotente,
  un même article réécrit simplement son document. L'expectation d'unicité reste utile pour les autres sources (CSV, PostgreSQL).
- Le nettoyage **marque** les abstracts trop courts (`abstract_too_short`) au lieu de les supprimer : la décision
  métier est tracée et réversible ; un abstract absent reste `null` (on ne maquille pas la donnée pour faire passer un test).
- La suite ArXiv contient 12 expectations (+2 au stade « enriched »), la suite HN 9.

## TP 5 — dbt

- `generate_schema_name` est surchargé : sans cela dbt crée `public_staging` / `public_marts` au lieu de `staging` / `marts`.
- La couche brute PostgreSQL est entièrement en `TEXT` ; le typage se fait dans `stg_arxiv_papers` (pattern ELT).
- Tests : 21 (génériques + 2 tests singuliers : pas de date future, total mensuel = total staging).

## TP 6 — Recherche

Voir `docs/requetes_devtools.md`. `significant_terms` sur un champ `text` exige `fielddata: true` (coûteux en mémoire) :
on utilise `significant_text`, sous un `sampler`.

## TP 7 — Orchestration

- Le DAG de l'énoncé enchaîne l'ingestion Logstash puis dbt, alors que dbt lit `arxiv-papers` : il manquait la
  **réindexation** (TP 3) et le **nettoyage** (TP 4). Les deux tâches ont été ajoutées.
- `validate_raw_arxiv` valide maintenant les **fichiers avant ingestion** (shift-left) ; ses échecs sont tolérés.
  `validate_enriched` est un vrai **circuit breaker** : en cas d'échec, le run est rouge.
- `collect_pipeline_metrics` a `trigger_rule=all_done` : les métriques sont collectées même après un échec.
- Simulation d'échec (étape 8) : tâches en aval en `upstream_failed`, puis **Clear** sur la tâche rouge pour ne
  relancer qu'elle et l'aval — possible parce que chaque tâche est idempotente.

## TP 8 — Observabilité

- `pipeline-logs` reçoit trois types de documents : `pipeline_metrics` (script), `airflow_task` (callbacks Airflow,
  un document par fin de tâche avec état et durée) et `alert` (Watcher).
- **Watcher** n'est pas inclus dans la licence *basic* : `make watcher` active la licence d'essai de 30 jours.
  Alternative gratuite : Kibana → Stack Management → Rules → « Elasticsearch query » sur `pipeline-logs`
  (`type:"airflow_task" and state:"failed"`, seuil > 0 sur 15 min).
