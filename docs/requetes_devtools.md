# Carnet de requêtes Kibana Dev Tools — SciPulse

Requêtes prêtes à copier dans **Kibana → Management → Dev Tools**.
Partie 1 : requêtes du TP 6 adaptées au mapping du projet. Partie 2 : corrigé des exercices `EXC-06E`.

> Deux adaptations par rapport à l'énoncé, liées au mapping réel :
> - **`significant_terms` sur `abstract` échoue** (« Fielddata is disabled on [abstract] ») car `abstract` est un champ `text`.
>   On utilise `significant_text`, conçu pour les champs texte (il ré-analyse le `_source`, à placer sous un `sampler`).
>   `significant_terms` reste la bonne agrégation pour un champ `keyword` comme `keywords`.
> - **Le phrase suggester** utilise le sous-champ `title.trigram` (shingles, sans stemming) : sur `title` (stemmé)
>   il proposerait « convolut neural network ».
> - Les filtres de date utilisent `date_published` (date de la v1). `date_updated` fonctionne aussi mais contient
>   des pics artificiels (mises à jour massives de métadonnées dans le dump).

---

## Partie 1 — TP 6

### 1. Recherche full-text

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": "reinforcement learning" } },
  "size": 3,
  "_source": ["arxiv_id", "title", "primary_category"]
}
```

Synonyme : remplacer `"reinforcement learning"` par `"RL"` — l'analyseur de recherche (`synonym_graph`) cherche aussi la forme longue.

```
GET /arxiv-papers/_search
{
  "query": { "match": { "abstract": { "query": "transformer attention image classification", "minimum_should_match": "75%" } } },
  "size": 3, "_source": ["title"]
}
```

```
GET /arxiv-papers/_search
{
  "query": { "multi_match": { "query": "generative adversarial network image synthesis", "fields": ["title^3", "abstract"], "type": "cross_fields" } },
  "size": 5, "_source": ["title", "primary_category", "date_published"]
}
```

```
GET /arxiv-papers/_search
{
  "query": { "match_phrase": { "abstract": { "query": "attention mechanism", "slop": 3 } } },
  "size": 0
}
```

### 2. Requête `bool` (exercice : cs.CV, « object detection », 2023+)

```
GET /arxiv-papers/_search
{
  "query": {
    "bool": {
      "must":     [ { "multi_match": { "query": "object detection", "fields": ["title^3", "abstract"], "type": "cross_fields" } } ],
      "filter":   [ { "term": { "primary_category": "cs.CV" } }, { "range": { "date_published": { "gte": "2023-01-01" } } } ],
      "should":   [ { "exists": { "field": "hn_score" } } ],
      "must_not": [ { "match": { "title": "survey review" } } ]
    }
  },
  "size": 5, "_source": ["title", "primary_category", "date_published", "hn_score"]
}
```

### 3. `function_score` : fraîcheur + popularité HN

```
GET /arxiv-papers/_search
{
  "query": {
    "function_score": {
      "query": { "multi_match": { "query": "large language model", "fields": ["title^3", "abstract"], "type": "cross_fields" } },
      "functions": [
        { "gauss": { "date_published": { "origin": "now", "scale": "365d", "decay": 0.5 } } },
        { "field_value_factor": { "field": "hn_score", "factor": 1.2, "modifier": "log1p", "missing": 1 } }
      ],
      "score_mode": "multiply",
      "boost_mode": "multiply"
    }
  },
  "size": 5, "_source": ["title", "date_published", "hn_score"], "explain": false
}
```

### 4. `more_like_this`

```
GET /arxiv-papers/_search
{ "query": { "match": { "title": "attention is all you need" } }, "size": 1, "_source": ["arxiv_id", "title"] }

GET /arxiv-papers/_search
{
  "query": { "more_like_this": { "fields": ["title", "abstract"], "like": [ { "_index": "arxiv-papers", "_id": "VOTRE_ID" } ],
                                 "min_term_freq": 1, "max_query_terms": 25, "min_doc_freq": 2 } },
  "size": 10, "_source": ["arxiv_id", "title", "primary_category"]
}
```

### 5. `highlight`

```
GET /arxiv-papers/_search
{
  "query": { "multi_match": { "query": "graph neural network node classification", "fields": ["title^3", "abstract"], "type": "cross_fields" } },
  "highlight": { "fields": { "title": {}, "abstract": { "fragment_size": 200, "number_of_fragments": 2 } },
                 "pre_tags": [">>>"], "post_tags": ["<<<"] },
  "size": 3, "_source": ["title"]
}
```

### 6. `suggest`

```
GET /arxiv-papers/_search
{
  "size": 0,
  "suggest": {
    "correction": {
      "text": "convlutional nueral network",
      "phrase": {
        "field": "title.trigram", "size": 3, "gram_size": 3,
        "direct_generator": [ { "field": "title.trigram", "suggest_mode": "always" } ],
        "highlight": { "pre_tag": "<em>", "post_tag": "</em>" }
      }
    }
  }
}

GET /arxiv-papers/_search
{ "suggest": { "title_autocomplete": { "prefix": "deep rein", "completion": { "field": "title.suggest", "size": 5, "skip_duplicates": true } } } }
```

### 7. Tendances : `significant_text` / `significant_terms`

```
GET /arxiv-papers/_search
{
  "size": 0,
  "query": { "range": { "date_published": { "gte": "2023-06-01" } } },
  "aggs": {
    "termes_emergents": {
      "sampler": { "shard_size": 5000 },
      "aggs": { "keywords": { "significant_text": { "field": "abstract", "size": 20, "min_doc_count": 10, "filter_duplicate_text": true } } }
    }
  }
}
```

```
GET /arxiv-papers/_search
{
  "size": 0,
  "query": { "term": { "primary_category": "cs.CL" } },
  "aggs": { "signature_NLP": { "sampler": { "shard_size": 3000 },
            "aggs": { "termes": { "significant_text": { "field": "abstract", "size": 15, "min_doc_count": 20 } } } } }
}
```

Sur les mots-clés TF-IDF (`keyword`) — comparer `terms` (fréquents) et `significant_terms` (distinctifs) :

```
GET /arxiv-papers/_search
{
  "size": 0,
  "query": { "bool": { "filter": [ { "term": { "primary_category": "cs.AI" } }, { "range": { "date_published": { "gte": "2023-01-01" } } } ] } },
  "aggs": {
    "frequents":   { "terms":             { "field": "keywords", "size": 20 } },
    "distinctifs": { "significant_terms": { "field": "keywords", "size": 20 } }
  }
}
```

### 8. Parent-child sur Hacker News

Les commentaires sont indexés si le poller tourne avec `--with-comments N` (c'est le cas dans le DAG).

```
GET /hn-items/_search
{ "size": 0, "aggs": { "par_type": { "terms": { "field": "type" } }, "par_format": { "terms": { "field": "post_type" } } } }

GET /hn-items/_search
{
  "query": { "has_child": { "type": "comment", "query": { "match": { "text": "interesting" } }, "score_mode": "max",
                            "inner_hits": { "size": 2, "_source": ["by", "text"] } } },
  "size": 5, "_source": ["title", "score", "by"]
}

GET /hn-items/_search
{ "query": { "exists": { "field": "arxiv_id_linked" } }, "size": 10,
  "_source": ["title", "url", "arxiv_id_linked", "score"], "sort": [ { "score": "desc" } ] }
```

---

## Partie 2 — Corrigé des exercices `EXC-06E`

Les énoncés citent les champs du dump brut (`update_date`, `authors_parsed`, `submitter`, `versions`).
Dans le projet, `update_date` est devenu **`date_updated`** (renommé par Logstash). Les champs `authors_parsed`,
`submitter` et `versions` n'existent que dans l'index brut **`arxiv-papers-raw`** (mapping dynamique) ;
quand c'est utile, l'équivalent sur `arxiv-papers` est donné.

### A — Briques unitaires

**1. Match simple**
```
GET /arxiv-papers/_search
{ "query": { "match": { "abstract": "reinforcement" } } }
```

**2. Term exact** — `categories` est un `keyword` multi-valué : `term` teste chaque valeur du tableau.
```
GET /arxiv-papers/_search
{ "query": { "term": { "categories": "cs.AI" } } }
```

**3. Match avec opérateur**
```
GET /arxiv-papers/_search
{ "query": { "match": { "title": { "query": "neural network", "operator": "and" } } } }
```

**4. Match phrase**
```
GET /arxiv-papers/_search
{ "query": { "match_phrase": { "title": "deep learning" } } }
```

**5. Multi-match avec boost**
```
GET /arxiv-papers/_search
{ "query": { "multi_match": { "query": "transformer", "fields": ["title^3", "abstract"] } } }
```

**6. Range sur date (année 2020)**
```
GET /arxiv-papers/_search
{ "query": { "range": { "date_updated": { "gte": "2020-01-01", "lte": "2020-12-31" } } } }
```

**7. Wildcard (auteurs commençant par « Ben »)** — sur l'index brut, chaque élément de `authors_parsed` est indexé
en `keyword` (sensible à la casse) :
```
GET /arxiv-papers-raw/_search
{ "query": { "wildcard": { "authors_parsed.keyword": { "value": "Ben*" } } }, "_source": ["title", "authors"] }
```
Équivalent sur l'index optimisé (auteurs `nested`) :
```
GET /arxiv-papers/_search
{ "query": { "nested": { "path": "authors", "query": { "wildcard": { "authors.name.keyword": { "value": "*Ben*" } } } } } }
```

**8. Exists**
```
GET /arxiv-papers/_search
{ "query": { "exists": { "field": "doi" } } }
```

### B — Contraintes logiques et agrégations

**9. must / must_not** — `math.CO` est rare dans un subset cs.* (peu ou pas de résultats : la requête reste valable).
```
GET /arxiv-papers/_search
{ "query": { "bool": { "must": [ { "term": { "categories": "math.CO" } } ],
                       "must_not": [ { "match": { "abstract": "graph" } } ] } } }
```

**10. should + minimum_should_match**
```
GET /arxiv-papers/_search
{ "query": { "bool": { "should": [ { "match": { "abstract": "quantum" } },
                                   { "match": { "abstract": "entanglement" } },
                                   { "match": { "abstract": "superposition" } } ],
                       "minimum_should_match": 2 } } }
```

**11. Fuzzy** — sur `title.trigram` (non stemmé) pour que la distance d'édition porte sur le mot réel :
```
GET /arxiv-papers/_search
{ "query": { "fuzzy": { "title.trigram": { "value": "reinfrocement", "fuzziness": 2 } } }, "_source": ["title"] }
```
Une requête `fuzzy` n'analyse pas sa saisie : sur `title` (stemmé), « reinfrocement » serait comparé au radical
indexé `reinforc` et l'écart dépasserait 2. Alternative sur `title` : un `match` avec fuzziness, qui stemme la saisie
avant la comparaison :
```
GET /arxiv-papers/_search
{ "query": { "match": { "title": { "query": "reinfrocement", "fuzziness": 2 } } }, "_source": ["title"] }
```

**12. Prefix + highlight**
```
GET /arxiv-papers/_search
{
  "query": { "prefix": { "abstract": "optim" } },
  "highlight": { "fields": { "abstract": { "fragment_size": 150, "number_of_fragments": 2 } } },
  "_source": ["title"]
}
```

**13. Top 10 catégories**
```
GET /arxiv-papers/_search
{ "size": 0, "aggs": { "top_categories": { "terms": { "field": "categories", "size": 10 } } } }
```

**14. Histogramme mensuel 2021**
```
GET /arxiv-papers/_search
{
  "size": 0,
  "query": { "range": { "date_updated": { "gte": "2021-01-01", "lt": "2022-01-01" } } },
  "aggs": { "par_mois": { "date_histogram": { "field": "date_updated", "calendar_interval": "month", "format": "yyyy-MM" } } }
}
```

**15. Bool imbriqué + boost**
```
GET /arxiv-papers/_search
{
  "query": {
    "bool": {
      "must":     [ { "match": { "title": { "query": "GAN", "boost": 2 } } } ],
      "should":   [ { "match_phrase": { "abstract": "image synthesis" } } ],
      "must_not": [ { "term": { "categories": "cs.CL" } } ]
    }
  }
}
```

### C — Requêtes avancées

**16. Multi-match cross_fields** (index brut : `authors_parsed` et `submitter` y existent)
```
GET /arxiv-papers-raw/_search
{ "query": { "multi_match": { "query": "Yoshua Bengio", "type": "cross_fields",
                              "fields": ["authors_parsed", "submitter"], "analyzer": "standard", "operator": "and" } } }
```
`best_fields` prend le score du **meilleur champ** : adapté quand tous les termes doivent se trouver dans un même champ
(« deep learning » dans le titre). `cross_fields` traite les champs comme **un seul grand champ** : adapté quand les termes
d'une même entité sont répartis entre plusieurs champs (prénom dans un élément, nom dans un autre, ou titre + abstract).

**17. Function score (gauss centré sur 2023-01-01)**
```
GET /arxiv-papers/_search
{
  "query": {
    "function_score": {
      "query": { "match": { "abstract": "attention mechanism" } },
      "functions": [ { "gauss": { "date_updated": { "origin": "2023-01-01", "scale": "180d", "decay": 0.5 } } } ],
      "boost_mode": "multiply"
    }
  }
}
```

**18. Script score sur le nombre de versions** — sur l'index optimisé, `versions_count` est un entier :
```
GET /arxiv-papers/_search
{
  "query": {
    "script_score": {
      "query": { "multi_match": { "query": "BERT", "fields": ["title", "abstract"] } },
      "script": { "source": "double n = doc['versions_count'].size() == 0 ? 1 : doc['versions_count'].value; return _score * Math.log(1 + n);" }
    }
  }
}
```
Sur l'index brut (mapping dynamique), on compte les valeurs distinctes de `versions.version` :
`doc.containsKey('versions.version.keyword') ? doc['versions.version.keyword'].size() : 1`.

**19. PIT + search_after (pagination profonde)**
```
POST /arxiv-papers/_pit?keep_alive=2m

GET /_search
{
  "size": 500,
  "query": { "term": { "categories": "astro-ph.CO" } },
  "pit": { "id": "<ID_RENVOYÉ>", "keep_alive": "2m" },
  "sort": [ { "date_updated": "desc" }, { "_shard_doc": "asc" } ]
}

GET /_search
{
  "size": 500,
  "query": { "term": { "categories": "astro-ph.CO" } },
  "pit": { "id": "<ID_RENVOYÉ_PAR_LA_PAGE_PRÉCÉDENTE>", "keep_alive": "2m" },
  "sort": [ { "date_updated": "desc" }, { "_shard_doc": "asc" } ],
  "search_after": [ <valeurs "sort" du dernier hit de la page précédente> ]
}

DELETE /_pit
{ "id": "<ID>" }
```
`from + size` oblige chaque shard à trier `from + size` documents à chaque page, et s'arrête à 10 000
(`index.max_result_window`). `search_after` repart du dernier tri vu : coût constant par page. Le PIT fige une vue
cohérente de l'index pendant le parcours (les documents ajoutés entre deux pages ne décalent rien) ; `_shard_doc` sert
de départage unique et peu coûteux.

**20. Agrégations pipeline**
```
GET /arxiv-papers/_search
{
  "size": 0,
  "query": { "range": { "date_published": { "gte": "2019-01-01" } } },
  "aggs": {
    "par_categorie": {
      "terms": { "field": "primary_category", "size": 10 },
      "aggs": {
        "par_annee": {
          "date_histogram": { "field": "date_published", "calendar_interval": "year", "format": "yyyy", "min_doc_count": 0 },
          "aggs": {
            "moyenne_mobile": { "moving_fn": { "buckets_path": "_count", "window": 3, "script": "MovingFunctions.unweightedAvg(values)" } }
          }
        },
        "tri": { "bucket_sort": { "sort": [ { "_count": { "order": "desc" } } ] } }
      }
    }
  }
}
```
Les agrégations *pipeline* calculent sur le **résultat d'autres agrégations** (les buckets), pas sur les documents :
une moyenne mobile ou un tri de buckets n'a de sens qu'une fois les comptages faits. `moving_avg` a été supprimée en 8.x :
on utilise `moving_fn`. « Après 2018 » est interprété comme « à partir du 1er janvier 2019 ».
