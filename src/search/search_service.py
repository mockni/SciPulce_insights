"""
SciPulse — Service de recherche full-text (TP 6, volet B)

Encapsule les requêtes Elasticsearch avancées derrière une interface Python :
  - full_text()          multi_match cross_fields + function_score (fraîcheur ; + score d'impact
                         sur l'index Gold enrichi arxiv-papers-enriched)
  - phrase_search()      match_phrase avec slop
  - more_like_this()     articles similaires à un article du corpus
  - more_like_text()     articles similaires à un texte libre
  - significant_terms()  termes distinctifs d'un sous-ensemble (tendances émergentes)
  - suggest()            correction de saisie (phrase suggester)
  - autocomplete()       autocomplétion des titres (completion suggester)
  - combined_search()    requête « tout-en-un » : recherche + highlight + aggs + suggestion

Usage en ligne de commande (démo du Jour 5) :
    python -m src.search.search_service "reinforcement learning robotics"
    python -m src.search.search_service "transformer" --categories cs.CL cs.LG --since 2022-01-01
    python -m src.search.search_service --mlt 1706.03762
    python -m src.search.search_service --suggest "convlutional nueral network"
    python -m src.search.search_service --trends --since 2023-06-01
"""

import argparse

from elasticsearch import Elasticsearch

from src.config import ES_HOST, INDEX_ARXIV, INDEX_ENRICHED

DATE_FIELD = "date_published"           # champ utilisé pour la fraîcheur et les filtres de date
SOURCE_FIELDS = ["arxiv_id", "title", "primary_category", "date_published", "date_updated",
                 "impact_score", "hn_score_total", "cited_by_count", "keywords"]
TEXT_FIELDS = {"abstract", "title"}     # champs `text` → significant_text ; sinon significant_terms


class SciPulseSearch:
    """
    Façade de recherche sur la couche Gold :
      - arxiv-papers (Module 1)            : recherche plein texte pondérée par la fraîcheur
      - arxiv-papers-enriched (Module 2)   : + pondération par le score d'impact composite
    """

    def __init__(self, es_host: str = ES_HOST, index: str = INDEX_ARXIV):
        self.es = Elasticsearch(es_host, request_timeout=30)
        self.index = index

    @property
    def enriched(self) -> bool:
        return self.index == INDEX_ENRICHED

    # ─────────────────────────────────────────────────────────────────────────
    #  Briques de requête
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _filters(categories: list[str] | None = None, date_from: str | None = None,
                 min_impact: float | None = None) -> list[dict]:
        filters = []
        if categories:
            filters.append({"terms": {"primary_category": categories}})
        if date_from:
            filters.append({"range": {DATE_FIELD: {"gte": date_from}}})
        if min_impact is not None:
            filters.append({"range": {"impact_score": {"gte": min_impact}}})
        return filters

    @staticmethod
    def _highlight(pre: str = "<mark>", post: str = "</mark>") -> dict:
        return {
            "fields": {
                "title": {"number_of_fragments": 0},
                "abstract": {"fragment_size": 200, "number_of_fragments": 2},
            },
            "pre_tags": [pre],
            "post_tags": [post],
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  Recherches
    # ─────────────────────────────────────────────────────────────────────────

    def full_text(
        self,
        query: str,
        size: int = 10,
        categories: list[str] | None = None,
        date_from: str | None = None,
        min_impact: float | None = None,
        boost_recent: bool = True,
        recency_scale: str = "365d",
    ) -> dict:
        """
        Recherche multi_match (titre ×3, mots-clés ×2, abstract) en cross_fields,
        pondérée par la fraîcheur (decay gaussien) et, sur l'index enrichi, par le score d'impact.
        """
        bool_query = {
            "must": [{
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "keywords^2", "abstract"],
                    "type": "cross_fields",
                }
            }]
        }
        filters = self._filters(categories, date_from, min_impact)
        if filters:
            bool_query["filter"] = filters

        functions = []
        if self.enriched:
            # impact_score ∈ [0, 100] → facteur ln(2 + impact/10) ∈ [0,69 ; 2,48] : jamais nul,
            # un article sans impact garde son score textuel (atténué) au lieu d'être éliminé
            functions.append({"field_value_factor": {"field": "impact_score", "factor": 0.1,
                                                     "modifier": "ln2p", "missing": 0}})
        if boost_recent:
            functions.append({"gauss": {DATE_FIELD: {"origin": "now", "scale": recency_scale, "decay": 0.5}}})

        query = {"bool": bool_query}
        if functions:
            query = {"function_score": {"query": query, "functions": functions,
                                        "score_mode": "multiply", "boost_mode": "multiply"}}
        body = {
            "query": query,
            "highlight": self._highlight(),
            "size": size,
            "_source": SOURCE_FIELDS,
        }
        return self.es.search(index=self.index, body=body)

    def phrase_search(self, phrase: str, slop: int = 2, field: str = "abstract", size: int = 10) -> dict:
        """match_phrase avec slop : « neural network architecture » à ±slop positions près."""
        body = {
            "query": {"match_phrase": {field: {"query": phrase, "slop": slop}}},
            "highlight": self._highlight(),
            "size": size,
            "_source": SOURCE_FIELDS,
        }
        return self.es.search(index=self.index, body=body)

    def more_like_this(self, arxiv_id: str, size: int = 10, fields: list[str] | None = None) -> dict:
        """Les `size` articles les plus proches d'un article du corpus (par son ID ArXiv)."""
        body = {
            "query": {
                "more_like_this": {
                    "fields": fields or ["title", "abstract"],
                    "like": [{"_index": self.index, "_id": arxiv_id}],
                    "min_term_freq": 1,
                    "max_query_terms": 25,
                    "min_doc_freq": 2,
                }
            },
            "size": size,
            "_source": SOURCE_FIELDS,
        }
        return self.es.search(index=self.index, body=body)

    def more_like_text(self, text: str, size: int = 10) -> dict:
        """Articles similaires à un texte libre (ex. : l'abstract de votre propre article)."""
        body = {
            "query": {
                "more_like_this": {
                    "fields": ["abstract"],
                    "like": [text],
                    "min_term_freq": 1,
                    "max_query_terms": 15,
                    "min_doc_freq": 2,
                }
            },
            "size": size,
            "_source": SOURCE_FIELDS,
        }
        return self.es.search(index=self.index, body=body)

    def significant_terms(
        self,
        category: str | None = None,
        date_from: str | None = None,
        field: str = "abstract",
        size: int = 20,
        sample_size: int = 5000,
        min_doc_count: int = 10,
    ) -> dict:
        """
        Termes qui distinguent un sous-ensemble (catégorie et/ou période) du reste du corpus.

        - champ `text` (abstract, title) → agrégation `significant_text` : elle ré-analyse
          le texte à la volée et ne nécessite pas d'activer fielddata (significant_terms
          sur un champ text échouerait avec « Fielddata is disabled »).
        - champ `keyword` (keywords) → agrégation `significant_terms`.
        """
        filters = []
        if category:
            filters.append({"term": {"primary_category": category}})
        if date_from:
            filters.append({"range": {DATE_FIELD: {"gte": date_from}}})

        if field in TEXT_FIELDS:
            inner = {"significant_text": {"field": field, "size": size, "min_doc_count": min_doc_count,
                                          "filter_duplicate_text": True}}
        else:
            inner = {"significant_terms": {"field": field, "size": size, "min_doc_count": min_doc_count}}

        body = {
            "size": 0,
            "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
            "aggs": {
                "echantillon": {
                    "sampler": {"shard_size": sample_size},
                    "aggs": {"termes": inner},
                }
            },
        }
        return self.es.search(index=self.index, body=body)

    def suggest(self, text: str, size: int = 3) -> dict:
        """Correction de saisie (« convlutional nueral network » → « convolutional neural network »)."""
        body = {
            "suggest": {
                "title_suggest": {
                    "text": text,
                    "phrase": {
                        "field": "title.trigram",
                        "size": size,
                        "gram_size": 3,
                        "direct_generator": [{"field": "title.trigram", "suggest_mode": "always"}],
                        "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
                    },
                }
            },
            "size": 0,
        }
        return self.es.search(index=self.index, body=body)

    def autocomplete(self, prefix: str, size: int = 5) -> dict:
        """Autocomplétion sur les titres (champ completion title.suggest)."""
        body = {
            "suggest": {
                "title_autocomplete": {
                    "prefix": prefix,
                    "completion": {"field": "title.suggest", "size": size, "skip_duplicates": True},
                }
            },
            "_source": ["arxiv_id", "title"],
        }
        return self.es.search(index=self.index, body=body)

    def top_impact(self, category: str | None = None, size: int = 10) -> dict:
        """Articles au score d'impact le plus élevé (index enrichi)."""
        query = {"term": {"primary_category": category}} if category else {"match_all": {}}
        body = {"query": query, "sort": [{"impact_score": "desc"}], "size": size, "_source": SOURCE_FIELDS}
        return self.es.search(index=self.index, body=body)

    def combined_search(
        self,
        query: str,
        categories: list[str] | None = None,
        date_from: str | None = None,
        boost_recent: bool = True,
        size: int = 10,
    ) -> dict:
        """
        Requête combinée : recherche full-text + highlighting +
        agrégations (termes émergents + répartition par catégorie) +
        suggestion de correction.

        C'est la requête « tout-en-un » pour l'interface de recherche.
        """
        # Recherche textuelle principale
        must = [{
            "multi_match": {
                "query": query,
                "fields": ["title^3", "abstract", "keywords^2"],
                "type": "cross_fields",
            }
        }]

        # Filtres
        filters = self._filters(categories, date_from)

        bool_query = {"must": must}
        if filters:
            bool_query["filter"] = filters

        # Clause should : bonus si l'article a fait parler de lui sur Hacker News (index enrichi)
        if self.enriched:
            bool_query["should"] = [{"term": {"has_hn": True}}]

        # Construire la requête
        if boost_recent:
            query_part = {
                "function_score": {
                    "query": {"bool": bool_query},
                    "functions": [
                        {"gauss": {DATE_FIELD: {"origin": "now", "scale": "365d", "decay": 0.5}}}
                    ],
                    "boost_mode": "multiply",
                }
            }
        else:
            query_part = {"bool": bool_query}

        body = {
            "query": query_part,
            "highlight": self._highlight(),
            "aggs": {
                # significant_text sous un sampler : on n'analyse que les 200 meilleurs résultats/shard
                "termes_emergents": {
                    "sampler": {"shard_size": 200},
                    "aggs": {
                        "termes": {"significant_text": {"field": "abstract", "size": 10,
                                                        "filter_duplicate_text": True}}
                    },
                },
                "par_categorie": {"terms": {"field": "primary_category", "size": 5}},
            },
            "suggest": {
                "correction": {
                    "text": query,
                    "phrase": {
                        "field": "title.trigram",
                        "size": 2,
                        "direct_generator": [{"field": "title.trigram", "suggest_mode": "missing"}],
                    },
                }
            },
            "size": size,
            "_source": SOURCE_FIELDS,
        }

        return self.es.search(index=self.index, body=body)

    # ─────────────────────────────────────────────────────────────────────────
    #  Affichage
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _buckets(agg: dict) -> list[dict]:
        """Buckets d'une agrégation, qu'elle soit directe ou imbriquée sous un sampler."""
        if not agg:
            return []
        if "buckets" in agg:
            return agg["buckets"]
        for value in agg.values():
            if isinstance(value, dict) and "buckets" in value:
                return value["buckets"]
        return []

    @staticmethod
    def format_results(response: dict) -> str:
        """Formate une réponse ES pour l'affichage terminal."""
        lines = []
        hits = response.get("hits", {}).get("hits", [])
        total = response.get("hits", {}).get("total", {}).get("value", 0)

        lines.append(f"\n  {total:,} résultats trouvés\n")

        for i, hit in enumerate(hits[:10], 1):
            src = hit.get("_source", {})
            score = hit.get("_score") or 0
            title = (src.get("title") or "?")[:70]
            cat = src.get("primary_category", "?")
            date = (src.get(DATE_FIELD) or src.get("date_updated") or "?")[:10]
            hn = src.get("hn_score_total")

            lines.append(f"  {i:>2}. [{score:.2f}] {title}")
            impact = src.get("impact_score")
            lines.append(f"      {cat} | {date}" + (f" | HN:{hn}" if hn else "")
                         + (f" | impact:{impact:.1f}" if impact is not None else ""))

            # Highlight
            hl = hit.get("highlight", {})
            if "abstract" in hl:
                fragment = hl["abstract"][0][:120]
                lines.append(f"      ... {fragment}...")
            lines.append("")

        # Agrégations
        aggs = response.get("aggregations", {})
        buckets = SciPulseSearch._buckets(aggs.get("par_categorie", {}))
        if buckets:
            lines.append("  Catégories :")
            for b in buckets:
                lines.append(f"    {b['key']:<10} {b['doc_count']:>5}")

        buckets = SciPulseSearch._buckets(aggs.get("termes_emergents", {}))
        if buckets:
            lines.append("\n  Termes distinctifs :")
            for b in buckets[:8]:
                lines.append(f"    {b['key']:<25} (score: {b.get('score', 0):.4f})")

        # Suggestions
        suggest = response.get("suggest", {})
        if "correction" in suggest:
            for s in suggest["correction"]:
                if s.get("options"):
                    lines.append(f"\n  💡 Vouliez-vous dire : {s['options'][0]['text']} ?")
                    break

        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="SciPulse — recherche dans le corpus ArXiv")
    parser.add_argument("query", nargs="?", help="Texte recherché (combined_search)")
    parser.add_argument("--categories", nargs="*")
    parser.add_argument("--since", help="Date minimale AAAA-MM-JJ")
    parser.add_argument("--mlt", metavar="ARXIV_ID", help="Articles similaires à cet article")
    parser.add_argument("--suggest", metavar="TEXTE", help="Correction de saisie")
    parser.add_argument("--trends", action="store_true", help="Termes émergents (avec --since / --categories)")
    args = parser.parse_args()

    search = SciPulseSearch()

    if args.mlt:
        r = search.more_like_this(args.mlt)
        print(f"\n🔁 Articles similaires à {args.mlt} :")
        print(search.format_results(r))
    elif args.suggest:
        r = search.suggest(args.suggest)
        options = [o["text"] for e in r.get("suggest", {}).get("title_suggest", []) for o in e.get("options", [])]
        print(f"\n💡 « {args.suggest} » → {options or 'aucune suggestion'}")
    elif args.trends:
        category = args.categories[0] if args.categories else None
        r = search.significant_terms(category=category, date_from=args.since)
        print("\n📈 Termes distinctifs :")
        for b in SciPulseSearch._buckets(r["aggregations"]["echantillon"]):
            print(f"   {b['key']:<25} score={b['score']:.4f}  ({b['doc_count']} docs)")
    elif args.query:
        r = search.combined_search(args.query, categories=args.categories, date_from=args.since)
        print(search.format_results(r))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
