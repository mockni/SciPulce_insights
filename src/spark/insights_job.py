"""
SciPulse Insights — 🥇 Job Spark : Silver × 3 → couche Gold `arxiv-papers-enriched`

Étapes :
  1. Lecture des trois couches Silver (Parquet MinIO via s3a://)
  2. Agrégation du signal Hacker News par article ArXiv
  3. Rapprochement des citations OpenAlex (DOI ArXiv, puis DOI d'éditeur)
  4. Jointure à trois sources + taux de correspondance (match coverage)
  5. Indicateurs temporels par catégorie (window functions : croissance annuelle, vélocité)
  6. TF-IDF distribué (Spark MLlib) sur les abstracts → mots-clés par article
  7. Score d'impact composite + segments analytiques
  8. Écriture dans Elasticsearch via le connecteur elasticsearch-hadoop (seule écriture Gold)

Formule du score (justifiée dans docs/02_pipeline_spark.md) :
    impact = 100 × (0,5 × S_citations + 0,3 × S_hn + 0,2 × S_récence)
    S_citations = log(1 + citations) / log(1 + max des citations de la cohorte catégorie × année)
    S_hn        = log(1 + points HN) / log(1 + max des points HN du corpus)
    S_récence   = exp(−ln 2 × âge en années / 2)      (demi-vie de 2 ans)

Usage (dans le conteneur Airflow, cluster Spark standalone) :
    spark-submit --master spark://spark-master:7077 src/spark/insights_job.py
Local / tests :
    python -m src.spark.insights_job --lake-uri file:///chemin/lake/ --output-parquet /tmp/gold
"""

import argparse
import logging
import math
import os
import sys
import time
from datetime import date, datetime, timezone

# Permet `spark-submit src/spark/insights_job.py` depuis la racine du projet
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from src import config  # noqa: E402

log = logging.getLogger("scipulse.spark")

# ── Paramètres du score (constantes documentées, modifiables en un seul endroit) ──
W_CITATIONS, W_HN, W_RECENCY = 0.5, 0.3, 0.2
RECENCY_HALF_LIFE_YEARS = 2.0
SEGMENT_HIGH_CITATION_PCT = 0.90     # « très cité » : top 10 % de sa cohorte
SEGMENT_MIN_CITATIONS = 10
SEGMENT_VIRAL_HN_POINTS = 100        # « viral » : ≥ 100 points cumulés sur HN
SEGMENT_LOW_CITATION_PCT = 0.50      # « peu cité » : moitié basse de sa cohorte (ou aucune donnée)
TFIDF_TOP_K = 8
TFIDF_VOCAB_SIZE = 20_000
SCIENTIFIC_STOPWORDS = [
    "paper", "propose", "proposed", "approach", "method", "methods", "results", "result", "show",
    "shows", "based", "using", "used", "use", "new", "novel", "present", "study", "work", "problem",
    "problems", "existing", "experiments", "experimental", "demonstrate", "performance", "state",
    "art", "furthermore", "however", "also", "different", "various", "provide", "proposes",
    "presents", "introduce", "introduces", "can", "well", "two", "one", "first", "data", "model", "models",
]


# ═════════════════════════════════════════════════════════════════════════════
#  SESSION
# ═════════════════════════════════════════════════════════════════════════════

def build_spark(app_name: str = "scipulse-insights", master: str | None = None) -> SparkSession:
    builder = (SparkSession.builder.appName(app_name)
               .master(master or config.SPARK_MASTER)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
               .config("spark.ui.showConsoleProgress", "false"))
    if config.LAKE_URI.startswith("s3a://"):
        builder = (builder
                   .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
                   .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
                   .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                           str(config.MINIO_ENDPOINT.startswith("https")).lower())
                   .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if os.getenv("SPARK_EXTRA_JARS"):        # jars embarqués dans l'image (hadoop-aws, es-hadoop)
        builder = builder.config("spark.jars", os.environ["SPARK_EXTRA_JARS"])
    elif os.getenv("SPARK_PACKAGES"):        # alternative : téléchargement Maven au démarrage
        builder = builder.config("spark.jars.packages", os.environ["SPARK_PACKAGES"])
    return builder.getOrCreate()


# Schémas minimaux des sources secondaires : si elles sont encore vides (premier lancement,
# poller HN pas encore exécuté), le job continue avec un DataFrame vide au lieu d'échouer.
HN_EMPTY_SCHEMA = ("hn_id string, type string, score long, descendants long, "
                   "published_at timestamp, arxiv_id_linked string")
CITATIONS_EMPTY_SCHEMA = ("openalex_id string, doi string, arxiv_id string, match_key_type string, "
                          "cited_by_count long, citations_by_year array<struct<year:int,cited_by_count:bigint>>, "
                          "field string, subfield string")


def _read_optional(spark: SparkSession, path: str, empty_schema: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except Exception as e:   # AnalysisException : chemin absent
        if "PATH_NOT_FOUND" not in str(e) and "Path does not exist" not in str(e):
            raise
        log.warning(f"⚠️  {path} absent : source traitée comme vide")
        return spark.createDataFrame([], empty_schema)


def read_silver(spark: SparkSession, lake_uri: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    # ArXiv est obligatoire : son absence doit faire échouer le job
    arxiv = spark.read.parquet(f"{lake_uri}{config.BUCKET_ARXIV_CLEAN}/{config.SILVER_ARXIV_PREFIX}")
    hn = _read_optional(spark, f"{lake_uri}{config.BUCKET_HN_CLEAN}/{config.SILVER_HN_PREFIX}", HN_EMPTY_SCHEMA)
    citations = _read_optional(spark, f"{lake_uri}{config.BUCKET_CITATIONS_CLEAN}/{config.SILVER_CITATIONS_PREFIX}",
                               CITATIONS_EMPTY_SCHEMA)
    return arxiv, hn, citations


# ═════════════════════════════════════════════════════════════════════════════
#  1. SIGNAL HACKER NEWS
# ═════════════════════════════════════════════════════════════════════════════

def aggregate_hn(hn: DataFrame) -> DataFrame:
    """Une ligne par article ArXiv cité sur HN (stories uniquement : elles portent le score)."""
    return (hn.filter(F.col("arxiv_id_linked").isNotNull() & (F.col("type") == "story"))
              .groupBy(F.col("arxiv_id_linked").alias("arxiv_id"))
              .agg(F.count("*").cast("int").alias("hn_posts"),
                   F.sum(F.coalesce("score", F.lit(0))).cast("int").alias("hn_score_total"),
                   F.max("score").cast("int").alias("hn_score_max"),
                   F.sum(F.coalesce("descendants", F.lit(0))).cast("int").alias("hn_comments_total"),
                   F.min("published_at").alias("hn_first_seen")))


# ═════════════════════════════════════════════════════════════════════════════
#  2. CITATIONS
# ═════════════════════════════════════════════════════════════════════════════

def match_citations(arxiv: DataFrame, citations: DataFrame, as_of_year: int) -> DataFrame:
    """
    Rattache chaque œuvre OpenAlex à un article ArXiv :
      - par le DOI ArXiv (10.48550/arxiv.<id>) : arxiv_id déjà déduit au Silver ;
      - par le DOI d'éditeur : jointure avec arxiv.doi.
    Si plusieurs œuvres correspondent (prépublication + version publiée), on garde la plus citée.
    """
    cols = ["openalex_id", "cited_by_count", "citations_by_year", "field", "subfield", "match_key_type"]
    via_arxiv = citations.filter(F.col("match_key_type") == "arxiv_doi").select("arxiv_id", *cols)
    journal = citations.filter(F.col("match_key_type") == "journal_doi").drop("arxiv_id")
    via_journal = (journal.join(arxiv.select("arxiv_id", "doi").filter(F.col("doi").isNotNull()), on="doi")
                          .select("arxiv_id", *cols))
    matched = via_arxiv.unionByName(via_journal)

    best = Window.partitionBy("arxiv_id").orderBy(F.col("cited_by_count").desc_nulls_last(), "openalex_id")
    # Citations des deux dernières années civiles (fonctions d'ordre supérieur : pas d'UDF Python)
    recent = F.aggregate(
        F.filter("citations_by_year", lambda c: c["year"] >= F.lit(as_of_year - 1)),
        F.lit(0).cast("long"), lambda acc, c: acc + c["cited_by_count"])
    return (matched.withColumn("rn", F.row_number().over(best)).filter("rn = 1").drop("rn")
                   .withColumn("citations_last_2y", F.coalesce(recent, F.lit(0)).cast("int"))
                   .withColumn("cited_by_count", F.col("cited_by_count").cast("int"))
                   .withColumnRenamed("field", "openalex_field")
                   .withColumnRenamed("subfield", "openalex_subfield")
                   .withColumnRenamed("match_key_type", "citation_match_key")
                   .drop("citations_by_year"))


# ═════════════════════════════════════════════════════════════════════════════
#  3. JOINTURE À TROIS SOURCES
# ═════════════════════════════════════════════════════════════════════════════

def join_sources(arxiv: DataFrame, hn_agg: DataFrame, cit: DataFrame, broadcast: bool = True) -> DataFrame:
    """
    ArXiv (grande table) ⟕ HN agrégé (petite table) ⟕ citations (≤ 1 ligne par article).
    - HN agrégé : quelques centaines à milliers de lignes → broadcast join (aucun shuffle de l'ArXiv).
    - Citations : même ordre de grandeur que l'ArXiv → sort-merge join par défaut ;
      broadcast possible tant qu'elles tiennent en mémoire (voir explain_strategies).
    """
    hn_side = F.broadcast(hn_agg) if broadcast else hn_agg
    return arxiv.join(hn_side, on="arxiv_id", how="left").join(cit, on="arxiv_id", how="left")


def explain_strategies(arxiv: DataFrame, cit: DataFrame) -> dict[str, str]:
    """Plans physiques de la jointure ArXiv × citations : sort-merge vs broadcast."""
    spark = arxiv.sparkSession
    previous = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    plans = {}
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")      # force le sort-merge
        plans["sort_merge"] = arxiv.join(cit, "arxiv_id", "left")._jdf.queryExecution().executedPlan().toString()
        plans["broadcast"] = arxiv.join(F.broadcast(cit), "arxiv_id", "left")._jdf.queryExecution().executedPlan().toString()
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous)
    return plans


def coverage(arxiv: DataFrame, hn_agg: DataFrame, citations: DataFrame, joined: DataFrame) -> dict:
    """Taux de correspondance entre les sources."""
    stats = joined.agg(
        F.count("*").alias("articles"),
        F.sum(F.col("openalex_id").isNotNull().cast("int")).alias("with_citations"),
        F.sum(F.col("hn_posts").isNotNull().cast("int")).alias("with_hn"),
        F.sum((F.col("openalex_id").isNotNull() & F.col("hn_posts").isNotNull()).cast("int")).alias("with_both"),
    ).first().asDict()
    linked_ids = hn_agg.count()
    linked_in_corpus = hn_agg.join(arxiv.select("arxiv_id"), "arxiv_id").count()
    works = citations.count()
    articles = stats["articles"] or 1
    stats.update({
        "pct_with_citations": round(stats["with_citations"] / articles, 4),
        "pct_with_hn": round(stats["with_hn"] / articles, 4),
        "pct_with_both": round(stats["with_both"] / articles, 4),
        "hn_linked_articles": linked_ids,
        "pct_hn_links_in_corpus": round(linked_in_corpus / linked_ids, 4) if linked_ids else 0.0,
        "openalex_works": works,
    })
    return stats


# ═════════════════════════════════════════════════════════════════════════════
#  4. INDICATEURS TEMPORELS (WINDOW FUNCTIONS)
# ═════════════════════════════════════════════════════════════════════════════

def add_velocity(df: DataFrame, as_of: date) -> DataFrame:
    """Vélocité de citation = citations / années écoulées depuis la publication (≥ 1 an)."""
    age_years = F.datediff(F.lit(as_of), F.to_date("date_published")) / F.lit(365.25)
    return (df.withColumn("age_years", F.greatest(age_years, F.lit(0.0)))
              .withColumn("citation_velocity",
                          F.round(F.col("cited_by_count") / F.greatest(F.col("age_years"), F.lit(1.0)), 3)))


def category_indicators(df: DataFrame) -> DataFrame:
    """Par catégorie × année : volume, croissance annuelle (lag) et vélocité moyenne."""
    per_year = (df.groupBy("primary_category", "year_published")
                  .agg(F.count("*").cast("int").alias("category_publications_year"),
                       F.round(F.avg("citation_velocity"), 3).alias("category_avg_velocity")))
    w = Window.partitionBy("primary_category").orderBy("year_published")
    previous = F.lag("category_publications_year").over(w)
    previous_year = F.lag("year_published").over(w)
    # Croissance calculée seulement si l'année précédente existe (pas de « saut » 2001 → 2017)
    consecutive = previous_year == F.col("year_published") - 1
    return per_year.withColumn(
        "category_yoy_growth",
        F.when(previous.isNull() | (previous == 0) | ~consecutive, F.lit(None).cast("double"))
         .otherwise(F.round((F.col("category_publications_year") - previous) / previous, 4)))


# ═════════════════════════════════════════════════════════════════════════════
#  5. TF-IDF DISTRIBUÉ (SPARK MLlib)
# ═════════════════════════════════════════════════════════════════════════════

def tfidf_keywords(arxiv: DataFrame, top_k: int = TFIDF_TOP_K, vocab_size: int = TFIDF_VOCAB_SIZE,
                   min_df: float | None = None) -> DataFrame:
    """
    Pipeline MLlib : RegexTokenizer → StopWordsRemover → CountVectorizer → IDF,
    puis les top_k termes de chaque vecteur TF-IDF (UDF sur vecteur creux : seuls les
    termes présents sont parcourus).
    """
    docs = arxiv.select("arxiv_id", F.concat_ws(". ", "title", "abstract").alias("text"))
    if min_df is None:
        n_docs = docs.count()
        min_df = float(min(5, max(1, n_docs // 100)))
    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="text", outputCol="tokens", pattern="[^a-zA-Z]+", minTokenLength=3,
                       toLowercase=True),
        StopWordsRemover(inputCol="tokens", outputCol="words",
                         stopWords=StopWordsRemover.loadDefaultStopWords("english") + SCIENTIFIC_STOPWORDS),
        CountVectorizer(inputCol="words", outputCol="tf", vocabSize=vocab_size, minDF=min_df),
        IDF(inputCol="tf", outputCol="tfidf"),
    ])
    model = pipeline.fit(docs)
    vocabulary = model.stages[2].vocabulary

    def top_terms(vector):
        if vector is None or len(vector.indices) == 0:
            return []
        ranked = sorted(zip(vector.values, vector.indices), key=lambda p: (-p[0], p[1]))[:top_k]
        return [vocabulary[int(i)] for _, i in ranked]

    top_udf = F.udf(top_terms, T.ArrayType(T.StringType()))
    return model.transform(docs).select("arxiv_id", top_udf("tfidf").alias("keywords"))


# ═════════════════════════════════════════════════════════════════════════════
#  6. SCORE D'IMPACT COMPOSITE
# ═════════════════════════════════════════════════════════════════════════════

def impact_score(df: DataFrame) -> DataFrame:
    # Cohorte = même catégorie primaire, même année : un article de 2024 n'est pas comparé à un de 2015
    cohort = Window.partitionBy("primary_category", "year_published")
    # Percentile calculé uniquement parmi les articles AYANT des données de citation
    cohort_sorted = (Window.partitionBy("primary_category", "year_published", "has_citations_data")
                           .orderBy(F.col("cited_by_count").asc_nulls_first()))
    max_cohort = F.max(F.log1p(F.coalesce("cited_by_count", F.lit(0)))).over(cohort)
    # Maximum HN du corpus : un scalaire (évite une fenêtre globale sur une seule partition)
    max_hn_value = df.agg(F.max(F.log1p(F.coalesce("hn_score_total", F.lit(0))))).first()[0] or 0.0
    max_hn = F.lit(float(max_hn_value))

    scored = (df
        .withColumn("has_hn", F.col("hn_posts").isNotNull())
        .withColumn("has_citations_data", F.col("openalex_id").isNotNull())
        .withColumn("citation_percentile",
                    F.when(F.col("has_citations_data"), F.round(F.percent_rank().over(cohort_sorted), 4)))
        .withColumn("score_citations",
                    F.when(max_cohort > 0, F.log1p(F.coalesce("cited_by_count", F.lit(0))) / max_cohort)
                     .otherwise(F.lit(0.0)))
        .withColumn("score_hn",
                    F.when(max_hn > 0, F.log1p(F.coalesce("hn_score_total", F.lit(0))) / max_hn)
                     .otherwise(F.lit(0.0)))
        .withColumn("score_recency",
                    F.exp(F.lit(-math.log(2) / RECENCY_HALF_LIFE_YEARS) * F.col("age_years")))
        .withColumn("impact_score", F.round(F.lit(100.0) * (
            F.lit(W_CITATIONS) * F.col("score_citations")
            + F.lit(W_HN) * F.col("score_hn")
            + F.lit(W_RECENCY) * F.col("score_recency")), 2)))
    for col in ("score_citations", "score_hn", "score_recency"):
        scored = scored.withColumn(col, F.round(col, 4))

    rank_w = Window.partitionBy("primary_category").orderBy(F.col("impact_score").desc(), "arxiv_id")
    pct = F.coalesce("citation_percentile", F.lit(0.0))
    highly_cited = (pct >= SEGMENT_HIGH_CITATION_PCT) & (F.coalesce("cited_by_count", F.lit(0)) >= SEGMENT_MIN_CITATIONS)
    viral = F.coalesce("hn_score_total", F.lit(0)) >= SEGMENT_VIRAL_HN_POINTS
    return (scored
        .withColumn("impact_rank_in_category", F.row_number().over(rank_w))
        .withColumn("segment",
                    F.when(highly_cited & viral, "academic_and_viral")
                     .when(highly_cited & ~F.col("has_hn"), "academic_only")
                     .when(viral & (pct <= SEGMENT_LOW_CITATION_PCT), "viral_low_citations")
                     .otherwise("other")))


# ═════════════════════════════════════════════════════════════════════════════
#  ASSEMBLAGE
# ═════════════════════════════════════════════════════════════════════════════

GOLD_COLUMNS = [
    "arxiv_id", "title", "abstract", "author_names", "authors_flat", "n_authors", "submitter",
    "categories", "primary_category", "domain", "n_categories", "date_published", "date_updated",
    "year_published", "doi", "arxiv_doi", "journal_ref", "versions_count", "abstract_length", "license",
    "keywords",
    "hn_posts", "hn_score_total", "hn_score_max", "hn_comments_total", "hn_first_seen", "has_hn",
    "openalex_id", "cited_by_count", "citations_last_2y", "citation_velocity", "citation_percentile",
    "openalex_field", "openalex_subfield", "has_citations_data", "citation_match_key",
    "category_publications_year", "category_yoy_growth", "category_avg_velocity",
    "score_citations", "score_hn", "score_recency", "impact_score", "impact_rank_in_category", "segment",
    "enriched_at",
]


def build_gold(arxiv: DataFrame, hn: DataFrame, citations: DataFrame, as_of: date,
               tfidf_min_df: float | None = None) -> tuple[DataFrame, dict]:
    hn_agg = aggregate_hn(hn)
    cit = match_citations(arxiv, citations, as_of.year)
    joined = join_sources(arxiv, hn_agg, cit)
    stats = coverage(arxiv, hn_agg, citations, joined)

    enriched = add_velocity(joined, as_of)
    indicators = category_indicators(enriched)
    enriched = enriched.join(F.broadcast(indicators), ["primary_category", "year_published"], "left")
    enriched = enriched.join(tfidf_keywords(arxiv, min_df=tfidf_min_df), "arxiv_id", "left")
    enriched = impact_score(enriched)

    gold = (enriched
            .withColumn("author_names", F.transform("authors", lambda a: a["name"]))
            .withColumn("enriched_at", F.current_timestamp())
            .select(*GOLD_COLUMNS))
    return gold, stats


def write_elasticsearch(gold: DataFrame, index: str = config.INDEX_ENRICHED, recreate: bool = False):
    """Écriture Gold via elasticsearch-hadoop (org.elasticsearch.spark.sql)."""
    from elasticsearch import Elasticsearch

    from src.utils.mappings import ENRICHED_SETTINGS, create_index

    es_url = config.ES_HOST.rstrip("/")
    create_index(Elasticsearch(es_url), index, ENRICHED_SETTINGS, recreate=recreate)
    host, _, port = es_url.split("://", 1)[-1].partition(":")
    (gold.write.format("org.elasticsearch.spark.sql")
         .option("es.nodes", host)
         .option("es.port", port or "9200")
         .option("es.nodes.wan.only", "true")           # conteneurs Docker : pas de découverte des nœuds
         .option("es.mapping.id", "arxiv_id")           # idempotence : un document par article
         .option("es.write.operation", "index")
         .option("es.batch.size.entries", "1000")
         .option("es.index.auto.create", "false")       # l'index est créé avec son mapping explicite
         .mode("append")
         .save(index))


def main():
    parser = argparse.ArgumentParser(description="SciPulse Insights — job Spark Gold")
    parser.add_argument("--lake-uri", default=config.LAKE_URI, help="s3a:// (MinIO) ou file:///…")
    parser.add_argument("--master", default=None)
    parser.add_argument("--as-of", default=None, help="Date de référence AAAA-MM-JJ (défaut : aujourd'hui)")
    parser.add_argument("--output-parquet", default=None, help="Écrire la Gold en Parquet au lieu d'ES (tests)")
    parser.add_argument("--recreate-index", action="store_true")
    parser.add_argument("--explain", action="store_true", help="Afficher les plans sort-merge / broadcast")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    started = time.time()
    as_of = date.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc).date()
    spark = build_spark(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

    arxiv, hn, citations = read_silver(spark, args.lake_uri)
    arxiv = arxiv.cache()
    gold, stats = build_gold(arxiv, hn, citations, as_of)

    log.info("📊 Taux de correspondance des sources :")
    for k, v in stats.items():
        log.info(f"     {k:<26} {v}")
    if args.explain:
        cit = match_citations(arxiv, citations, as_of.year)
        for name, plan in explain_strategies(arxiv, cit).items():
            log.info(f"\n===== Plan physique — {name} =====\n{plan}")

    gold = gold.cache()
    rows = gold.count()
    if args.output_parquet:
        gold.write.mode("overwrite").parquet(args.output_parquet)
        log.info(f"🏁 {rows:,} articles Gold écrits en Parquet : {args.output_parquet}")
    else:
        write_elasticsearch(gold, recreate=args.recreate_index)
        log.info(f"🏁 {rows:,} articles Gold écrits dans '{config.INDEX_ENRICHED}'")

    from src.monitoring.events import emit, emit_layer_write

    emit("join_coverage", layer="gold", source="enriched", **stats)
    emit_layer_write("gold", "enriched", rows=rows, objects=1, started=started, index=config.INDEX_ENRICHED)
    spark.stop()


if __name__ == "__main__":
    main()
