"""
SciPulse — Schémas Parquet explicites de la couche Silver

Pourquoi des schémas explicites ?
  - une colonne entièrement vide dans un fichier (ex. journal_ref) serait sinon
    écrite avec le type Arrow « null », que Spark ne sait pas lire ;
  - les types restent identiques d'un fichier à l'autre (lecture Spark d'un dossier) ;
  - les timestamps sont en microsecondes UTC (Spark 3.5 refuse les nanosecondes).
Le schéma Silver devient un contrat versionné, au même titre que les suites GX.
"""

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")

ARXIV_SCHEMA = pa.schema([
    ("arxiv_id", pa.string()),
    ("title", pa.string()),
    ("abstract", pa.string()),
    ("abstract_length", pa.int32()),
    ("abstract_too_short", pa.bool_()),
    ("authors", pa.list_(pa.struct([("name", pa.string()), ("affiliation", pa.string())]))),
    ("authors_flat", pa.string()),
    ("n_authors", pa.int32()),
    ("submitter", pa.string()),
    ("categories", pa.list_(pa.string())),
    ("primary_category", pa.string()),
    ("domain", pa.string()),
    ("n_categories", pa.int32()),
    ("date_published", TS),
    ("date_updated", TS),
    ("year_published", pa.int32()),
    ("doi", pa.string()),
    ("arxiv_doi", pa.string()),
    ("journal_ref", pa.string()),
    ("versions_count", pa.int32()),
    ("license", pa.string()),
])

HN_SCHEMA = pa.schema([
    ("hn_id", pa.string()),
    ("type", pa.string()),
    ("post_type", pa.string()),
    ("by", pa.string()),
    ("time", pa.int64()),
    ("published_at", TS),
    ("title", pa.string()),
    ("url", pa.string()),
    ("domain", pa.string()),
    ("text", pa.string()),
    ("score", pa.int64()),
    ("descendants", pa.int64()),
    ("parent", pa.string()),
    ("story_id", pa.string()),
    ("kids_count", pa.int64()),
    ("arxiv_id_linked", pa.string()),
    ("arxiv_link_source", pa.string()),
    ("feed", pa.string()),
    ("fetched_at", TS),
])

CITATIONS_SCHEMA = pa.schema([
    ("openalex_id", pa.string()),
    ("doi", pa.string()),
    ("arxiv_id", pa.string()),
    ("match_key_type", pa.string()),          # arxiv_doi | journal_doi
    ("title", pa.string()),
    ("work_type", pa.string()),
    ("publication_year", pa.int32()),
    ("publication_date", pa.string()),
    ("cited_by_count", pa.int64()),
    ("citations_by_year", pa.list_(pa.struct([("year", pa.int32()), ("cited_by_count", pa.int64())]))),
    ("referenced_works_count", pa.int64()),
    ("field", pa.string()),
    ("subfield", pa.string()),
    ("oa_domain", pa.string()),
    ("fetched_at", TS),
])
