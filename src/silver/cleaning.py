"""
SciPulse — 🥈 Règles de nettoyage Silver (fonctions pures, testées unitairement)

Les règles sont guidées par l'analyse de qualité du Bronze (notebooks/01_analyse_qualite.ipynb)
et par les échecs des suites Great Expectations « bronze » :

ArXiv
  1. Lignes JSON illisibles                → rejetées (comptées)
  2. Hors périmètre (ni cs.*, ni stat.ML)  → rejetées (comptées)
  3. Identifiant manquant                  → rejeté
  4. Doublons d'identifiant                → on garde la version la plus récemment mise à jour
  5. Titre ou abstract vide                → rejeté (inexploitable pour la recherche plein texte)
  6. Abstract < 50 caractères              → conservé mais signalé (abstract_too_short)
  7. Espaces / retours à la ligne          → normalisés (titre, abstract, auteurs)
  8. Catégories                            → tableau + catégorie primaire + domaine (« encodage »)
  9. Dates                                 → timestamps UTC ; date de publication = version v1
 10. DOI                                   → minuscules, sans préfixe https://doi.org/ (clé de jointure)
 11. Auteurs                               → liste structurée {name, affiliation} + version plate

Hacker News
  1. Items sans identifiant, supprimés (deleted) ou masqués (dead) → rejetés
  2. Doublons (un item relu à plusieurs cycles) → on garde la mesure la plus récente
  3. Horodatage epoch → timestamp UTC ; texte HTML → texte brut
  4. Score et commentaires : uniquement pour les stories / jobs / polls (null pour un commentaire)
  5. Liens ArXiv détectés dans l'URL ou le texte → arxiv_id_linked (identifiant sans version)
"""

import json
import re
from collections import Counter

import pandas as pd

from src.utils.arxiv_utils import (
    arxiv_doi,
    clean_text,
    domain_of,
    extract_arxiv_ids,
    first_version_date,
    html_to_text,
    matches_scope,
    normalize_arxiv_id,
    normalize_doi,
    parse_authors,
    parse_update_date,
    split_categories,
)

SHORT_ABSTRACT = 50

ARXIV_SILVER_COLUMNS = [
    "arxiv_id", "title", "abstract", "abstract_length", "abstract_too_short",
    "authors", "authors_flat", "n_authors", "submitter",
    "categories", "primary_category", "domain", "n_categories",
    "date_published", "date_updated", "year_published",
    "doi", "arxiv_doi", "journal_ref", "versions_count", "license",
]

HN_SILVER_COLUMNS = [
    "hn_id", "type", "post_type", "by", "time", "published_at", "title", "url", "domain",
    "text", "score", "descendants", "parent", "story_id", "kids_count",
    "arxiv_id_linked", "arxiv_link_source", "feed", "fetched_at",
]

HN_TYPES = ["story", "comment", "job", "poll", "pollopt"]


# ═════════════════════════════════════════════════════════════════════════════
#  ARXIV
# ═════════════════════════════════════════════════════════════════════════════

def parse_jsonl(lines, report: Counter) -> list[dict]:
    records = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            report["json_errors"] += 1
            continue
        if isinstance(record, dict):
            records.append(record)
        else:
            report["json_errors"] += 1
    return records


def _to_utc(value):
    return pd.to_datetime(value, utc=True, errors="coerce") if value else pd.NaT


def clean_arxiv_record(raw: dict) -> dict:
    """Un article brut du dump → une ligne Silver (sans décision de rejet)."""
    categories = split_categories(raw.get("categories"))
    primary = categories[0] if categories else None
    title = clean_text(raw.get("title")) or None
    abstract = clean_text(raw.get("abstract")) or None
    authors, authors_flat = parse_authors(raw.get("authors"), raw.get("authors_parsed"))
    date_updated = _to_utc(parse_update_date(raw.get("update_date")))
    date_published = _to_utc(first_version_date(raw.get("versions")))
    if pd.isna(date_published):
        date_published = date_updated
    versions = raw.get("versions")
    arxiv_id = normalize_arxiv_id(raw.get("id"))
    return {
        "arxiv_id": arxiv_id,
        "title": title,
        "abstract": abstract,
        "abstract_length": len(abstract) if abstract else 0,
        "abstract_too_short": bool(abstract) and len(abstract) < SHORT_ABSTRACT,
        "authors": authors,
        "authors_flat": authors_flat or None,
        "n_authors": len(authors),
        "submitter": clean_text(raw.get("submitter")) or None,
        "categories": categories,
        "primary_category": primary,
        "domain": domain_of(primary),
        "n_categories": len(categories),
        "date_published": date_published,
        "date_updated": date_updated,
        "year_published": None if pd.isna(date_published) else int(date_published.year),
        "doi": normalize_doi(raw.get("doi")),
        "arxiv_doi": arxiv_doi(arxiv_id),
        "journal_ref": clean_text(raw.get("journal-ref")) or None,
        "versions_count": len(versions) if isinstance(versions, list) else 0,
        "license": raw.get("license"),
    }


def clean_arxiv(records: list[dict], scope_prefixes=None, report: Counter | None = None) -> pd.DataFrame:
    """
    Applique les règles ArXiv à une liste d'articles bruts.
    Retourne le DataFrame Silver ; `report` reçoit les compteurs de chaque règle.
    """
    report = report if report is not None else Counter()
    rows = []
    for raw in records:
        report["input"] += 1
        if scope_prefixes and not matches_scope(raw.get("categories"), scope_prefixes):
            report["out_of_scope"] += 1
            continue
        if not raw.get("id"):
            report["missing_id"] += 1
            continue
        if raw.get("doi") and not normalize_doi(raw.get("doi")):
            report["invalid_doi"] += 1
        if raw.get("update_date") and not parse_update_date(raw.get("update_date")):
            report["invalid_dates"] += 1
        rows.append(clean_arxiv_record(raw))

    df = pd.DataFrame(rows, columns=ARXIV_SILVER_COLUMNS)
    if df.empty:
        return _typed_arxiv(df)

    # Doublons : on garde la version la plus récemment mise à jour
    before = len(df)
    df = (df.sort_values("date_updated", na_position="first")
            .drop_duplicates(subset="arxiv_id", keep="last"))
    report["duplicates"] += before - len(df)

    missing_title = df["title"].isna()
    missing_abstract = df["abstract"].isna()
    report["missing_title"] += int(missing_title.sum())
    report["missing_abstract"] += int((missing_abstract & ~missing_title).sum())
    df = df[~missing_title & ~missing_abstract]

    report["short_abstracts_flagged"] += int(df["abstract_too_short"].sum())
    report["output"] += len(df)
    return _typed_arxiv(df.sort_values("arxiv_id").reset_index(drop=True))


def _typed_arxiv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("date_published", "date_updated"):
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("abstract_length", "n_authors", "n_categories", "versions_count"):
        df[col] = df[col].astype("int32")
    df["year_published"] = df["year_published"].astype("Int32")
    df["abstract_too_short"] = df["abstract_too_short"].astype(bool)
    return df


def arxiv_bronze_frame(records: list[dict]) -> pd.DataFrame:
    """Vue tabulaire du Bronze (champs bruts, sans nettoyage) pour l'analyse et GX."""
    columns = ["id", "title", "abstract", "authors", "categories", "update_date", "doi",
               "submitter", "journal-ref", "license", "versions", "authors_parsed"]
    return pd.DataFrame([{c: r.get(c) for c in columns} for r in records], columns=columns)


def diagnose_arxiv_bronze(df: pd.DataFrame, scope_prefixes=None) -> dict:
    """Indicateurs de qualité du Bronze ArXiv (analyse du notebook + rapport du job Silver)."""
    report = {"rows": len(df)}
    for col in ("id", "title", "abstract", "categories", "update_date", "doi", "authors"):
        report[f"null_{col}"] = int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum())
    report["duplicate_ids"] = int(df["id"].duplicated().sum())
    report["bad_date_format"] = int((~df["update_date"].astype(str).str.fullmatch(r"\d{4}-\d{2}-\d{2}")).sum())
    abstracts = df["abstract"].dropna().astype(str).str.strip()
    report["short_abstracts"] = int((abstracts.str.len() < SHORT_ABSTRACT).sum())
    report["multiline_titles"] = int(df["title"].dropna().astype(str).str.contains(r"\n|\s{2,}").sum())
    report["non_string_categories"] = int(df["categories"].map(lambda v: v is not None and not isinstance(v, str)).sum())
    report["versions_not_list"] = int(df["versions"].map(lambda v: not isinstance(v, list)).sum())
    report["invalid_doi"] = int(df["doi"].dropna().map(lambda d: normalize_doi(d) is None).sum())
    if scope_prefixes:
        report["out_of_scope"] = int((~df["categories"].map(lambda c: matches_scope(c, scope_prefixes))).sum())
    return report


# ═════════════════════════════════════════════════════════════════════════════
#  HACKER NEWS
# ═════════════════════════════════════════════════════════════════════════════

def classify_post(item: dict) -> str:
    """Type éditorial : show_hn, ask_hn, launch_hn, article, text, job, comment, poll…"""
    item_type = item.get("type", "story")
    if item_type != "story":
        return item_type
    title = (item.get("title") or "").lower()
    for prefix, label in (("show hn", "show_hn"), ("ask hn", "ask_hn"), ("launch hn", "launch_hn")):
        if title.startswith(prefix):
            return label
    return "article" if item.get("url") else "text"


def url_domain(url: str | None) -> str | None:
    if not url:
        return None
    match = re.match(r"^[a-z]+://([^/:?#]+)", str(url), re.IGNORECASE)
    if not match:
        return None
    host = match.group(1).lower()
    return host[4:] if host.startswith("www.") else host


def detect_arxiv_link(item: dict) -> tuple[str | None, str | None]:
    """Premier identifiant ArXiv cité dans l'URL, sinon dans le texte → (id, 'url'|'text')."""
    ids = extract_arxiv_ids(item.get("url"))
    if ids:
        return ids[0], "url"
    ids = extract_arxiv_ids(html_to_text(item.get("text")))
    if ids:
        return ids[0], "text"
    return None, None


def clean_hn_record(item: dict) -> dict:
    is_comment = item.get("type") == "comment"
    arxiv_id, link_source = detect_arxiv_link(item)
    parent = str(item["parent"]) if item.get("parent") is not None else None
    if is_comment:
        root = item.get("_story_id") or parent
        story_id = str(root) if root is not None else None
    else:
        story_id = str(item["id"])
    return {
        "hn_id": str(item["id"]),
        "type": item.get("type"),
        "post_type": classify_post(item),
        "by": item.get("by"),
        "time": item.get("time"),
        "published_at": pd.to_datetime(item.get("time"), unit="s", utc=True, errors="coerce")
        if item.get("time") is not None else pd.NaT,
        "title": clean_text(item.get("title")) or None,
        "url": item.get("url"),
        "domain": url_domain(item.get("url")),
        "text": html_to_text(item.get("text")),
        "score": None if is_comment else item.get("score", 0),
        "descendants": None if is_comment else item.get("descendants", 0),
        "parent": parent,
        "story_id": story_id,
        "kids_count": len(item.get("kids") or []),
        "arxiv_id_linked": arxiv_id,
        "arxiv_link_source": link_source,
        "feed": item.get("_feed"),
        "fetched_at": pd.to_datetime(item.get("_fetched_at"), utc=True, errors="coerce"),
    }


def clean_hn(items: list[dict], report: Counter | None = None) -> pd.DataFrame:
    report = report if report is not None else Counter()
    rows = []
    for item in items:
        report["input"] += 1
        if item.get("id") is None:
            report["missing_id"] += 1
            continue
        if item.get("deleted") or item.get("dead"):
            report["deleted_or_dead"] += 1
            continue
        rows.append(clean_hn_record(item))
    df = pd.DataFrame(rows, columns=HN_SILVER_COLUMNS)
    df = typed_hn(df)
    before = len(df)
    df = dedupe_hn(df)
    report["duplicates"] += before - len(df)
    report["arxiv_links"] += int(df["arxiv_id_linked"].notna().sum())
    report["output"] += len(df)
    return df


def dedupe_hn(df: pd.DataFrame) -> pd.DataFrame:
    """Un item relu à plusieurs cycles → on garde la mesure la plus récente (score à jour)."""
    return (df.sort_values("fetched_at", na_position="first")
              .drop_duplicates(subset="hn_id", keep="last")
              .sort_values("hn_id").reset_index(drop=True))


def typed_hn(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("published_at", "fetched_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("time", "score", "descendants", "kids_count"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def hn_bronze_frame(items: list[dict]) -> pd.DataFrame:
    columns = ["id", "type", "by", "time", "title", "url", "text", "score", "descendants",
               "parent", "kids", "deleted", "dead", "_fetched_at", "_feed"]
    return pd.DataFrame([{c: it.get(c) for c in columns} for it in items], columns=columns)
