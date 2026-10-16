"""
SciPulse — Fonctions utilitaires communes pour les articles ArXiv.

Fonctions pures (sans accès réseau) : faciles à tester avec Pytest.
"""

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Identifiants ArXiv :
#  - nouveau format : 2301.07041, 2301.07041v2 (depuis 2007)
#  - ancien format  : cs/0112017, math.GT/0309136
NEW_ID_RE = re.compile(r"(\d{4}\.\d{4,5})(v\d+)?")
OLD_ID_RE = re.compile(r"([a-z\-]+(?:\.[A-Z]{2})?/\d{7})(v\d+)?")

ARXIV_URL_PATTERN = re.compile(
    r"arxiv\.org/(?:abs|pdf|html)/"
    r"(?P<id>\d{4}\.\d{4,5}|[a-z\-]+(?:\.[A-Z]{2})?/\d{7})"
    r"(?P<version>v\d+)?",
    re.IGNORECASE,
)

ES_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"  # même rendu que le filtre `date` de Logstash


def split_categories(categories) -> list[str]:
    """'cs.AI cs.LG stat.ML' → ['cs.AI', 'cs.LG', 'stat.ML'] (accepte aussi une liste)."""
    if not categories:
        return []
    if isinstance(categories, (list, tuple)):
        return [c for c in categories if c]
    return str(categories).split()


def primary_category(categories) -> str | None:
    cats = split_categories(categories)
    return cats[0] if cats else None


def domain_of(category: str | None) -> str | None:
    """'cs.AI' → 'cs' ; 'stat.ML' → 'stat' ; 'quant-ph' → 'quant-ph'."""
    if not category:
        return None
    return category.split(".")[0]


def clean_text(text: str | None) -> str | None:
    """Remplace les retours à la ligne et espaces multiples par un espace."""
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip()


def parse_update_date(value: str | None) -> str | None:
    """'2023-02-07' → '2023-02-07T00:00:00.000Z' (format produit par Logstash)."""
    if not value:
        return None
    try:
        dt = datetime.strptime(str(value)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return dt.strftime(ES_DATE_FORMAT)


def parse_version_date(value: str | None) -> str | None:
    """'Mon, 2 Apr 2007 19:18:42 GMT' → '2007-04-02T19:18:42.000Z'."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(ES_DATE_FORMAT)


def first_version_date(versions) -> str | None:
    """Date de la v1 d'un article (= date de première publication sur ArXiv)."""
    if not versions or not isinstance(versions, list):
        return None
    first = versions[0]
    if isinstance(first, dict):
        return parse_version_date(first.get("created"))
    return None


def normalize_arxiv_id(raw_id: str | None) -> str | None:
    """Supprime le suffixe de version : '2301.07041v3' → '2301.07041'."""
    if not raw_id:
        return None
    raw_id = str(raw_id).strip()
    m = NEW_ID_RE.fullmatch(raw_id) or OLD_ID_RE.fullmatch(raw_id)
    return m.group(1) if m else raw_id


def extract_arxiv_ids(text: str | None) -> list[str]:
    """Extrait tous les identifiants ArXiv (sans version) cités dans une URL ou un texte."""
    if not text:
        return []
    seen = []
    for m in ARXIV_URL_PATTERN.finditer(text):
        arxiv_id = m.group("id")
        if arxiv_id not in seen:
            seen.append(arxiv_id)
    return seen


# ── Auteurs ──────────────────────────────────────────────────────────────────

_PARENS_RE = re.compile(r"\([^()]*\)")
_AND_RE = re.compile(r"\s+and\s+|^and\s+", re.IGNORECASE)


def _split_top_level(text: str) -> list[str]:
    """Découpe sur les virgules qui ne sont pas entre parenthèses."""
    parts, depth, current = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    # second niveau : « A and B » → [A, B] (hors parenthèses)
    result = []
    for part in parts:
        protected = _PARENS_RE.sub(lambda m: m.group(0).replace(" and ", " \x00 "), part)
        for sub in _AND_RE.split(protected):
            sub = sub.replace(" \x00 ", " and ").strip()
            if sub:
                result.append(sub)
    return result


def _full_name(parts) -> str:
    """['Martin', 'Alice', ''] → 'Alice Martin' (format authors_parsed du dump)."""
    if not parts:
        return ""
    last = parts[0] if len(parts) > 0 else ""
    first = parts[1] if len(parts) > 1 else ""
    suffix = parts[2] if len(parts) > 2 else ""
    return " ".join(p.strip() for p in (first, last, suffix) if p and p.strip())


def parse_authors(authors_str: str | None, authors_parsed=None) -> tuple[list[dict], str]:
    """
    Construit la liste structurée des auteurs (pour le champ nested) et
    une version plate « Nom1, Nom2, ... » (pour la recherche et dbt).

    - Si `authors_parsed` (fourni par le dump Kaggle) est présent, on l'utilise :
      c'est la source la plus fiable.
    - Sinon, heuristique sur la chaîne brute : séparation sur les virgules
      et sur « and ». Les affiliations entre parenthèses sont extraites.
    """
    authors: list[dict] = []

    if authors_parsed and isinstance(authors_parsed, list):
        for parts in authors_parsed:
            if isinstance(parts, (list, tuple)):
                name = _full_name(parts)
                # le dump ajoute parfois l'affiliation en 4e position
                affiliation = parts[3].strip() if len(parts) > 3 and parts[3] else None
                if name:
                    authors.append({"name": name, "affiliation": affiliation or None})
    elif authors_str:
        text = clean_text(authors_str) or ""
        for chunk in _split_top_level(text):
            # « Alice Martin (MIT) » → nom + affiliation
            affiliations = [a.strip("() ") for a in _PARENS_RE.findall(chunk)]
            name = _PARENS_RE.sub("", chunk).strip(" ,.")
            if len(name) < 2:
                continue
            authors.append({"name": name, "affiliation": affiliations[0] if affiliations else None})

    flat = ", ".join(a["name"] for a in authors)
    return authors, flat
