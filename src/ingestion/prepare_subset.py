"""
SciPulse — Extraction d'un subset CS/ML depuis le dump ArXiv complet (TP 1, étape 4 — option C)

Le dump Kaggle « arxiv-metadata-oai-snapshot.json » (~4 Go, JSON lines) est trié
par identifiant : les premières lignes datent de 2007. Prendre « les 100 000
premiers articles cs.* » donnerait donc un corpus sans aucun article récent
(pas de LLM, pas de diffusion…). Trois stratégies sont proposées :

  --mode random  (défaut) échantillonnage uniforme sur tout le dump (reservoir sampling, graine fixe)
  --mode last    les N derniers articles correspondants (corpus très récent)
  --mode first   les N premiers (comportement du pseudo-code de l'énoncé)

Usage :
    python -m src.ingestion.prepare_subset data/arxiv-metadata-oai-snapshot.json
    python -m src.ingestion.prepare_subset data/arxiv-metadata-oai-snapshot.json --max 100000 --mode last

Téléchargement du dump (compte Kaggle requis) :
    kaggle datasets download -d Cornell-University/arxiv -p data/ --unzip
"""

import argparse
import json
import random
import sys
from collections import deque
from pathlib import Path

DEFAULT_INPUT = "data/arxiv-metadata-oai-snapshot.json"
DEFAULT_OUTPUT = "data/arxiv-raw/arxiv-cs-subset-100k.json"
DEFAULT_PREFIXES = ("cs.", "stat.ML")


def matches(categories: str, prefixes=DEFAULT_PREFIXES) -> bool:
    """Vrai si au moins une catégorie commence par un des préfixes (cs.*, stat.ML)."""
    return any(cat.startswith(prefixes) for cat in (categories or "").split())


def iter_matching_lines(path: str, prefixes=DEFAULT_PREFIXES):
    """Générateur : lignes brutes (str) dont l'article appartient au périmètre."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                article = json.loads(line)
            except json.JSONDecodeError:
                continue
            if matches(article.get("categories", ""), prefixes):
                yield line


def select(lines, max_docs: int, mode: str, seed: int = 42) -> list[str]:
    if mode == "first":
        selected = []
        for line in lines:
            selected.append(line)
            if len(selected) >= max_docs:
                break
        return selected

    if mode == "last":
        return list(deque(lines, maxlen=max_docs))

    # reservoir sampling (algorithme R) : échantillon uniforme en une seule passe
    rng = random.Random(seed)
    reservoir: list[str] = []
    for i, line in enumerate(lines):
        if i < max_docs:
            reservoir.append(line)
        else:
            j = rng.randint(0, i)
            if j < max_docs:
                reservoir[j] = line
    return reservoir


def main():
    parser = argparse.ArgumentParser(description="SciPulse — subset CS/ML du dump ArXiv")
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--max", type=int, default=100_000)
    parser.add_argument("--mode", choices=["random", "last", "first"], default="random")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"❌ Dump introuvable : {args.input}")
        print("   kaggle datasets download -d Cornell-University/arxiv -p data/ --unzip")
        sys.exit(1)

    print(f"🔎 Filtrage de {args.input} (catégories {', '.join(DEFAULT_PREFIXES)}) — mode {args.mode}")
    selected = select(iter_matching_lines(args.input), args.max, args.mode, args.seed)

    # tri par identifiant pour un fichier lisible et reproductible
    selected.sort(key=lambda l: json.loads(l).get("id", ""))

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as out:
        for line in selected:
            out.write(line + "\n")

    print(f"✅ {len(selected):,} articles écrits dans {args.output}")


if __name__ == "__main__":
    main()
