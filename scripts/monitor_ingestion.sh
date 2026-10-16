#!/usr/bin/env bash
# =============================================================================
#  SciPulse — Suivi de la progression d'une ingestion / réindexation
#  Usage : bash scripts/monitor_ingestion.sh [index] [intervalle_s]
#  Ex.   : bash scripts/monitor_ingestion.sh arxiv-papers-raw 10
#  Arrêt automatique quand le compteur est stable 3 fois de suite (ou Ctrl+C).
# =============================================================================
INDEX="${1:-arxiv-papers-raw}"
INTERVAL="${2:-10}"
ES_HOST="${ES_HOST:-http://localhost:9200}"

previous=-1
stable=0
echo "📈 Suivi de l'index '$INDEX' sur $ES_HOST (toutes les ${INTERVAL}s)"
while true; do
  COUNT=$(curl -s "$ES_HOST/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null)
  COUNT=${COUNT:-0}
  echo "$(date +%H:%M:%S) — $COUNT documents indexés"
  if [ "$COUNT" = "$previous" ] && [ "$COUNT" != "0" ]; then
    stable=$((stable + 1))
    if [ "$stable" -ge 3 ]; then
      echo "✅ Compteur stabilisé à $COUNT documents."
      exit 0
    fi
  else
    stable=0
  fi
  previous=$COUNT
  sleep "$INTERVAL"
done
