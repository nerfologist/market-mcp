#!/bin/sh
# Sync source to the Docker host and rebuild. Target settings come from deploy.env (not in git,
# see deploy.env.example). .env lives only on the host; data/isins.json (if present locally) and
# config/ (holdings.json, thresholds.json) are synced too, everything else under data/ is the container's.
#   ./deploy.sh          full sync + rebuild
#   ./deploy.sh config   sync config/ only (after editing holdings.json or thresholds.json; no restart needed)
set -e
cd "$(dirname "$0")"
[ -f deploy.env ] && . ./deploy.env
HOST="${DEPLOY_HOST:-docker-host}"
DIR="${DEPLOY_DIR:-/srv/market-mcp}"
DOCKER="${DEPLOY_DOCKER:-docker}"
if [ "$1" = "config" ]; then
  rsync -av config/ "$HOST:$DIR/config/"
  exit 0
fi
rsync -av --exclude .env --exclude deploy.env --exclude .git --exclude 'PLAN-*.md' --exclude __pycache__ \
  --include data/ --include data/isins.json --exclude 'data/*' ./ "$HOST:$DIR/"
ssh "$HOST" "cd $DIR && $DOCKER compose up -d --build && sleep 5 && curl -s http://127.0.0.1:8765/healthz"
