#!/bin/sh
# Sync source to the Docker host and rebuild. Target settings come from deploy.env (not in git,
# see deploy.env.example). .env lives only on the host; data/isins.json (if present locally) is
# synced too, everything else under data/ is the container's.
set -e
cd "$(dirname "$0")"
[ -f deploy.env ] && . ./deploy.env
HOST="${DEPLOY_HOST:-docker-host}"
DIR="${DEPLOY_DIR:-/srv/market-mcp}"
DOCKER="${DEPLOY_DOCKER:-docker}"
rsync -av --exclude .env --exclude deploy.env --exclude .git --exclude 'PLAN-*.md' --exclude __pycache__ \
  --include data/ --include data/isins.json --exclude 'data/*' ./ "$HOST:$DIR/"
ssh "$HOST" "cd $DIR && $DOCKER compose up -d --build && sleep 5 && curl -s http://127.0.0.1:8765/healthz"
