#!/bin/bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

DB_NAME="${COMPOSE_PROJECT_NAME:-db}"

docker compose exec api ./scripts/routines/pre_hop.sh

docker compose run --rm \
  -e HOP_LOG_LEVEL=Basic \
  -e HOP_PROJECT_NAME=Jade-Extrator \
  -e HOP_PROJECT_FOLDER=/files/jade-extrator \
  -e HOP_RUN_PARAMETERS="DATABASE_URL=jdbc:postgresql://db:5432/${DB_NAME}?user=postgres&password=postgres,ADMIN_DATABASE_URL=jdbc:postgresql://db:5432/${DB_NAME}_admin?user=postgres&password=postgres" \
  -v xml:/files/jade-extrator/datasets/lattes_xml \
  hop \
  /opt/hop/hop-run.sh \
    -j /files/jade-extrator/workflows/Index.hwf

docker compose exec api ./scripts/routines/post_hop.sh