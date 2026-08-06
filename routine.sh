#!/bin/bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

DB_NAME="${COMPOSE_PROJECT_NAME:-db}"

docker compose exec api ./scripts/routines/pre_hop.sh

docker compose run --rm \
  --no-deps \
  -v xml:/files/jade-extrator/datasets/lattes_xml \
  hop \
  /opt/hop/hop-run.sh \
    --project=Jade-Extrator \
    --workflow=/files/jade-extrator/workflows/Index.hwf \
    --level=Basic \
    -param:DATABASE_URL="jdbc:postgresql://db:5432/${DB_NAME}?user=postgres&password=postgres" \
    -param:ADMIN_DATABASE_URL="jdbc:postgresql://db:5432/${DB_NAME}_admin?user=postgres&password=postgres"

docker compose exec api ./scripts/routines/post_hop.sh