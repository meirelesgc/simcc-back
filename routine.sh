#!/bin/bash
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

DB_NAME="${COMPOSE_PROJECT_NAME:-db}"

docker compose exec api ./scripts/routines/pre_hop.sh

docker compose exec api http GET "http://hop:8080/hop/execWorkflow" \
  workflow==/files/jade-extrator/workflows/Index.hwf \
  DATABASE_URL=="jdbc:postgresql://db:5432/${DB_NAME}?user=postgres&password=postgres" \
  ADMIN_DATABASE_URL=="jdbc:postgresql://simcc-admin-db:5432/${DB_NAME}_admin?user=postgres&password=postgres"

docker compose exec api ./scripts/routines/post_hop.sh