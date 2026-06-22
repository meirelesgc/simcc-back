#!/bin/bash
set -e

API_SERVICE=$(docker compose config --services | grep -E '(^|_)api$' | head -n1)

if [ "$API_SERVICE" = "api" ]; then
  HOP_SERVICE="hop"
  DB_SERVICE="db"
  DB_NAME="db"
else
  PREFIX=${API_SERVICE%_api}
  HOP_SERVICE="${PREFIX}_hop"
  DB_SERVICE="${PREFIX}_db"
  DB_NAME="${PREFIX}"
fi

docker compose exec "$API_SERVICE" ./scripts/routines/pre_hop.sh

docker compose exec "$API_SERVICE" http GET "http://${HOP_SERVICE}:8080/hop/execWorkflow" \
  workflow==/files/jade-extrator/workflows/Index.hwf \
  DATABASE_URL=="jdbc:postgresql://${DB_SERVICE}:5432/${DB_NAME}?user=postgres&password=postgres" \
  ADMIN_DATABASE_URL=="jdbc:postgresql://${DB_SERVICE}:5432/${DB_NAME}_admin?user=postgres&password=postgres"

docker compose exec "$API_SERVICE" ./scripts/routines/post_hop.sh