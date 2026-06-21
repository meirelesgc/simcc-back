#!/bin/bash
set -e

API_SERVICE=$(docker compose config --services | grep '_api$' | head -n1)
PREFIX=${API_SERVICE%_api}

HOP_SERVICE="${PREFIX}_hop"
DB_SERVICE="${PREFIX}_db"

docker compose exec "$API_SERVICE" ./scripts/routines/pre_hop.sh

docker compose exec "$API_SERVICE" curl -G "http://${HOP_SERVICE}:8080/hop/execWorkflow" \
  --data-urlencode "workflow=/files/jade-extrator/workflows/Index.hwf" \
  --data-urlencode "DATABASE_URL=jdbc:postgresql://${DB_SERVICE}:5432/${PREFIX}?user=postgres&password=postgres" \
  --data-urlencode "ADMIN_DATABASE_URL=jdbc:postgresql://${DB_SERVICE}:5432/${PREFIX}_admin?user=postgres&password=postgres"

docker compose exec "$API_SERVICE" ./scripts/routines/post_hop.sh