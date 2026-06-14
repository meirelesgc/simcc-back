#!/bin/bash

set -e

API_SERVICE=$(docker compose config --services | grep '_api$' | head -n1)
HOP_SERVICE=$(docker compose --profile routines config --services | grep '_hop$' | head -n1)

if [ -z "$API_SERVICE" ] || [ -z "$HOP_SERVICE" ]; then
  echo "Erro: Servicos nao encontrados."
  exit 1
fi

echo "Starting PRE_HOP routines..."
docker compose exec "$API_SERVICE" ./scripts/routines/pre_hop.sh

echo "Running Apache Hop..."
docker compose --profile routines run --rm "$HOP_SERVICE"

echo "Starting POST_HOP routines..."
docker compose exec "$API_SERVICE" ./scripts/routines/post_hop.sh

echo "All routines completed successfully."