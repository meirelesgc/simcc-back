#!/bin/bash
set -e

SERVICE_API="${SERVICE_API:-simcc-back}"

docker compose up -d "$SERVICE_API"

exec_api() {
  # Removido 'poetry run', pois o python do venv já está no PATH
  docker compose exec "$SERVICE_API" python "/app/routines/$1"
}

echo "--- Início da Rotina ---"

# exec_api soap_lattes.py

docker compose --profile extração up --abort-on-container-exit hop

ROTINES=(
  population.py
  production.py
  pog.py
  researcher_image.py
  researcher_indprod.py
  program_indprod.py
  powerBI.py
  abstract_ai.py
  embedding_database.py
  openAlex.py
  search_terms.py
)

for r in "${ROTINES[@]}"; do
  exec_api "$r"
done

echo "--- Rotina Concluída ---"