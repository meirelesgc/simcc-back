#!/bin/bash
set -e

SERVICE_API="${SERVICE_API:-simcc-back}"

LIMIT=100
OFFSET=0
FIRST_RUN=true

docker compose up -d "$SERVICE_API"

exec_api() {
  docker compose exec "$SERVICE_API" python "/app/routines/$1" "${@:2}"
}

echo "--- Início da Rotina SOAP_LATTES ---"

while true; do
  echo "Processando bloco OFFSET=$OFFSET LIMIT=$LIMIT"

  if [ "$FIRST_RUN" = true ]; then
    OUTPUT=$(exec_api soap_lattes.py --limit "$LIMIT" --offset "$OFFSET" --clean-xml)
    FIRST_RUN=false
  else
    OUTPUT=$(exec_api soap_lattes.py --limit "$LIMIT" --offset "$OFFSET")
  fi

  if echo "$OUTPUT" | grep -q "Nenhum registro retornado"; then
    echo "Carga completa do SOAP_LATTES"
    break
  fi

  OFFSET=$((OFFSET + LIMIT))
done

echo "--- SOAP_LATTES concluído ---"

docker compose --profile extração run --rm hop

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

echo "--- Início das rotinas pós-extração ---"

for r in "${ROTINES[@]}"; do
  exec_api "$r"
done

echo "--- Rotina Concluída ---"
