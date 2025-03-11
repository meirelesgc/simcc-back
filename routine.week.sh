#!/usr/bin/env bash

check_error() {
  if [ $? -ne 0 ]; then
    echo "Erro: Falha ao executar $1 em $DATE_FORMAT"
    exit 1
  fi
}

SIMCC_HOME=$(dirname "$(readlink -f "$0")")

cd "$SIMCC_HOME" || exit 1
source .venv/bin/activate
source .env

"${SIMCC_HOME}/.venv/bin/python" routines/search_terms.py
check_error "terms"

"${SIMCC_HOME}/.venv/bin/python" routines/openAlex.py
check_error "openAlex"