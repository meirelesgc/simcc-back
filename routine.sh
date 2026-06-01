#!/bin/bash

# routine.sh - Orchestrates the execution of SIMCC routines in a specific order.

set -e # Exit immediately if a command exits with a non-zero status.

echo "Starting SIMCC Routines..."

echo "-2/-2 Running sync_graduate_programs.py..."
# poetry run python scripts/routines/sync_admin/sync_graduate_programs.py

echo "-1/-2 Running sync_gp_researchers.py..."
# poetry run python scripts/routines/sync_admin/sync_gp_researchers.py

echo "1/12 Running soap_lattes.py..."
# poetry run python scripts/routines/soap_lattes.py

echo "2/12 Running simcc_hop (Docker)..."
# docker compose --profile routines run --rm simcc_hop

echo "3/12 Running pog.py..."
poetry run python scripts/routines/pog.py

echo "4/12 Running researcher_production.py..."
poetry run python scripts/routines/researcher_production.py

echo "5/12 Running research_dictionaries.py..."
poetry run python scripts/routines/research_dictionaries.py

echo "6/12 Running get_lattes_10.py..."
poetry run python scripts/routines/get_lattes_10.py

echo "7/12 Running researcher_indprod.py..."
poetry run python scripts/routines/researcher_indprod.py

echo "8/12 Running graduate_program_indprod.py..."
poetry run python scripts/routines/graduate_program_indprod.py

echo "9/12 Running abstract_ai.py..."
#poetry run python scripts/routines/abstract_ai.py

echo "10/12 Running get_openAlex.py..."
poetry run python scripts/routines/get_openAlex.py

echo "11/12 Running search_terms.py..."
poetry run python scripts/routines/search_terms.py

echo "12/12 Running researcher_classification.py..."
poetry run python scripts/routines/researcher_classification.py

echo "12/12 Running sync_research_lines.py..."
# poetry run python scripts/routines/sync_research_lines.py

echo "All SIMCC routines completed successfully."
