#!/bin/bash

set -e

CONTAINER_NAME="$1"

PRE_HOP=(
    "sync_admin/sync_graduate_programs.py"
    "sync_admin/sync_gp_researchers.py"
    "soap_lattes.py"
)

POST_HOP=(
    "pog.py"
    "researcher_production.py"
    "research_dictionaries.py"
    "get_lattes_10.py"
    "researcher_indprod.py"
    "graduate_program_indprod.py"
    "abstract_ai.py"
    "get_openAlex.py"
    "search_terms.py"
    "researcher_classification.py"
    "sync_research_lines.py"
)

run_python() {
    local script="$1"

    if [ -n "$CONTAINER_NAME" ]; then
        docker compose exec -T "$CONTAINER_NAME" \
            python "scripts/routines/$script"
    else
        poetry run python "scripts/routines/$script"
    fi
}

run_group() {
    local group_name="$1"
    shift
    local routines=("$@")

    local total=${#routines[@]}

    for i in "${!routines[@]}"; do
        local current=$((i + 1))

        echo "[$group_name] $current/$total Running ${routines[$i]}..."
        run_python "${routines[$i]}"
    done
}

echo "Starting SIMCC Routines..."

run_group "PRE_HOP" "${PRE_HOP[@]}"

echo "Running simcc_hop (Docker)..."
docker compose --profile routines run --rm simcc_hop

run_group "POST_HOP" "${POST_HOP[@]}"

echo "All SIMCC routines completed successfully."