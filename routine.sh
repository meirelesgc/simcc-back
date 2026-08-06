#!/bin/bash
set -e

docker compose exec api ./scripts/routines/pre_hop.sh

docker compose run --rm -e HOP_FILE_PATH=/files/jade-extrator/workflows/Index.hwf hop

docker compose exec api ./scripts/routines/post_hop.sh