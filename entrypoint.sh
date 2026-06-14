#!/bin/sh

mkdir -p storage/xml/current
mkdir -p storage/xml/zip
chown -R appuser:appuser storage/xml

mkdir -p logs
chown -R appuser:appuser logs

# Executa as migrações do banco de dados
alembic upgrade head

# Inicia a aplicação
fastapi run src/simcc --host 0.0.0.0 --workers 4 