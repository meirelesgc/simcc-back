#!/bin/sh

# Executa as migrações do banco de dados
alembic upgrade head

# Inicia a aplicação
fastapi run src/simcc --host 0.0.0.0 --workers 4 