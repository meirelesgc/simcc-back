import csv
from http import HTTPStatus

import pytest

import simcc.routers.powerBi
import simcc.services.powerBi_service


@pytest.mark.asyncio
async def test_dim_log_category(client, tmp_path, monkeypatch):
    """
    Garante que o endpoint /dim_log_category.csv retorne HTTP 200 OK
    e gere o arquivo CSV com as categorias de log esperadas.
    """
    monkeypatch.setattr(simcc.services.powerBi_service, 'PATH', str(tmp_path))
    monkeypatch.setattr(simcc.routers.powerBi, 'STORAGE_PATH', tmp_path)

    response = client.get('/dim_log_category.csv')
    assert response.status_code == HTTPStatus.OK

    csv_file = tmp_path / 'dim_log_category.csv'
    assert csv_file.exists()

    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)

    categories = [r['category'] for r in rows]
    assert 'http' in categories
    assert 'database' in categories
    assert 'routine' in categories
    assert 'system' in categories


@pytest.mark.asyncio
async def test_dim_log_event(client, tmp_path, monkeypatch):
    """
    Garante que o endpoint /dim_log_event.csv retorne HTTP 200 OK
    e gere o arquivo CSV com os tipos de eventos de log esperados.
    """
    monkeypatch.setattr(simcc.services.powerBi_service, 'PATH', str(tmp_path))
    monkeypatch.setattr(simcc.routers.powerBi, 'STORAGE_PATH', tmp_path)

    response = client.get('/dim_log_event.csv')
    assert response.status_code == HTTPStatus.OK

    csv_file = tmp_path / 'dim_log_event.csv'
    assert csv_file.exists()

    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)

    events = [r['event'] for r in rows]
    assert 'request.received' in events
    assert 'query.error' in events
    assert 'routine.started' in events


@pytest.mark.asyncio
async def test_fat_logs_and_supporting_csvs(client, tmp_path, monkeypatch):
    """
    Garante que o endpoint /fat_logs.csv retorne HTTP 200 OK, unifique os logs
    e que os endpoints de apoio (HTTP, DB, Routine) retornem dados explodidos
    vinculados por um log_id único.
    """
    # 1. Preparar diretório de logs e arquivos JSONL com logs de 3 categorias distintas
    log_dir = tmp_path / 'logs'
    log_dir.mkdir()

    log_1 = (
        '{"timestamp": "2026-07-23T03:30:00Z", "level": "info", "application": "simcc", '
        '"environment": "production", "hostname": "host-http", "category": "http", '
        '"event": "request.received", "message": "API call", "request_id": "req-111", '
        '"duration": 15.4, "data": {"route": "/health", "method": "GET", "user_id": "usr-1", '
        '"error_message": null}}\n'
    )
    log_2 = (
        '{"timestamp": "2026-07-23T03:31:00Z", "level": "error", "application": "simcc", '
        '"environment": "production", "hostname": "host-db", "category": "database", '
        '"event": "query.error", "message": "DB fail", "request_id": "req-222", '
        '"duration": 3.2, "data": {"database_name": "simcc_db", "operation_name": "get_by_id", '
        '"error_message": "Conn lost", "sql": "SELECT * FROM..."}}\n'
    )
    log_3 = (
        '{"timestamp": "2026-07-23T03:32:00Z", "level": "info", "application": "simcc", '
        '"environment": "production", "hostname": "host-routine", "category": "routine", '
        '"event": "routine.started", "message": "Task init", "request_id": null, '
        '"duration": null, "data": {"routine_name": "sync_lattes", "error_message": null, '
        '"items_found": 10, "items_succeeded": 8, "items_failed": 2}}\n'
    )

    log_file = log_dir / '2026-07-23.jsonl'
    log_file.write_text(log_1 + log_2 + log_3, encoding='utf-8')

    # 2. Configurar caminhos e variáveis de ambiente isoladas nos testes
    monkeypatch.setenv('LOG_DIR', str(log_dir))
    monkeypatch.setattr(simcc.services.powerBi_service, 'PATH', str(tmp_path))
    monkeypatch.setattr(simcc.routers.powerBi, 'STORAGE_PATH', tmp_path)

    # 3. Testar a tabela FATO geral (/fat_logs.csv)
    response = client.get('/fat_logs.csv')
    assert response.status_code == HTTPStatus.OK

    csv_file = tmp_path / 'fat_logs.csv'
    assert csv_file.exists()

    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        fat_rows = list(reader)

    assert len(fat_rows) == 3

    # Validar campos raiz padrão
    for idx, row in enumerate(fat_rows):
        assert row['log_id'] == f'LOG_{idx}'
        assert row['application'] == 'simcc'
        assert row['environment'] == 'production'

    assert fat_rows[0]['hostname'] == 'host-http'
    assert fat_rows[0]['category'] == 'http'
    assert fat_rows[1]['hostname'] == 'host-db'
    assert fat_rows[1]['category'] == 'database'
    assert fat_rows[2]['hostname'] == 'host-routine'
    assert fat_rows[2]['category'] == 'routine'

    # 4. Testar a tabela de apoio de HTTP (/fat_logs_http.csv)
    response_http = client.get('/fat_logs_http.csv')
    assert response_http.status_code == HTTPStatus.OK

    csv_http = tmp_path / 'fat_logs_http.csv'
    assert csv_http.exists()

    with open(csv_http, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        http_rows = list(reader)

    assert len(http_rows) == 1
    assert (
        http_rows[0]['log_id'] == 'LOG_0'
    )  # Liga com o primeiro registro geral
    assert http_rows[0]['route'] == '/health'
    assert http_rows[0]['method'] == 'GET'
    assert http_rows[0]['user_id'] == 'usr-1'
    assert http_rows[0]['error_message'] == ''

    # 5. Testar a tabela de apoio de Database (/fat_logs_database.csv)
    response_db = client.get('/fat_logs_database.csv')
    assert response_db.status_code == HTTPStatus.OK

    csv_db = tmp_path / 'fat_logs_database.csv'
    assert csv_db.exists()

    with open(csv_db, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        db_rows = list(reader)

    assert len(db_rows) == 1
    assert db_rows[0]['log_id'] == 'LOG_1'  # Liga com o segundo registro geral
    assert db_rows[0]['database_name'] == 'simcc_db'
    assert db_rows[0]['operation_name'] == 'get_by_id'
    assert db_rows[0]['error_message'] == 'Conn lost'
    assert db_rows[0]['sql'] == 'SELECT * FROM...'

    # 6. Testar a tabela de apoio de Routine (/fat_logs_routine.csv)
    response_routine = client.get('/fat_logs_routine.csv')
    assert response_routine.status_code == HTTPStatus.OK

    csv_routine = tmp_path / 'fat_logs_routine.csv'
    assert csv_routine.exists()

    with open(csv_routine, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        routine_rows = list(reader)

    assert len(routine_rows) == 1
    assert (
        routine_rows[0]['log_id'] == 'LOG_2'
    )  # Liga com o terceiro registro geral
    assert routine_rows[0]['routine_name'] == 'sync_lattes'
    assert routine_rows[0]['error_message'] == ''
    assert int(routine_rows[0]['items_found']) == 10
    assert int(routine_rows[0]['items_succeeded']) == 8
    assert int(routine_rows[0]['items_failed']) == 2
