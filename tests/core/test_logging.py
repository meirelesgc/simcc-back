import json
from datetime import datetime

import pytest
from fastapi import Depends
from sqlalchemy import text

import simcc.core.logging.handlers
from simcc import app
from simcc.core.db.database import get_async_session
from simcc.core.logging import logger
from simcc.core.logging.config import register_db_logging
from simcc.core.logging.context import (
    clear_logging_context,
    method_ctx,
    request_id_ctx,
    route_ctx,
    routine_name_ctx,
)
from simcc.core.logging.events import (
    query_error,
    request_error,
    request_finished,
    request_received,
    routine_error,
    routine_finished,
    routine_started,
)


def test_logger_schema_and_file_creation(tmp_path, monkeypatch):
    # Use a temp directory for logs to avoid cluttering production logs
    monkeypatch.setattr(simcc.core.logging.handlers, 'LOG_DIR', str(tmp_path))

    # Clear context before test
    clear_logging_context()

    # 1. Log a simple message
    logger.info('Test message', extra_param='hello')

    # Check that the file was created
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = tmp_path / f'{date_str}.jsonl'
    assert filepath.exists()

    # Read the file
    lines = filepath.read_text().splitlines()
    assert len(lines) == 1

    log_data = json.loads(lines[0])

    # Validate required fields
    assert 'timestamp' in log_data
    assert log_data['level'] == 'info'
    assert log_data['application'] == 'simcc'
    assert log_data['category'] == 'system'
    assert log_data['event'] == 'system.log'
    assert log_data['message'] == 'Test message'
    assert log_data['request_id'] is None
    assert log_data['duration'] is None
    assert isinstance(log_data['data'], dict)
    assert log_data['data']['extra_param'] == 'hello'
    assert log_data['environment'] == 'development'


def test_context_vars_propagation(tmp_path, monkeypatch):
    monkeypatch.setattr(simcc.core.logging.handlers, 'LOG_DIR', str(tmp_path))
    clear_logging_context()

    # Set context vars
    request_id_ctx.set('req-12345')
    route_ctx.set('/test-route')
    method_ctx.set('POST')
    routine_name_ctx.set('sync_data')

    logger.warning('Warning message')

    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = tmp_path / f'{date_str}.jsonl'
    lines = filepath.read_text().splitlines()
    log_data = json.loads(lines[-1])

    assert log_data['request_id'] == 'req-12345'
    assert log_data['data']['route'] == '/test-route'
    assert log_data['data']['method'] == 'POST'
    assert log_data['data']['routine_name'] == 'sync_data'


def test_event_helpers(tmp_path, monkeypatch):
    monkeypatch.setattr(simcc.core.logging.handlers, 'LOG_DIR', str(tmp_path))
    clear_logging_context()

    # Test HTTP events
    request_received('GET', '/users')
    request_finished('GET', '/users', 15.5)
    request_error('GET', '/users', 20.0, 'Timeout')

    # Test Routine events
    routine_started('import_xml')
    routine_finished('import_xml', 1200.0)
    routine_error('import_xml', 1500.0, 'Disk full')

    # Test Database event
    query_error(
        'find_by_id',
        'users_db',
        'Connection failure',
        5.2,
        'SELECT * FROM users',
    )

    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = tmp_path / f'{date_str}.jsonl'
    lines = filepath.read_text().splitlines()

    # 7 logs emitted
    assert len(lines) == 7

    # Verify request.received
    log_rec = json.loads(lines[0])
    assert log_rec['category'] == 'http'
    assert log_rec['event'] == 'request.received'
    assert log_rec['data']['method'] == 'GET'
    assert log_rec['data']['route'] == '/users'

    # Verify request.finished
    log_fin = json.loads(lines[1])
    assert log_fin['category'] == 'http'
    assert log_fin['event'] == 'request.finished'
    assert log_fin['duration'] == 15.5

    # Verify request.error
    log_err = json.loads(lines[2])
    assert log_err['category'] == 'http'
    assert log_err['event'] == 'request.error'
    assert log_err['data']['error_message'] == 'Timeout'

    # Verify database query.error
    log_db = json.loads(lines[6])
    assert log_db['category'] == 'database'
    assert log_db['event'] == 'query.error'
    assert log_db['data']['operation_name'] == 'find_by_id'
    assert log_db['data']['database_name'] == 'users_db'
    assert log_db['data']['error_message'] == 'Connection failure'
    # SQL should be None because configured level is INFO, not DEBUG
    assert log_db['data']['sql'] is None


@pytest.mark.asyncio
async def test_database_query_error_logging(
    session, engine, tmp_path, monkeypatch
):
    monkeypatch.setattr(simcc.core.logging.handlers, 'LOG_DIR', str(tmp_path))
    clear_logging_context()

    # Register the db logging listeners on the test engine
    register_db_logging(engine)

    # Execute a query that will fail
    with pytest.raises(Exception):
        await session.execute(text('SELECT * FROM table_that_does_not_exist'))

    # Verify that a query.error log was created
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = tmp_path / f'{date_str}.jsonl'
    assert filepath.exists()

    lines = filepath.read_text().splitlines()
    assert len(lines) > 0

    # We find the query.error log
    db_logs = [
        json.loads(line)
        for line in lines
        if json.loads(line).get('event') == 'query.error'
    ]
    assert len(db_logs) == 1

    log_db = db_logs[0]
    assert log_db['category'] == 'database'
    assert log_db['event'] == 'query.error'
    assert log_db['data']['database_name'] is not None
    assert 'table_that_does_not_exist' in log_db['data']['error_message']
    assert (
        log_db['data']['sql'] is None
    )  # Because configured log level is INFO (production default)


@pytest.mark.asyncio
async def test_request_id_traceability_across_layers(
    client, session, tmp_path, monkeypatch
):
    monkeypatch.setattr(simcc.core.logging.handlers, 'LOG_DIR', str(tmp_path))
    clear_logging_context()

    # Register the db logging on the current session's engine
    register_db_logging(session.bind)

    # Define a test endpoint that executes a failing query
    @app.get('/test-logging-trace-error')
    async def trace_error_endpoint(db=Depends(get_async_session)):
        await db.execute(text('SELECT * FROM non_existent_table_for_trace'))
        return {'ok': True}

    # Make the HTTP request, expecting it to raise an exception
    with pytest.raises(Exception):
        client.get('/test-logging-trace-error')

    # Read the logs written to the JSONL file
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = tmp_path / f'{date_str}.jsonl'
    assert filepath.exists()

    lines = filepath.read_text().splitlines()
    assert len(lines) >= 3

    logs = [json.loads(line) for line in lines]

    # Filter logs of interest
    http_received = [l for l in logs if l.get('event') == 'request.received']
    db_error = [l for l in logs if l.get('event') == 'query.error']
    http_error = [l for l in logs if l.get('event') == 'request.error']

    assert len(http_received) == 1
    assert len(db_error) == 1
    assert len(http_error) == 1

    req_id = http_received[0]['request_id']
    assert req_id is not None

    # Assert that they all share the same request_id!
    assert db_error[0]['request_id'] == req_id
    assert http_error[0]['request_id'] == req_id


def test_clean_old_logs(tmp_path):
    from datetime import timedelta
    from simcc.core.logging.cleanup import clean_old_logs

    today = datetime.now()
    old_date = today - timedelta(days=10)
    recent_date = today - timedelta(days=2)

    old_file = tmp_path / f"{old_date.strftime('%Y-%m-%d')}.jsonl"
    recent_file = tmp_path / f"{recent_date.strftime('%Y-%m-%d')}.jsonl"
    today_file = tmp_path / f"{today.strftime('%Y-%m-%d')}.jsonl"

    old_file.write_text('{"log": "old"}\n')
    recent_file.write_text('{"log": "recent"}\n')
    today_file.write_text('{"log": "today"}\n')

    deleted = clean_old_logs(log_dir=str(tmp_path), max_age_days=7)

    assert len(deleted) == 1
    assert str(old_file) in deleted
    assert not old_file.exists()
    assert recent_file.exists()
    assert today_file.exists()
