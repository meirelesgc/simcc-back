import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from simcc.core.telemetry import init_telemetry
from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.metrics import reset_metrics_for_tests
from simcc.core.telemetry.tracing import (
    get_in_memory_span_exporter,
    reset_tracing_for_tests,
)


@pytest.fixture
def app_with_tracing():
    reset_tracing_for_tests()
    reset_metrics_for_tests()
    test_app = FastAPI()

    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-test',
        service_namespace='simcc',
        service_version='1.0.0',
        environment='test',
    )
    init_telemetry(test_app, config)

    @test_app.get('/test-ping')
    def ping():
        return {'status': 'ok'}

    @test_app.get('/test-error')
    def err():
        raise ValueError('Simulated error')

    return test_app


@pytest.mark.unit
def test_fastapi_http_span_generation(app_with_tracing):
    client = TestClient(app_with_tracing, raise_server_exceptions=False)
    exporter = get_in_memory_span_exporter()
    assert exporter is not None
    exporter.clear()

    resp = client.get('/test-ping')
    assert resp.status_code == 200

    spans = exporter.get_finished_spans()
    assert len(spans) >= 1

    http_span = next(
        (
            s
            for s in spans
            if '/test-ping' in s.name
            or s.attributes.get('http.route') == '/test-ping'
            or s.attributes.get('http.target') == '/test-ping'
        ),
        None,
    )
    assert http_span is not None
    assert (
        http_span.attributes.get('http.status_code') == 200
        or http_span.attributes.get('http.response.status_code') == 200
    )


@pytest.mark.unit
def test_fastapi_http_span_error_recording(app_with_tracing):
    client = TestClient(app_with_tracing, raise_server_exceptions=False)
    exporter = get_in_memory_span_exporter()
    assert exporter is not None
    exporter.clear()

    resp = client.get('/test-error')
    assert resp.status_code == 500

    spans = exporter.get_finished_spans()
    assert len(spans) >= 1
