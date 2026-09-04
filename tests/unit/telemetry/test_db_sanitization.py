import pytest
from opentelemetry import trace

from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.tracing import (
    get_in_memory_span_exporter,
    reset_tracing_for_tests,
    setup_tracing,
)


@pytest.fixture(autouse=True)
def setup_in_memory():
    reset_tracing_for_tests()
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-sanitization-test',
        service_namespace='simcc',
        service_version='1.0.0',
        environment='test',
    )
    setup_tracing(config)
    yield
    reset_tracing_for_tests()


@pytest.mark.unit
def test_sanitizing_span_processor_db_statement():
    exporter = get_in_memory_span_exporter()
    assert exporter is not None
    exporter.clear()

    tracer = trace.get_tracer('test.sanitizer')
    with tracer.start_as_current_span('db.query') as span:
        span.set_attribute(
            'db.statement',
            "SELECT * FROM researcher WHERE email = 'secret@ufba.br' AND id = 12345",
        )
        span.set_attribute('prompt', 'Qual o segredo do universo?')
        span.set_attribute('response', 'A resposta é 42.')

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]

    # Statement must have literals replaced by placeholders
    stmt = span.attributes['db.statement']
    assert 'secret@ufba.br' not in stmt
    assert '12345' not in stmt
    assert '?' in stmt

    # Sensitive keys must be redacted
    assert span.attributes['prompt'] == '[REDACTED]'
    assert span.attributes['response'] == '[REDACTED]'
