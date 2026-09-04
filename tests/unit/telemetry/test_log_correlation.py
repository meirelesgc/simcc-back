import pytest
from opentelemetry import trace

from simcc.core.logging.config import format_schema_processor
from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.tracing import (
    reset_tracing_for_tests,
    setup_tracing,
)


@pytest.fixture(autouse=True)
def setup_tracing_fixture():
    reset_tracing_for_tests()
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-log-test',
        service_namespace='simcc',
        service_version='1.0.0',
        environment='test',
    )
    setup_tracing(config)
    yield
    reset_tracing_for_tests()


@pytest.mark.unit
def test_format_schema_processor_injects_trace_id():
    tracer = trace.get_tracer('test.log')
    with tracer.start_as_current_span('test_active_span') as span:
        event_dict = {
            'event': 'test.event',
            'message': 'Testing log correlation',
            'category': 'http',
        }
        res = format_schema_processor(None, 'info', event_dict)

        span_ctx = span.get_span_context()
        expected_trace_id = f'{span_ctx.trace_id:032x}'
        expected_span_id = f'{span_ctx.span_id:016x}'

        assert 'trace_id' in res['data']
        assert res['data']['trace_id'] == expected_trace_id
        assert res['data']['span_id'] == expected_span_id


@pytest.mark.unit
def test_format_schema_processor_without_active_span():
    event_dict = {
        'event': 'system.ready',
        'message': 'App started',
        'category': 'system',
    }
    res = format_schema_processor(None, 'info', event_dict)
    assert res['event'] == 'system.ready'
    assert 'data' in res
