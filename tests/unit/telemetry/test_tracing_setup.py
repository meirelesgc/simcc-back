import pytest

from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.tracing import (
    create_telemetry_resource,
    get_in_memory_span_exporter,
    get_tracer,
    reset_tracing_for_tests,
    setup_tracing,
)


@pytest.fixture(autouse=True)
def cleanup_tracing():
    reset_tracing_for_tests()
    yield
    reset_tracing_for_tests()


@pytest.mark.unit
def test_create_telemetry_resource():
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='test-service',
        service_namespace='test-namespace',
        service_version='1.0.0',
        environment='test',
    )
    resource = create_telemetry_resource(config)
    attrs = dict(resource.attributes)
    assert attrs['service.name'] == 'test-service'
    assert attrs['service.namespace'] == 'test-namespace'
    assert attrs['service.version'] == '1.0.0'
    assert attrs['deployment.environment.name'] == 'test'


@pytest.mark.unit
def test_setup_tracing_in_memory():
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-back',
        service_namespace='simcc',
        service_version='4.5.0',
        environment='test',
    )
    provider = setup_tracing(config)
    assert provider is not None

    exporter = get_in_memory_span_exporter()
    assert exporter is not None
    assert len(exporter.get_finished_spans()) == 0

    tracer = get_tracer('test_module')
    with tracer.start_as_current_span('test_span') as span:
        span.set_attribute('custom_key', 'custom_value')

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == 'test_span'
    assert spans[0].attributes['custom_key'] == 'custom_value'
